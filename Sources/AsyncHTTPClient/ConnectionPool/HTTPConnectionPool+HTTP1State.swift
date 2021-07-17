//===----------------------------------------------------------------------===//
//
// This source file is part of the AsyncHTTPClient open source project
//
// Copyright (c) 2021 Apple Inc. and the AsyncHTTPClient project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of AsyncHTTPClient project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import NIO

extension HTTPConnectionPool {
    struct HTTP1ConnectionState {
        enum State {
            case waitingToStart(retries: Int)
            case starting(retries: Int)
            case available(Connection, since: NIODeadline)
            case leased(Connection)
            case failed
            case closed
        }

        private var state: State
        let eventLoop: EventLoop
        let connectionID: Connection.ID

        init(connectionID: Connection.ID, eventLoop: EventLoop) {
            self.connectionID = connectionID
            self.eventLoop = eventLoop
            self.state = .starting
        }

        var isStarting: Bool {
            switch self.state {
            case .starting, .waitingToStart:
                return true
            case .failed, .closed, .available, .leased:
                return false
            }
        }

        var isAvailable: Bool {
            switch self.state {
            case .available:
                return true
            case .starting, .leased, .failed, .closed:
                return false
            }
        }

        var isLeased: Bool {
            switch self.state {
            case .leased:
                return true
            case .starting, .available, .failed, .closed:
                return false
            }
        }

        var availableAndLastReturn: NIODeadline? {
            switch self.state {
            case .available(_, since: let lastReturn):
                return lastReturn
            case .starting, .leased, .failed, .closed:
                return nil
            }
        }

        mutating func started(_ connection: Connection) {
            switch self.state {
            case .starting:
                self.state = .available(connection, since: .now())
            case .available, .leased, .failed, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func failedToStart() {
            switch self.state {
            case .starting:
                self.state = .failed
            case .available, .leased, .failed, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func lease() -> Connection {
            switch self.state {
            case .available(let connection, since: _):
                self.state = .leased(connection)
                return connection
            case .starting, .leased, .failed, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func release() {
            switch self.state {
            case .leased(let connection):
                self.state = .available(connection, since: .now())
            case .starting, .available, .failed, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func close() -> Connection {
            switch self.state {
            case .available(let connection, since: _):
                self.state = .closed
                return connection
            case .starting, .leased, .failed, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func cancel() -> Connection {
            switch self.state {
            case .leased(let connection):
                return connection
            case .starting, .available, .failed, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }
    }

    struct HTTP1StateMachine {
        enum State: Equatable {
            case running
            case shuttingDown(unclean: Bool)
            case shutDown
        }

        typealias Action = HTTPConnectionPool.StateMachine.Action

        let maximumConcurrentConnections: Int
        let idGenerator: Connection.ID.Generator
        private var connections: [HTTP1ConnectionState] {
            didSet {
                assert(self.connections.count <= self.maximumConcurrentConnections)
            }
        }

        private var waiters: CircularBuffer<Waiter>
        private var state: State = .running

        init(idGenerator: Connection.ID.Generator, maximumConcurrentConnections: Int) {
            self.idGenerator = idGenerator
            self.maximumConcurrentConnections = maximumConcurrentConnections
            self.connections = []
            self.connections.reserveCapacity(self.maximumConcurrentConnections)
            self.waiters = CircularBuffer(initialCapacity: 32)
        }

        mutating func executeRequest(
            _ request: HTTPSchedulableRequest, 
            onPreferred preferredEL: EventLoop, 
            required: Bool
        ) -> Action {
            switch self.state {
            case .running:
                break
            case .shuttingDown, .shutDown:
                // it is fairly unlikely that this condition is met, since the ConnectionPoolManager
                // also fails new requests immediately, if it is shutting down. However there might
                // be race conditions in which a request passes through a running connection pool
                // manager, but hits a connection pool that is already shutting down.
                //
                // (Order in one lock does not guarantee order in the next lock!)
                return .init(.failRequest(request, HTTPClientError.alreadyShutdown, cancelWaiter: nil), .none)
            }

            switch self.findAvailableConnection(onPreferred: preferredEL, required: `required`) {
            case .elMatch(connectionIndex: let index):
                // if we have an available perfect match connection, we can use it right away
                let connection = self.connections[index].lease()
                return .init(
                    .executeRequest(request, connection, cancelWaiter: nil),
                    .cancelTimeoutTimer(connection.id)
                )
            
            case .offElMatch(connectionIndex: let index) where `required` == true:
                // There is a connection, but it doesn't match or required EL
                assert(self.connections[index].eventLoop !== preferredEL)
                
                // No matter what we do, the request will need to wait!
                let newWaiter = Waiter(request: request)
                self.waiters.append(newWaiter)
                
                if self.maximumConcurrentConnections > self.connections.count {
                    // if we are not at max connections, we should create a new connection
                    let newConnection = HTTP1ConnectionState(connectionID: self.idGenerator.next(), eventLoop: preferredEL)
                    self.connections.append(newConnection)
                    
                    return .init(
                        .scheduleWaiterTimeout(newWaiter.requestID, request, on: preferredEL),
                        .createConnection(newConnection.connectionID, on: preferredEL)
                    )
                }
                
                // we are at maximum connections.
                if self.doesAnActiveConnectionExist(for: preferredEL) {
                    // another connection exists that serves on this el. For this reason we should
                    // just wait for this el to become available
                    return .init(
                        .scheduleWaiterTimeout(newWaiter.requestID, request, on: preferredEL),
                        .none
                    )
                }
                
                // another connection does not exist, that could eventually serve our request.
                // replace the current connection with a new one on the correct el

                let newConnectionID = self.idGenerator.next()
                var replacement = HTTP1ConnectionState(connectionID: newConnectionID, eventLoop: preferredEL)
                swap(&replacement, &self.connections[index])

                return .init(
                    .scheduleWaiterTimeout(newWaiter.requestID, request, on: preferredEL),
                    .replaceConnection(replacement.close(), with: newConnectionID, on: preferredEL)
                )
                
            case .offElMatch(connectionIndex: let index):
                assert(`required` == false)
                let connection = self.connections[index].lease()
                return .init(
                    .executeRequest(request, connection, cancelWaiter: nil),
                    .cancelTimeoutTimer(connection.id)
                )
                
            case .none:
                // there isn't any idle connection.
                
                // No matter what we do, the request will need to wait!
                let newWaiter = Waiter(request: request)
                self.waiters.append(newWaiter)
                
                if self.maximumConcurrentConnections > self.connections.count {
                    // if we are not at max connections, we should create a new connection
                    let newConnection = HTTP1ConnectionState(connectionID: self.idGenerator.next(), eventLoop: preferredEL)
                    self.connections.append(newConnection)
                    
                    return .init(
                        .scheduleWaiterTimeout(newWaiter.requestID, request, on: preferredEL),
                        .createConnection(newConnection.connectionID, on: preferredEL)
                    )
                }

                // all connections are busy and there is no room for more connections, we need to wait!
                return .init(
                    .scheduleWaiterTimeout(newWaiter.requestID, request, on: preferredEL),
                    .none
                )
            }
        }
        
        enum AvailableConnectionMatch {
            case elMatch(connectionIndex: Int)
            case offElMatch(connectionIndex: Int)
            case none
        }
        
        private func findAvailableConnection(onPreferred preferredEL: EventLoop, required: Bool)
            -> AvailableConnectionMatch
        {
            var eventLoopMatch: (Int, NIODeadline)?
            var goodMatch: (Int, NIODeadline)?
            
            // To find an appropriate connection we iterate all existing connections.
            // While we do this we try to find the best fitting connection for our request.
            //
            // A perfect match, runs on the same eventLoop and has been idle the shortest amount
            // of time.
            //
            // An okay match is not on the same eventLoop, and has been idle for the shortest
            // time (if the eventLoop is not enforced). If the eventLoop is enforced we take the
            // connection that has been idle the longest.
            for (index, conn) in self.connections.enumerated() {
                guard let connReturn = conn.availableAndLastReturn else {
                    continue
                }

                if conn.eventLoop === preferredEL {
                    switch eventLoopMatch {
                    case .none:
                        eventLoopMatch = (index, connReturn)
                    case .some((_, let existingMatchReturn)) where connReturn > existingMatchReturn:
                        eventLoopMatch = (index, connReturn)
                    default:
                        break
                    }
                } else {
                    switch (required, goodMatch) {
                    case (true, .none) where self.connections.count < self.maximumConcurrentConnections:
                        // If we require a specific eventLoop, and we have space for new connections,
                        // we should create a new connection if, we don't find a perfect match.
                        // We only continue the search to maybe find a perfect match.
                        break
                    case (true, .none):
                        // We require a specific eventLoop, but there is no room for a new one.
                        goodMatch = (index, connReturn)
                    case (true, .some((_, let existingMatchReturn))):
                        // We require a specific eventLoop, but there is no room for a new one.
                        if connReturn < existingMatchReturn {
                            // The current candidate has been idle for longer than our current
                            // replacement candidate. For this reason swap
                            goodMatch = (index, connReturn)
                        }
                    case (false, .none):
                        goodMatch = (index, connReturn)
                    case (false, .some((_, let existingMatchReturn))):
                        // We don't require a specific eventLoop. For this reason we want to pick a
                        // matching eventLoop that has been idle the shortest.
                        if connReturn > existingMatchReturn {
                            goodMatch = (index, connReturn)
                        }
                    }
                }
            }
            
            // if we found an eventLoopMatch, we can execute the request right away
            if let (index, _) = eventLoopMatch {
                return .elMatch(connectionIndex: index)
            }
            
            // if we found a good match, let's use this
            if let (index, _) = goodMatch {
                return .offElMatch(connectionIndex: index)
            }

            return .none
        }
        
        private func doesAnActiveConnectionExist(for eventLoop: EventLoop) -> Bool {
            self.connections.contains {
                $0.eventLoop === eventLoop
            }
        }

        mutating func newHTTP1ConnectionCreated(_ connection: Connection) -> Action {
            guard let index = self.connections.firstIndex(where: { $0.connectionID == connection.id }) else {
                preconditionFailure("There is a new connection, that we didn't request!")
            }
            
            self.connections[index].started(connection)
            
            switch self.state {
            case .running:
                return self.idleConnectionHasBecomeAvailable(&self.connections[index])
            case .shuttingDown(unclean: let unclean):
                // if we are in shutdown, we want to get rid off this connection asap.
                var connection = self.connections.remove(at: index)
                let isShutdown: StateMachine.ConnectionAction.IsShutdown
                if self.connections.isEmpty {
                    self.state = .shutDown
                    isShutdown = .yes(unclean: unclean)
                } else {
                    isShutdown = .no
                }

                return .init(.none, .closeConnection(connection.close(), isShutdown: isShutdown))

            case .shutDown:
                preconditionFailure("The pool is already shutdown all connections must already been torn down")
            }
        }
        
        private mutating func idleConnectionHasBecomeAvailable(_ connection: inout HTTP1ConnectionState) -> Action {
            guard !self.waiters.isEmpty else {
                // if we don't have any waiters, we can put this connection to idle right away
                return .init(
                    .none,
                    .scheduleTimeoutTimer(connection.connectionID)
                )
            }
            
            // if we have waiters, we probably want to execute them.
            var waiterIndex = self.waiters.startIndex
            var connectionAction: StateMachine.ConnectionAction = .none
            while waiterIndex < self.waiters.endIndex {
                // use defer increase waiter index here, to allow for early exits
                defer { waiterIndex = self.waiters.index(after: waiterIndex) }

                // check the waiters, if they are executable on this connection
                if self.waiters[waiterIndex].canBeRun(on: connection.eventLoop) {
                    let waiter = self.waiters.remove(at: waiterIndex)
                    return .init(
                        .executeRequest(waiter.request, connection.lease(), cancelWaiter: waiter.requestID),
                        connectionAction
                    )
                } else {
                    // if they are not executable on this connection, we must ensure there is 
                    // another connection that they will be run on eventually.
                    let waiterEL = self.waiters[waiterIndex].eventLoopRequirement!
                    if self.doesAnActiveConnectionExist(for: waiterEL) {
                        // if there is a connection for the waiter, we can continue. because this 
                        // waiter will be picked up by another connection in the future point. 
                        // Forward progress is guaranteed.
                        continue
                    }

                    guard case .none = connectionAction else {
                        // while we were in this loop, we already identified another connection 
                        // action that we will do. however we are looking for a waiter that we can
                        // execute on right now.
                    }

                    if self.connections.count < self.maximumConcurrentConnections {
                        // if there are less than `maximumConcurrentConnections`, we can start a new
                        // connection for this waiter.
                        let newConnectionID = self.idGenerator.next()
                        connectionAction = .createConnection(newConnectionID, on: waiterEL)
                        // we might find a task that we can execute right now.
                        continue
                    }

                    assert(self.connections.count == self.maximumConcurrentConnections)
                    // if we are at the connection maximum, and there is no connection for the 
                    // waiter, we must close our new idle connection and create a new connection
                    // on the correct EL.

                    let newConnectionID = self.idGenerator.next()
                    var replacement = HTTP1ConnectionState(connectionID: newConnectionID, eventLoop: waiterEL)
                    swap(&replacement, &connection)
                    connectionAction = .replaceConnection(replacement.close(), with: newConnectionID, on: waiterEL)
                }
            }

            // we were not able to find a task action, but we may have a connection action
            return .init(.none, connectionAction)
        }

        mutating func failedToCreateNewConnection(_ error: Error, connectionID: Connection.ID) -> Action {
            guard let index = self.connections.firstIndex(where: { $0.connectionID == connectionID }) else {
                preconditionFailure("We tried to create a new connection, that we know nothing about?")
            }

            var connectionState = self.connections[index]

            switch self.state {
            case .running:
                connectionState.failedToStart()

                if let nextWaiter = self.waiters.popFirst() {
                    assert(self.connections.count == self.maximumConcurrentConnections,
                           "Why do we have waiters, if we could open more connections?")

                    let eventLoop = nextWaiter.eventLoopRequirement ?? connectionState.eventLoop
                    let newConnectionState = HTTP1ConnectionState(
                        connectionID: self.idGenerator.next(),
                        eventLoop: eventLoop
                    )
                    self.connections[index] = newConnectionState
                    return .init(requestAction, .createConnection(newConnectionState.connectionID, on: eventLoop))
                }

                self.connections.remove(at: index)
                return .init(requestAction, .none)

            case .shuttingDown(unclean: let unclean):
                self.connections.remove(at: index)
                let isShutdown: StateMachine.ConnectionAction.IsShutdown
                if self.connections.isEmpty {
                    self.state = .shutDown
                    isShutdown = .yes(unclean: unclean)
                } else {
                    isShutdown = .no
                }

                // the cleanupAction here is pretty lazy :)
                return .init(.none, .cleanupConnection(close: [], cancel: [], isShutdown: isShutdown))

            case .shutDown:
                preconditionFailure("The pool is already shutdown all connections must already been torn down")
            }
        }

        mutating func connectionTimeout(_ connectionID: Connection.ID) -> Action {
            guard let index = self.connections.firstIndex(where: { $0.connectionID == connectionID }) else {
                // because of a race this connection (connection close runs against trigger of timeout)
                // was already removed from the state machine.
                return .init(.none, .none)
            }

            assert(self.state == .running, "If we are shutting down, we must not have any idle connections")

            var connectionState = self.connections[index]
            guard connectionState.isAvailable else {
                // connection is not available anymore, we may have just leased it for a request
                return .init(.none, .none)
            }

            assert(self.waiters.isEmpty, "We have an idle connection, that times out, but waiters? Something is very wrong!")

            self.connections.remove(at: index)
            return .init(.none, .closeConnection(connectionState.close(), isShutdown: .no))
        }

        mutating func http1ConnectionReleased(_ connectionID: Connection.ID) -> Action {
            guard let index = self.connections.firstIndex(where: { $0.connectionID == connectionID }) else {
                preconditionFailure("A connection that we don't know was released? Something is very wrong...")
            }

            var connectionState = self.connections[index]
            connectionState.release()

            switch self.state {
            case .running:
                guard let nextWaiter = self.waiters.popFirst() else {
                    // there is no more work to do immediately
                    self.connections[index] = connectionState
                    return .init(.none, .scheduleTimeoutTimer(connectionID))
                }

                assert(self.connections.count == self.maximumConcurrentConnections,
                       "Why do we have waiters, if we could open more connections?")

                guard nextWaiter.canBeRun(on: connectionState.eventLoop) else {
                    let eventLoop = nextWaiter.eventLoopRequirement!
                    let newConnection = HTTP1ConnectionState(
                        connectionID: self.idGenerator.next(),
                        eventLoop: eventLoop,
                        waiter: nextWaiter
                    )
                    self.connections[index] = newConnection
                    return .init(.none, .replaceConnection(connectionState.close(), with: newConnection.connectionID, on: eventLoop))
                }

                let connection = connectionState.lease()
                self.connections[index] = connectionState
                return .init(
                    .executeRequest(nextWaiter.request, connection, cancelWaiter: nextWaiter.requestID),
                    .none
                )

            case .shuttingDown(unclean: let unclean):
                assert(self.waiters.isEmpty, "Expected to have already cancelled all waiters")

                self.connections.remove(at: index)
                let isShutdown: StateMachine.ConnectionAction.IsShutdown
                if self.connections.isEmpty {
                    self.state = .shutDown
                    isShutdown = .yes(unclean: unclean)
                } else {
                    isShutdown = .no
                }

                return .init(.none, .closeConnection(connectionState.close(), isShutdown: isShutdown))

            case .shutDown:
                preconditionFailure("The pool is already shutdown all connections must already been torn down")
            }
        }

        /// A connection has been closed
        mutating func connectionClosed(_ connectionID: Connection.ID) -> Action {
            guard let index = self.connections.firstIndex(where: { $0.connectionID == connectionID }) else {
                // because of a race this connection (connection close runs against replace)
                // was already removed from the state machine.
                return .init(.none, .none)
            }

            switch self.state {
            case .running:
                guard let nextWaiter = self.waiters.popFirst() else {
                    self.connections.remove(at: index)
                    return .init(.none, .none)
                }

                let closedConnection = self.connections[index]
                assert(self.connections.count == self.maximumConcurrentConnections,
                       "Why do we have waiters, if we could open more connections?")

                let eventLoop = nextWaiter.eventLoopRequirement ?? closedConnection.eventLoop
                let newConnection = HTTP1ConnectionState(
                    connectionID: self.idGenerator.next(),
                    eventLoop: eventLoop,
                    waiter: nextWaiter
                )
                self.connections[index] = newConnection
                return .init(.none, .createConnection(newConnection.connectionID, on: eventLoop))

            case .shuttingDown(unclean: let unclean):
                assert(self.waiters.isEmpty, "Expected to have already cancelled all waiters")

                self.connections.remove(at: index)
                if self.connections.isEmpty {
                    self.state = .shutDown
                    return .init(.none, .cleanupConnection(close: [], cancel: [], isShutdown: .yes(unclean: unclean)))
                } else {
                    return .init(.none, .none)
                }

            case .shutDown:
                preconditionFailure("The pool is already shutdown all connections must already been torn down")
            }
        }

        mutating func timeoutWaiter(_ requestID: RequestID) -> Action {
            // 1. check waiters in starting connections
            let connectionIndex = self.connections.firstIndex(where: {
                $0.isStarting(for: requestID)
            })

            if let connectionIndex = connectionIndex {
                var connectionState = self.connections[connectionIndex]
                var requestAction: StateMachine.RequestAction = .none
                if let waiter = connectionState.removeStartWaiter() {
                    requestAction = .failRequest(waiter.request, HTTPClientError.connectTimeout, cancelWaiter: nil)
                }
                self.connections[connectionIndex] = connectionState

                return .init(requestAction, .none)
            }

            // 2. check waiters in queue
            let waiterIndex = self.waiters.firstIndex(where: { $0.requestID == requestID })
            if let waiterIndex = waiterIndex {
                // TBD: This is slow. Do we maybe want something more sophisticated here?
                let waiter = self.waiters.remove(at: waiterIndex)
                return .init(
                    .failRequest(waiter.request, HTTPClientError.getConnectionFromPoolTimeout, cancelWaiter: nil),
                    .none
                )
            }

            // 3. we reach this point, because the waiter may already have been scheduled. The waiter
            //    was not cancelled because of a race condition
            return .init(.none, .none)
        }

        mutating func cancelWaiter(_ requestID: RequestID) -> Action {
            // 1. check waiters in starting connections
            let connectionIndex = self.connections.firstIndex(where: {
                $0.isStarting(for: requestID)
            })

            if let connectionIndex = connectionIndex {
                var connectionState = self.connections[connectionIndex]
                var requestAction: StateMachine.RequestAction = .none
                if let waiter = connectionState.removeStartWaiter() {
                    requestAction = .failRequest(waiter.request, HTTPClientError.cancelled, cancelWaiter: waiter.requestID)
                }
                self.connections[connectionIndex] = connectionState

                return .init(requestAction, .none)
            }

            // 2. check waiters in queue
            let waiterIndex = self.waiters.firstIndex(where: { $0.requestID == requestID })
            if let waiterIndex = waiterIndex {
                // TBD: This is potentially slow. Do we maybe want something more sophisticated here?
                let waiter = self.waiters.remove(at: waiterIndex)
                return .init(
                    .failRequest(waiter.request, HTTPClientError.cancelled, cancelWaiter: requestID),
                    .none
                )
            }

            // 3. we reach this point, because the waiter may already have been forwarded to an
            //    idle connection. The connection will need to handle the cancellation in that case.
            return .init(.none, .none)
        }

        mutating func shutdown() -> Action {
            precondition(self.state == .running, "Shutdown must only be called once")

            var requestAction: StateMachine.RequestAction = .none

            // If we have remaining waiters, we should fail all of them with a cancelled error
            var requests = self.waiters.map { ($0.request, $0.requestID) }
            self.waiters.removeAll()

            var close = [Connection]()
            var cancel = [Connection]()

            self.connections = self.connections.compactMap { connectionState -> HTTPConnectionPool.HTTP1ConnectionState? in
                var connectionState = connectionState

                if connectionState.isStarting {
                    // starting connections cant be cancelled so far... we will need to wait until
                    // the connection starts up or fails.

                    if let waiter = connectionState.removeStartWaiter() {
                        requests.append((waiter.request, waiter.requestID))
                    }

                    return connectionState
                } else if connectionState.isAvailable {
                    close.append(connectionState.close())
                    return nil
                } else if connectionState.isLeased {
                    cancel.append(connectionState.cancel())
                    return connectionState
                }

                preconditionFailure("Must not be reached. Any of the above conditions should be true")
            }

            // If there aren't any more connections, everything is shutdown
            let isShutdown: StateMachine.ConnectionAction.IsShutdown
            let unclean = !(cancel.isEmpty && requests.isEmpty)
            if self.connections.isEmpty {
                self.state = .shutDown
                isShutdown = .yes(unclean: unclean)
            } else {
                self.state = .shuttingDown(unclean: unclean)
                isShutdown = .no
            }

            if !requests.isEmpty {
                requestAction = .failRequests(requests, HTTPClientError.cancelled)
            }

            return .init(requestAction, .cleanupConnection(close: close, cancel: cancel, isShutdown: isShutdown))
        }
    }
}

extension HTTPConnectionPool.HTTP1StateMachine: CustomStringConvertible {
    var description: String {
        var starting = 0
        var leased = 0
        var parked = 0

        for connectionState in self.connections {
            if connectionState.isStarting {
                starting += 1
            } else if connectionState.isLeased {
                leased += 1
            } else if connectionState.isAvailable {
                parked += 1
            }
        }

        let waiters = self.waiters.count

        return "connections: [starting: \(starting) | leased: \(leased) | parked: \(parked)], waiters: \(waiters)"
    }
}
