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
            /// the connection is creating a connection. Valid transitions are to: .backingOff, .available and .failed
            case starting(retries: Int)
            /// the connection is waiting to retry the establishing a connection. Valid transitions to: .starting and .closed
            case backingOff(retries: Int)
            case available(Connection, since: NIODeadline)
            case leased(Connection)
            case failed
            case closed
        }

        private var state: State
        private(set) var connectionID: Connection.ID
        let eventLoop: EventLoop

        init(connectionID: Connection.ID, eventLoop: EventLoop, retries: Int = 0) {
            self.connectionID = connectionID
            self.eventLoop = eventLoop
            self.state = .starting(retries: retries)
        }

        var isConnecting: Bool {
            switch self.state {
            case .starting:
                return true
            case .backingOff, .failed, .closed, .available, .leased:
                return false
            }
        }

        var isBackingOff: Bool {
            switch self.state {
            case .backingOff:
                return true
            case .starting, .failed, .closed, .available, .leased:
                return false
            }
        }

        var isAvailable: Bool {
            switch self.state {
            case .available:
                return true
            case .backingOff, .starting, .leased, .failed, .closed:
                return false
            }
        }

        var isLeased: Bool {
            switch self.state {
            case .leased:
                return true
            case .backingOff, .starting, .available, .failed, .closed:
                return false
            }
        }

        var availableAndLastReturn: NIODeadline? {
            switch self.state {
            case .available(_, since: let lastReturn):
                return lastReturn
            case .backingOff, .starting, .leased, .failed, .closed:
                return nil
            }
        }

        mutating func started(_ connection: Connection) {
            switch self.state {
            case .starting:
                self.state = .available(connection, since: .now())
            case .backingOff, .available, .leased, .failed, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        /// The connection failed to start
        /// - Returns: How often the connection failed to start. Use this int to calculate backoff intervals.
        mutating func failedToStart() -> Int {
            switch self.state {
            case .starting(let retries):
                self.state = .backingOff(retries: retries + 1)
                return retries
            case .backingOff, .available, .leased, .failed, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func retryConnecting(newConnectionID: Connection.ID) {
            switch self.state {
            case .backingOff(let retries):
                self.connectionID = newConnectionID
                self.state = .starting(retries: retries)
            case .starting, .available, .leased, .failed, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func lease() -> Connection {
            switch self.state {
            case .available(let connection, since: _):
                self.state = .leased(connection)
                return connection
            case .backingOff, .starting, .leased, .failed, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func release() {
            switch self.state {
            case .leased(let connection):
                self.state = .available(connection, since: .now())
            case .backingOff, .starting, .available, .failed, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func close() -> Connection {
            switch self.state {
            case .available(let connection, since: _):
                self.state = .closed
                return connection
            case .backingOff, .starting, .leased, .failed, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func cancel() -> Connection {
            switch self.state {
            case .leased(let connection):
                return connection
            case .backingOff, .starting, .available, .failed, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func cleanup(_ context: inout StateMachine.ConnectionAction.CleanupContext) -> Bool {
            switch self.state {
            case .backingOff:
                context.connectBackoff.append(self.connectionID)
                return true
            case .starting:
                return false
            case .available(let connection, since: _):
                context.close.append(connection)
                return true
            case .leased(let connection):
                context.cancel.append(connection)
                return false
            case .failed, .closed:
                preconditionFailure("Unexpected state: Did not expect to have connections with this state in the state machine: \(self.state)")
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

        private var queue: CircularBuffer<Waiter>
        private var state: State = .running

        init(idGenerator: Connection.ID.Generator, maximumConcurrentConnections: Int) {
            self.idGenerator = idGenerator
            self.maximumConcurrentConnections = maximumConcurrentConnections
            self.connections = []
            self.connections.reserveCapacity(self.maximumConcurrentConnections)

            self.queue = CircularBuffer(initialCapacity: 32)
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

            if required {
                preconditionFailure("EL requirements not supported yet.")
            }

            if let index = self.findAvailableConnectionIndex(onPreferred: preferredEL) {
                let connection = self.connections[index].lease()
                return .init(
                    .executeRequest(request, connection, cancelWaiter: nil),
                    .cancelTimeoutTimer(connection.id)
                )
            }

            // No matter what we do now, the request will need to wait!
            let newWaiter = Waiter(request: request)
            self.queue.append(newWaiter)

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

        mutating func newHTTP1ConnectionCreated(_ connection: Connection) -> Action {
            guard let index = self.connections.firstIndex(where: { $0.connectionID == connection.id }) else {
                preconditionFailure("There is a new connection, that we didn't request!")
            }

            self.connections[index].started(connection)
            return self.nextActionForIdleConnection(connectionIndex: index)
        }

        mutating func failedToCreateNewConnection(_ error: Error, connectionID: Connection.ID) -> Action {
            guard let index = self.connections.firstIndex(where: { $0.connectionID == connectionID }) else {
                preconditionFailure("We tried to create a new connection, that we know nothing about?")
            }

            switch self.state {
            case .running:
                let eventLoop = self.connections[index].eventLoop
                let retries = self.connections[index].failedToStart()

                let backoff = TimeAmount.milliseconds(100) * (2 ^ retries)
                let jitterRange = backoff.nanoseconds / 100 * 5
                let jitteredBackoff = backoff + .nanoseconds((-jitterRange...jitterRange).randomElement()!)
                return .init(.none, .scheduleBackoffTimer(connectionID, backoff: jitteredBackoff, on: eventLoop))

            case .shuttingDown:
                return self.removeFailedOrClosedConnectionForShutdown(connectionIndex: index)

            case .shutDown:
                preconditionFailure("The pool is already shutdown all connections must already been torn down")
            }
        }

        mutating func connectionCreationBackoffDone(_ connectionID: Connection.ID) -> Action {
            guard let index = self.connections.firstIndex(where: { $0.connectionID == connectionID }) else {
                // this might have been triggered, after we discarded the connection. Therefore we can
                // ignore this message
                return .none
            }

            let waiting = self.queue.count
            let stats = self.stats

            assert(stats.backingOff >= 1, "This connection is currently in backoff")

            // if there are more requests waiting, than we have starting connections, we should
            // start this connection once more. We expect that it will be used.
            if waiting > stats.connecting {
                let newConnectionID = self.idGenerator.next()
                let eventLoop = self.connections[index].eventLoop
                self.connections[index].retryConnecting(newConnectionID: newConnectionID)
                return .init(.none, .createConnection(newConnectionID, on: eventLoop))
            }

            // if we have more starting connections, than requests queued, we don't need to retry
            // this connection. Instead we should remove it.
            self.connections.remove(at: index)
            return .none
        }

        mutating func connectionIdleTimeout(_ connectionID: Connection.ID) -> Action {
            guard let index = self.connections.firstIndex(where: { $0.connectionID == connectionID }) else {
                // because of a race this connection (connection close runs against trigger of timeout)
                // was already removed from the state machine.
                return .none
            }

            assert(self.state == .running, "If we are shutting down, we must not have any idle connections")

            var connectionState = self.connections[index]
            guard connectionState.isAvailable else {
                // connection is not available anymore, we may have just leased it for a request
                return .none
            }

            assert(self.queue.isEmpty, "We have an idle connection, that times out, but waiters? Something is very wrong!")

            self.connections.remove(at: index)
            return .init(.none, .closeConnection(connectionState.close(), isShutdown: .no))
        }

        mutating func http1ConnectionReleased(_ connectionID: Connection.ID) -> Action {
            guard let index = self.connections.firstIndex(where: { $0.connectionID == connectionID }) else {
                preconditionFailure("A connection that we don't know was released? Something is very wrong...")
            }

            self.connections[index].release()
            return self.nextActionForIdleConnection(connectionIndex: index)
        }

        /// A connection has been closed
        mutating func connectionClosed(_ connectionID: Connection.ID) -> Action {
            guard let index = self.connections.firstIndex(where: { $0.connectionID == connectionID }) else {
                // because of a race this connection (connection close runs against connection idle
                // timeout timer) was already removed from the state machine.
                return .none
            }

            switch self.state {
            case .running:
                let waiterCount = self.queue.count
                guard waiterCount > 0 else {
                    self.connections.remove(at: index)
                    return .none
                }

                let closedConnection = self.connections[index]
                let newConnection = HTTP1ConnectionState(
                    connectionID: self.idGenerator.next(),
                    eventLoop: closedConnection.eventLoop
                )
                self.connections[index] = newConnection
                return .init(.none, .createConnection(newConnection.connectionID, on: newConnection.eventLoop))

            case .shuttingDown:
                return self.removeFailedOrClosedConnectionForShutdown(connectionIndex: index)

            case .shutDown:
                preconditionFailure("The pool is already shutdown all connections must already been torn down")
            }
        }

        mutating func timeoutWaiter(_ requestID: RequestID) -> Action {
            // 1. check waiters in queue
            let waiterIndex = self.queue.firstIndex(where: { $0.requestID == requestID })
            if let waiterIndex = waiterIndex {
                // TBD: This is slow. Do we maybe want something more sophisticated here?
                let waiter = self.queue.remove(at: waiterIndex)
                return .init(
                    .failRequest(waiter.request, HTTPClientError.getConnectionFromPoolTimeout, cancelWaiter: nil),
                    .none
                )
            }

            // 2. we reach this point, because the waiter may already have been scheduled. A
            //    connection might have become available very shortly before the waiter timed out.
            return .none
        }

        mutating func cancelWaiter(_ requestID: RequestID) -> Action {
            // 1. check waiters in queue
            let waiterIndex = self.queue.firstIndex(where: { $0.requestID == requestID })
            if let waiterIndex = waiterIndex {
                // TBD: This is potentially slow. Do we maybe want something more sophisticated here?
                self.queue.remove(at: waiterIndex)
                return .init(
                    .cancelWaiterTimeout(requestID),
                    .none
                )
            }

            // 2. we reach this point, because the waiter may already have been forwarded to an
            //    idle connection. The connection will need to handle the cancellation in that case.
            return .none
        }

        mutating func shutdown() -> Action {
            precondition(self.state == .running, "Shutdown must only be called once")

            // If we have remaining waiters, we should fail all of them with a cancelled error
            let waitingRequests = self.queue.map { ($0.request, $0.requestID) }
            self.queue.removeAll()

            var cleanupContext = StateMachine.ConnectionAction.CleanupContext()
            self.connections = self.connections.compactMap { connectionState in
                var connectionState = connectionState
                if connectionState.cleanup(&cleanupContext) {
                    return nil
                }
                return connectionState
            }

            // If there aren't any more connections, everything is shutdown
            let isShutdown: StateMachine.ConnectionAction.IsShutdown
            let unclean = !(cleanupContext.cancel.isEmpty && waitingRequests.isEmpty)
            if self.connections.isEmpty {
                self.state = .shutDown
                isShutdown = .yes(unclean: unclean)
            } else {
                self.state = .shuttingDown(unclean: unclean)
                isShutdown = .no
            }

            var requestAction: StateMachine.RequestAction = .none
            if !waitingRequests.isEmpty {
                requestAction = .failRequests(waitingRequests, HTTPClientError.cancelled)
            }

            return .init(requestAction, .cleanupConnections(cleanupContext, isShutdown: isShutdown))
        }

        // MARK: - Private Methods -

        private func findAvailableConnectionIndex(onPreferred preferredEL: EventLoop)
            -> Int? {
            var eventLoopMatch: (Int, NIODeadline)?
            var goodMatch: (Int, NIODeadline)?

            // To find an appropriate connection we iterate all existing connections.
            // While we do this we try to find the best fitting connection for our request.
            //
            // A perfect match, runs on the same eventLoop and has been idle the shortest amount
            // of time.
            //
            // An okay match is not on the same eventLoop, and has been idle for the shortest
            // time.
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
                    switch goodMatch {
                    case .none:
                        goodMatch = (index, connReturn)
                    case .some((_, let existingMatchReturn)):
                        // We don't require a specific eventLoop. For this reason we want to pick a
                        // matching eventLoop that has been idle the shortest.
                        if connReturn > existingMatchReturn {
                            goodMatch = (index, connReturn)
                        }
                    }
                }
            }

            if let (index, _) = eventLoopMatch {
                return index
            }

            if let (index, _) = goodMatch {
                return index
            }

            return nil
        }

        private mutating func nextActionForIdleConnection(connectionIndex index: Int) -> Action {
            assert(self.connections[index].isAvailable, "Connection at index: \(index) must be available")

            switch self.state {
            case .running:
                guard !self.queue.isEmpty else {
                    return .init(.none, .scheduleTimeoutTimer(self.connections[index].connectionID))
                }

                let waiter = self.queue.removeFirst()
                return .init(
                    .executeRequest(waiter.request, self.connections[index].lease(), cancelWaiter: waiter.requestID),
                    .none
                )

            case .shuttingDown:
                return self.closeIdleConnectionForShutdown(connectionIndex: index)

            case .shutDown:
                preconditionFailure("It the pool is already shutdown, all connections must have been torn down.")
            }
        }

        private mutating func closeIdleConnectionForShutdown(connectionIndex index: Int) -> Action {
            guard case .shuttingDown(unclean: let unclean) = self.state else {
                preconditionFailure("This method must only be called, if in shutdown. Invalid state: \(self.state)")
            }

            assert(self.queue.isEmpty, "Expected to have already cancelled all waiters")
            // if we are in shutdown, we want to get rid off this connection asap.
            var connectionState = self.connections.remove(at: index)
            if self.connections.isEmpty {
                self.state = .shutDown
                return .init(
                    .none,
                    .closeConnection(connectionState.close(), isShutdown: .yes(unclean: unclean))
                )
            } else {
                return .init(
                    .none,
                    .closeConnection(connectionState.close(), isShutdown: .no)
                )
            }
        }

        private mutating func removeFailedOrClosedConnectionForShutdown(connectionIndex index: Int) -> Action {
            guard case .shuttingDown(unclean: let unclean) = self.state else {
                preconditionFailure("This method must only be called, if in shutdown. Invalid state: \(self.state)")
            }

            assert(self.queue.isEmpty, "Expected to have already cancelled all waiters")
            self.connections.remove(at: index)
            if self.connections.isEmpty {
                self.state = .shutDown
                return .init(
                    .none,
                    .cleanupConnections(.init(), isShutdown: .yes(unclean: unclean))
                )
            } else {
                return .none
            }
        }

        struct Stats {
            var idle: Int = 0
            var leased: Int = 0
            var connecting: Int = 0
            var backingOff: Int = 0
        }

        private var stats: Stats {
            var stats = Stats()
            for connectionState in self.connections {
                if connectionState.isConnecting {
                    stats.connecting += 1
                } else if connectionState.isBackingOff {
                    stats.backingOff += 1
                } else if connectionState.isLeased {
                    stats.leased += 1
                } else if connectionState.isAvailable {
                    stats.idle += 1
                }
            }
            return stats
        }
    }
}

extension HTTPConnectionPool.HTTP1StateMachine: CustomStringConvertible {
    var description: String {
        let stats = self.stats
        let waiters = self.queue.count

        return "connections: [connecting: \(stats.connecting) | backoff: \(stats.backingOff) | leased: \(stats.leased) | idle: \(stats.idle)], waiters: \(waiters)"
    }
}
