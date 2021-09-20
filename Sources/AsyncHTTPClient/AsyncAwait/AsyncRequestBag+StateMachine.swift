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

import Darwin
import Logging
import NIO
import NIOHTTP1

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
extension AsyncRequestBag {
    struct StateMachine {
        struct ExecutionContext {
            let executor: HTTPRequestExecutor
            let allocator: ByteBufferAllocator
            let continuation: UnsafeContinuation<AsyncResponse, Error>
        }

        private enum State {
            case initialized
            case waiting(UnsafeContinuation<AsyncResponse, Error>)
            case queued(UnsafeContinuation<AsyncResponse, Error>, HTTPRequestScheduler)
            case executing(ExecutionContext, RequestStreamState, ResponseStreamState)
            case finished(error: Error?, AsyncResponse.Body.IteratorStream.ID?)
            case modifying
        }

        fileprivate enum RequestStreamState {
            case initialized
            case producing
            case paused
            case finished
        }

        fileprivate enum ResponseStreamState {
            enum Next {
                case askExecutorForMore
                case error(Error)
                case eof
            }

            case initialized
            case waitingForStream(CircularBuffer<ByteBuffer>, next: Next)
            case buffering(AsyncResponse.Body.IteratorStream.ID, CircularBuffer<ByteBuffer>, next: Next)
            case waitingForRemote(AsyncResponse.Body.IteratorStream.ID, UnsafeContinuation<ByteBuffer?, Error>)
            case finished(AsyncResponse.Body.IteratorStream.ID, UnsafeContinuation<ByteBuffer?, Error>)
        }

        private var state: State

        init() {
            self.state = .initialized
        }

        mutating func registerContinuation(_ continuation: UnsafeContinuation<AsyncResponse, Error>) {
            guard case .initialized = self.state else {
                preconditionFailure("Invalid state: \(self.state)")
            }

            self.state = .waiting(continuation)
        }

        mutating func requestWasQueued(_ scheduler: HTTPRequestScheduler) {
            guard case .waiting(let continuation) = self.state else {
                // There might be a race between `requestWasQueued` and `willExecuteRequest`:
                //
                // If the request is created and passed to the HTTPClient on thread A, it will move into
                // the connection pool lock in thread A. If no connection is available, thread A will
                // add the request to the waiters and leave the connection pool lock.
                // `requestWasQueued` will be called outside the connection pool lock on thread A.
                // However if thread B has a connection that becomes available and thread B enters the
                // connection pool lock directly after thread A, the request will be immediately
                // scheduled for execution on thread B. After the thread B has left the lock it will
                // call `willExecuteRequest` directly after.
                //
                // Having an order in the connection pool lock, does not guarantee an order in calling:
                // `requestWasQueued` and `willExecuteRequest`.
                //
                // For this reason we must check the state here... If we are not `.initialized`, we are
                // already executing.
                return
            }

            self.state = .queued(continuation, scheduler)
        }

        enum FailAction {
            case none
            case failContinuation(UnsafeContinuation<AsyncResponse, Error>, Error, HTTPRequestScheduler?, HTTPRequestExecutor?)
            case failResponseStream(UnsafeContinuation<ByteBuffer?, Error>, Error, HTTPRequestExecutor)
        }

        mutating func fail(_ error: Error) -> FailAction {
            switch self.state {
            case .initialized:
                preconditionFailure("")

            case .waiting(let continuation):
                self.state = .finished(error: error, nil)
                return .failContinuation(continuation, error, nil, nil)

            case .queued(let continuation, let scheduler):
                self.state = .finished(error: error, nil)
                return .failContinuation(continuation, error, scheduler, nil)

            case .executing(let context, _, .initialized):
                self.state = .finished(error: error, nil)
                return .failContinuation(context.continuation, error, nil, context.executor)

            case .executing(_, _, .waitingForStream(_, next: .error)),
                 .executing(_, _, .buffering(_, _, next: .error)):
                return .none

            case .executing(let context, let requestStreamState, .waitingForStream(let buffer, next: .askExecutorForMore)),
                 .executing(let context, let requestStreamState, .waitingForStream(let buffer, next: .eof)):
                switch requestStreamState {
                case .initialized:
                    preconditionFailure("Invalid state")

                case .paused, .finished:
                    self.state = .executing(context, requestStreamState, .waitingForStream(buffer, next: .error(error)))
                    return .none

                case .producing:
                    self.state = .executing(context, .paused, .waitingForStream(buffer, next: .error(error)))
                    return .none
                }

            case .executing(let context, let requestStreamState, .buffering(let streamID, let buffer, next: .askExecutorForMore)),
                 .executing(let context, let requestStreamState, .buffering(let streamID, let buffer, next: .eof)):

                switch requestStreamState {
                case .initialized:
                    preconditionFailure("Invalid state")

                case .paused, .finished:
                    self.state = .executing(context, requestStreamState, .buffering(streamID, buffer, next: .error(error)))
                    return .none

                case .producing:
                    self.state = .executing(context, .paused, .buffering(streamID, buffer, next: .error(error)))
                    return .none
                }

            case .executing(let context, _, .waitingForRemote(let streamID, let continuation)):
                self.state = .finished(error: error, streamID)
                return .failResponseStream(continuation, error, context.executor)

            case .finished(error: _, _):
                return .none

            case .modifying:
                preconditionFailure("Invalid state")

            case .executing(let context, _, .finished(let streamID, let continuation)):
                self.state = .finished(error: error, streamID)
                return .failResponseStream(continuation, error, context.executor)
            }
        }

        // MARK: - Request -

        mutating func willExecuteRequest(_ executor: HTTPRequestExecutor) -> Bool {
            switch self.state {
            case .waiting(let continuation), .queued(let continuation, _):
                let context = ExecutionContext(
                    executor: executor,
                    allocator: .init(),
                    continuation: continuation
                )
                self.state = .executing(context, .initialized, .initialized)
                return true
            case .finished(error: .some, .none):
                return false
            case .initialized,
                 .executing,
                 .finished(error: .none, _),
                 .finished(error: .some, .some),
                 .modifying:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        enum ResumeProducingAction {
            case resumeStream(ByteBufferAllocator)
            case none
        }

        mutating func resumeRequestBodyStream() -> ResumeProducingAction {
            switch self.state {
            case .initialized, .waiting, .queued:
                preconditionFailure("A request stream can only be resumed, if the request was started")

            case .executing(let context, .initialized, .initialized):
                self.state = .executing(context, .producing, .initialized)
                return .resumeStream(context.allocator)

            case .executing(_, .producing, _):
                preconditionFailure("Expected that resume is only called when if we were paused before")

            case .executing(let context, .paused, let responseState):
                self.state = .executing(context, .producing, responseState)
                return .resumeStream(context.allocator)

            case .executing(_, .finished, _):
                // the channels writability changed to writable after we have forwarded all the
                // request bytes. Can be ignored.
                return .none

            case .executing(_, .initialized, .waitingForStream),
                 .executing(_, .initialized, .buffering),
                 .executing(_, .initialized, .waitingForRemote),
                 .executing(_, .initialized, .finished):
                preconditionFailure("Invalid states: Response can not be received before request")

            case .finished:
                return .none

            case .modifying:
                preconditionFailure("Invalid state")
            }
        }

        mutating func pauseRequestBodyStream() {
            switch self.state {
            case .initialized,
                 .waiting,
                 .queued,
                 .executing(_, .initialized, _):
                preconditionFailure("A request stream can only be resumed, if the request was started")

            case .executing(let context, .producing, let responseSteam):
                self.state = .executing(context, .paused, responseSteam)

            case .executing(_, .paused, _),
                 .executing(_, .finished, _),
                 .finished:
                // the channels writability changed to writable after we have forwarded all the
                // request bytes. Can be ignored.
                break

            case .modifying:
                preconditionFailure("Invalid state")
            }
        }

        enum NextWriteAction {
            case write(IOData, HTTPRequestExecutor, continue: Bool)
            case ignore
        }

        func producedNextRequestPart(_ part: IOData) -> NextWriteAction {
            switch self.state {
            case .initialized,
                 .waiting,
                 .queued,
                 .executing(_, .initialized, _),
                 .executing(_, .finished, _):
                preconditionFailure("A request stream can only be resumed, if the request was started")

            case .executing(let context, .producing, _):
                return .write(part, context.executor, continue: true)

            case .executing(let context, .paused, _):
                return .write(part, context.executor, continue: false)

            case .finished:
                return .ignore

            case .modifying:
                preconditionFailure("Invalid state")
            }
        }

        enum ProduceErrorAction {
            case none
            case informRequestAboutFailure(Error, cancelExecutor: HTTPRequestExecutor, failResponseStream: UnsafeContinuation<ByteBuffer?, Error>?)
        }

        mutating func failedToProduceNextRequestPart(_ error: Error) -> ProduceErrorAction {
            switch self.state {
            case .initialized,
                 .waiting,
                 .queued,
                 .executing(_, .initialized, _),
                 .executing(_, .finished, _):
                preconditionFailure("A request stream can only be resumed, if the request was started")

            case .executing(let context, .producing, .initialized),
                 .executing(let context, .producing, .waitingForStream),
                 .executing(let context, .paused, .initialized),
                 .executing(let context, .paused, .waitingForStream):
                self.state = .finished(error: error, nil)
                return .informRequestAboutFailure(error, cancelExecutor: context.executor, failResponseStream: nil)

            case .executing(let context, .producing, .buffering(let streamID, _, next: _)),
                 .executing(let context, .paused, .buffering(let streamID, _, next: _)):
                self.state = .finished(error: error, streamID)
                return .informRequestAboutFailure(error, cancelExecutor: context.executor, failResponseStream: nil)

            case .executing(let context, .producing, .waitingForRemote(let streamID, let continuation)),
                 .executing(let context, .paused, .waitingForRemote(let streamID, let continuation)),
                 .executing(let context, .producing, .finished(let streamID, let continuation)),
                 .executing(let context, .paused, .finished(let streamID, let continuation)):
                self.state = .finished(error: error, streamID)
                return .informRequestAboutFailure(error, cancelExecutor: context.executor, failResponseStream: continuation)

            case .finished:
                return .none

            case .modifying:
                preconditionFailure("Invalid state")
            }
        }

        enum FinishAction {
            case forwardStreamFinished(HTTPRequestExecutor)
            case none
        }

        mutating func finishRequestBodyStream() -> FinishAction {
            switch self.state {
            case .initialized,
                 .waiting,
                 .queued,
                 .executing(_, .initialized, _),
                 .executing(_, .finished, _):
                preconditionFailure("Invalid state: \(self.state)")

            case .executing(let context, .producing, let responseState),
                 .executing(let context, .paused, let responseState):
                self.state = .executing(context, .finished, responseState)
                return .forwardStreamFinished(context.executor)

            case .finished:
                return .none

            case .modifying:
                preconditionFailure("Invalid state")
            }
        }

        // MARK: - Response -

        enum ReceiveResponseHeadAction {
            case succeedResponseHead(HTTPResponseHead, UnsafeContinuation<AsyncResponse, Error>)
            case none
        }

        mutating func receiveResponseHead(_ head: HTTPResponseHead) -> ReceiveResponseHeadAction {
            switch self.state {
            case .initialized,
                 .waiting,
                 .queued,
                 .executing(_, _, .waitingForStream),
                 .executing(_, _, .buffering),
                 .executing(_, _, .waitingForRemote):
                preconditionFailure("How can we receive a response, if the request hasn't started yet.")

            case .executing(let context, let requestState, .initialized):
                self.state = .executing(context, requestState, .waitingForStream(.init(), next: .askExecutorForMore))
                return .succeedResponseHead(head, context.continuation)

            case .finished(error: .some, _):
                return .none

            case .executing(_, _, .finished),
                 .finished(error: .none, _):
                preconditionFailure("How can the request be finished without error, before receiving response head?")

            case .modifying:
                preconditionFailure("Invalid state")
            }
        }

        enum ReceiveResponsePartAction {
            case none
            case succeedContinuation(UnsafeContinuation<ByteBuffer?, Error>, ByteBuffer)
        }

        mutating func receiveResponseBodyParts(_ buffer: CircularBuffer<ByteBuffer>) -> ReceiveResponsePartAction {
            switch self.state {
            case .initialized, .waiting, .queued:
                preconditionFailure("How can we receive a response body part, if the request hasn't started yet.")
            case .executing(_, _, .initialized):
                preconditionFailure("If we receive a response body, we must have received a head before")

            case .executing(let context, let requestState, .buffering(let streamID, var currentBuffer, next: let next)):
                guard case .askExecutorForMore = next else {
                    preconditionFailure("If we have received an error or eof before, why did we get another body part? Next: \(next)")
                }

                self.state = .modifying
                if currentBuffer.isEmpty {
                    currentBuffer = buffer
                } else {
                    currentBuffer.append(contentsOf: buffer)
                }
                self.state = .executing(context, requestState, .buffering(streamID, currentBuffer, next: next))
                return .none

            case .executing(let executor, let requestState, .waitingForStream(var currentBuffer, next: let next)):
                guard case .askExecutorForMore = next else {
                    preconditionFailure("If we have received an error or eof before, why did we get another body part? Next: \(next)")
                }

                self.state = .modifying
                if currentBuffer.isEmpty {
                    currentBuffer = buffer
                } else {
                    currentBuffer.append(contentsOf: buffer)
                }
                self.state = .executing(executor, requestState, .waitingForStream(currentBuffer, next: next))
                return .none

            case .executing(let executor, let requestState, .waitingForRemote(let streamID, let continuation)):
                var buffer = buffer
                let first = buffer.removeFirst()
                self.state = .executing(executor, requestState, .buffering(streamID, buffer, next: .askExecutorForMore))
                return .succeedContinuation(continuation, first)

            case .finished(error: .some, _):
                return .none
            case .executing(_, _, .finished),
                 .finished(error: .none, _):
                preconditionFailure("How can the request be finished without error, before receiving response head?")
            case .modifying:
                preconditionFailure("Invalid state")
            }
        }

        enum ConsumeAction {
            case succeedContinuation(UnsafeContinuation<ByteBuffer?, Error>, ByteBuffer?)
            case failContinuation(UnsafeContinuation<ByteBuffer?, Error>, Error)
            case askExecutorForMore(HTTPRequestExecutor)
        }

        struct TriedToRegisteredASecondConsumer: Error {}

        mutating func consumeNextResponsePart(
            streamID: AsyncResponse.Body.IteratorStream.ID,
            continuation: UnsafeContinuation<ByteBuffer?, Error>
        ) -> ConsumeAction {
            switch self.state {
            case .initialized,
                 .waiting,
                 .queued,
                 .executing(_, _, .initialized):
                preconditionFailure("If we receive a response body, we must have received a head before")

            case .executing(_, _, .finished(_, _)):
                preconditionFailure("This is an invalid state at this point. We are waiting for the request stream to finish to succeed or response stream.")

            case .executing(let context, let requestState, .waitingForStream(var buffer, next: .askExecutorForMore)):
                if buffer.isEmpty {
                    self.state = .executing(context, requestState, .waitingForRemote(streamID, continuation))
                    return .askExecutorForMore(context.executor)
                } else {
                    self.state = .modifying
                    let toReturn = buffer.removeFirst()
                    self.state = .executing(context, requestState, .buffering(streamID, buffer, next: .askExecutorForMore))
                    return .succeedContinuation(continuation, toReturn)
                }

            case .executing(_, _, .waitingForStream(_, next: .error(let error))):
                self.state = .finished(error: error, streamID)
                return .failContinuation(continuation, error)

            case .executing(_, _, .waitingForStream(let buffer, next: .eof)) where buffer.isEmpty:
                self.state = .finished(error: nil, streamID)
                return .succeedContinuation(continuation, nil)

            case .executing(let context, let requestState, .waitingForStream(var buffer, next: .eof)):
                assert(!buffer.isEmpty)
                self.state = .modifying
                let toReturn = buffer.removeFirst()
                self.state = .executing(context, requestState, .buffering(streamID, buffer, next: .eof))
                return .succeedContinuation(continuation, toReturn)

            case .executing(let context, let requestState, .buffering(let streamID, var buffer, next: .askExecutorForMore)):
                if buffer.isEmpty {
                    self.state = .executing(context, requestState, .waitingForRemote(streamID, continuation))
                    return .askExecutorForMore(context.executor)
                } else {
                    self.state = .modifying
                    let toReturn = buffer.removeFirst()
                    self.state = .executing(context, requestState, .buffering(streamID, buffer, next: .askExecutorForMore))
                    return .succeedContinuation(continuation, toReturn)
                }

            case .executing(_, _, .buffering(let registeredStreamID, _, next: .error(let error))):
                guard registeredStreamID == streamID else {
                    return .failContinuation(continuation, TriedToRegisteredASecondConsumer())
                }
                self.state = .finished(error: error, registeredStreamID)
                return .failContinuation(continuation, error)

            case .executing(_, _, .buffering(let registeredStreamID, let buffer, next: .eof)) where buffer.isEmpty:
                guard registeredStreamID == streamID else {
                    return .failContinuation(continuation, TriedToRegisteredASecondConsumer())
                }
                self.state = .finished(error: nil, registeredStreamID)
                return .succeedContinuation(continuation, nil)

            case .executing(let context, let requestState, .buffering(let streamID, var buffer, next: .eof)):
                assert(!buffer.isEmpty)
                self.state = .modifying
                let toReturn = buffer.removeFirst()
                self.state = .executing(context, requestState, .buffering(streamID, buffer, next: .eof))
                return .succeedContinuation(continuation, toReturn)

            case .executing(_, _, .waitingForRemote(let registeredStreamID, let continuation)):
                if registeredStreamID != streamID {
                    return .failContinuation(continuation, TriedToRegisteredASecondConsumer())
                }
                preconditionFailure("")

            case .finished(error: .some(let error), let registeredStreamID):
                guard registeredStreamID == streamID else {
                    return .failContinuation(continuation, TriedToRegisteredASecondConsumer())
                }
                return .failContinuation(continuation, error)
            case .finished(error: .none, let registeredStreamID):
                guard registeredStreamID == streamID else {
                    return .failContinuation(continuation, TriedToRegisteredASecondConsumer())
                }
                return .succeedContinuation(continuation, nil)
            case .modifying:
                preconditionFailure("Invalid state")
            }
        }

        enum ReceiveResponseEndAction {
            case succeedContinuation(UnsafeContinuation<ByteBuffer?, Error>, ByteBuffer)
            case succeedRequest(UnsafeContinuation<ByteBuffer?, Error>)
            case none
        }

        mutating func succeedRequest(_ newChunks: CircularBuffer<ByteBuffer>?) -> ReceiveResponseEndAction {
            switch self.state {
            case .initialized, .waiting, .queued:
                preconditionFailure("How can we receive a response body part, if the request hasn't started yet.")

            case .executing(_, _, .initialized):
                preconditionFailure("If we receive a response end, we must have received a head before")

            case .executing(let context, let requestState, .waitingForStream(var buffer, next: .askExecutorForMore)):
                if let newChunks = newChunks, !newChunks.isEmpty {
                    self.state = .modifying
                    buffer.append(contentsOf: newChunks)
                }
                self.state = .executing(context, requestState, .waitingForStream(buffer, next: .eof))
                return .none

            case .executing(let context, let requestState, .waitingForRemote(let streamID, let continuation)):
                if var newChunks = newChunks, !newChunks.isEmpty {
                    self.state = .modifying
                    let first = newChunks.removeFirst()
                    self.state = .executing(context, requestState, .buffering(streamID, newChunks, next: .eof))
                    return .succeedContinuation(continuation, first)
                }

                self.state = .finished(error: nil, streamID)
                return .succeedRequest(continuation)

            case .executing(let context, let requestState, .buffering(let streamID, var buffer, next: .askExecutorForMore)):
                if let newChunks = newChunks, !newChunks.isEmpty {
                    self.state = .modifying
                    buffer.append(contentsOf: newChunks)
                }
                self.state = .executing(context, requestState, .buffering(streamID, buffer, next: .eof))
                return .none

            case .finished(error: .some, _):
                return .none

            case .finished(error: .none, _):
                preconditionFailure("How can the request be finished without error, before receiving response head?")
            case .modifying:
                preconditionFailure("Invalid state")

            case .executing(_, _, .waitingForStream(_, next: .error)),
                 .executing(_, _, .waitingForStream(_, next: .eof)),
                 .executing(_, _, .buffering(_, _, next: .error)),
                 .executing(_, _, .buffering(_, _, next: .eof)),
                 .executing(_, _, .finished(_, _)):
                preconditionFailure("How can the request be succeeded, if we received an error or eof before")
            }
        }
    }
}
