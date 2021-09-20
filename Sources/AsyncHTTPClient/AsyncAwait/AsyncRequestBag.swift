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

import Logging
import NIO
import NIOHTTP1

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
actor AsyncRequestBag {
    // TODO: We should drop the request after sending to free up resource ASAP
    let request: AsyncRequest

    let logger: Logger

    let requestHead: HTTPRequestHead
    let requestFramingMetadata: RequestFramingMetadata

    let idleReadTimeout: TimeAmount?
    let connectionDeadline: NIODeadline
    let eventLoopPreference: HTTPClient.EventLoopPreference

    private var state: StateMachine = .init()
    private var isCancelled = false

    init(request: AsyncRequest,
         logger: Logger,
         connectionDeadline: NIODeadline,
         eventLoopPreference: HTTPClient.EventLoopPreference) {
        self.request = request
        self.logger = logger
        self.idleReadTimeout = nil
        self.connectionDeadline = connectionDeadline
        self.eventLoopPreference = eventLoopPreference

        self.requestHead = HTTPRequestHead(
            version: .http1_1,
            method: request.method,
            uri: request.url,
            headers: request.headers
        )

        switch request.body?.mode {
        case .byteBuffer(let byteBuffer):
            self.requestFramingMetadata = .init(connectionClose: false, body: .fixedSize(byteBuffer.readableBytes))
        case .sequence:
            self.requestFramingMetadata = .init(connectionClose: false, body: .stream)
        case .asyncSequence:
            self.requestFramingMetadata = .init(connectionClose: false, body: .stream)
        case .none:
            self.requestFramingMetadata = .init(connectionClose: false, body: .none)
        }
    }

    nonisolated func cancel() {
        Task.detached {
            await self.cancel0()
        }
    }

    func result() async throws -> AsyncResponse {
        try await withUnsafeThrowingContinuation { continuation in
            self.state.registerContinuation(continuation)
        }
    }

    // MARK: Scheduled request

    private func cancel0() {
        self.isCancelled = true
        self.fail0(HTTPClientError.cancelled)
    }

    private func requestWasQueued0(_ scheduler: HTTPRequestScheduler) {
        self.state.requestWasQueued(scheduler)
    }

    private func fail0(_ error: Error) {
        switch self.state.fail(error) {
        case .none:
            break

        case .failResponseStream(let continuation, let error, let executor):
            continuation.resume(throwing: error)
            executor.cancelRequest(self)

        case .failContinuation(let continuation, let error, let scheduler, let executor):
            continuation.resume(throwing: error)
            scheduler?.cancelRequest(self)
            executor?.cancelRequest(self)
        }
    }

    // MARK: Scheduled request

    private func willExecuteRequest0(_ executor: HTTPRequestExecutor) {
        if !self.state.willExecuteRequest(executor) {
            return executor.cancelRequest(self)
        }
    }

    private func resumeRequestBodyStream0() async {
        switch self.state.resumeRequestBodyStream() {
        case .none:
            break
        case .resumeStream(let allocator):
            switch self.request.body?.mode {
            case .asyncSequence(let next):
                // it is safe to call this async here. it dispatches...
                await self.writeRequestStream(allocator, next: next)

            case .byteBuffer(let byteBuffer):
                guard case .write(let part, let executor, true) = self.state.producedNextRequestPart(.byteBuffer(byteBuffer)) else {
                    preconditionFailure("")
                }
                executor.writeRequestBodyPart(part, request: self)

                guard case .forwardStreamFinished(let executor) = self.state.finishRequestBodyStream() else {
                    preconditionFailure("")
                }
                executor.finishRequestBodyStream(self)

            case .none:
                break

            case .sequence(let create):
                do {
                    let byteBuffer = try create(allocator) // <--- only throw point

                    guard case .write(let iodata, let executor, continue: true) = self.state.producedNextRequestPart(.byteBuffer(byteBuffer)) else {
                        preconditionFailure("")
                    }

                    executor.writeRequestBodyPart(iodata, request: self)

                    guard case .forwardStreamFinished(let executor) = self.state.finishRequestBodyStream() else {
                        preconditionFailure("")
                    }
                    executor.finishRequestBodyStream(self)
                } catch {
                    switch self.state.failedToProduceNextRequestPart(error) {
                    case .none:
                        break
                    case .informRequestAboutFailure(let error, let cancelExecutor, let continuation):
                        self.fail(error)
                        cancelExecutor.cancelRequest(self)
                        continuation?.resume(throwing: error)
                    }
                    return
                }
            }
        }
    }

    private func pauseRequestBodyStream0() {
        self.state.pauseRequestBodyStream()
    }

    private func receiveResponseHead0(_ head: HTTPResponseHead) {
        switch self.state.receiveResponseHead(head) {
        case .none:
            break
        case .succeedResponseHead(let head, let continuation):
            let asyncResponse = AsyncResponse(
                bag: self,
                version: head.version,
                status: head.status,
                headers: head.headers
            )
            continuation.resume(returning: asyncResponse)
        }
    }

    private func receiveResponseBodyParts0(_ buffer: CircularBuffer<ByteBuffer>) {
        switch self.state.receiveResponseBodyParts(buffer) {
        case .none:
            break
        case .succeedContinuation(let continuation, let bytes):
            continuation.resume(returning: bytes)
        }
    }

    private func succeedRequest0(_ buffer: CircularBuffer<ByteBuffer>?) {
        switch self.state.succeedRequest(buffer) {
        case .succeedRequest(let continuation):
            continuation.resume(returning: nil)
        case .succeedContinuation(let continuation, let byteBuffer):
            continuation.resume(returning: byteBuffer)
        case .none:
            break
        }
    }

    // MARK: Other methods

    private func writeRequestStream(
        _ allocator: ByteBufferAllocator,
        next: @escaping ((ByteBufferAllocator) async throws -> IOData?)
    ) async {
        while true {
            do {
                guard let part = try await next(allocator) else { // <---- dispatch point!
                    // no more data to produce
                    switch self.state.finishRequestBodyStream() {
                    case .none:
                        break
                    case .forwardStreamFinished(let executor):
                        executor.finishRequestBodyStream(self)
                    }
                    return
                }

                let action = self.state.producedNextRequestPart(part)
                switch action {
                case .write(let part, let executor, let continueAfter):
                    executor.writeRequestBodyPart(part, request: self)
                    if !continueAfter {
                        return
                    }
                case .ignore:
                    return
                }
            } catch {
                // producing more failed
                switch self.state.failedToProduceNextRequestPart(error) {
                case .none:
                    break
                case .informRequestAboutFailure(let error, cancelExecutor: let executor, let continuation):
                    executor.cancelRequest(self)
                    self.fail(error)
                    continuation?.resume(throwing: error)
                }
                return
            }
        }
    }
}

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
extension AsyncRequestBag: HTTPSchedulableRequest {
    nonisolated func requestWasQueued(_ scheduler: HTTPRequestScheduler) {
        Task.detached {
            await self.requestWasQueued0(scheduler)
        }
    }

    nonisolated func fail(_ error: Error) {
        Task.detached {
            await self.fail0(error)
        }
    }
}

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
extension AsyncRequestBag: HTTPExecutableRequest {
    nonisolated func willExecuteRequest(_ executor: HTTPRequestExecutor) {
        Task.detached {
            await self.willExecuteRequest0(executor)
        }
    }

    nonisolated func requestHeadSent() {}

    nonisolated func resumeRequestBodyStream() {
        Task.detached {
            await self.resumeRequestBodyStream0()
        }
    }

    nonisolated func pauseRequestBodyStream() {
        Task.detached {
            await self.pauseRequestBodyStream0()
        }
    }

    nonisolated func receiveResponseHead(_ head: HTTPResponseHead) {
        Task.detached {
            await self.receiveResponseHead0(head)
        }
    }

    nonisolated func receiveResponseBodyParts(_ buffer: CircularBuffer<ByteBuffer>) {
        Task.detached {
            await self.receiveResponseBodyParts0(buffer)
        }
    }

    nonisolated func succeedRequest(_ buffer: CircularBuffer<ByteBuffer>?) {
        Task.detached {
            await self.succeedRequest0(buffer)
        }
    }
}

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
extension AsyncRequestBag {
    func nextResponsePart(streamID: AsyncResponse.Body.IteratorStream.ID) async throws -> ByteBuffer? {
        try await withUnsafeThrowingContinuation { continuation in
            switch self.state.consumeNextResponsePart(streamID: streamID, continuation: continuation) {
            case .succeedContinuation(let continuation, let result):
                continuation.resume(returning: result)
            case .failContinuation(let continuation, let error):
                continuation.resume(throwing: error)
            case .askExecutorForMore(let executor):
                executor.demandResponseBodyStream(self)
            }
        }
    }

    func cancelResponseStream0(streamID: AsyncResponse.Body.IteratorStream.ID) {}

    nonisolated func cancelResponseStream(streamID: AsyncResponse.Body.IteratorStream.ID) {
        Task.detached {
            await self.cancelResponseStream0(streamID: streamID)
        }
    }
}
