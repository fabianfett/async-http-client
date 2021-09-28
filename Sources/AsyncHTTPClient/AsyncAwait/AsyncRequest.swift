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

import struct Foundation.Data
import Logging
import NIO
import NIOHTTP1

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
// HTTPClient.AsyncRequest
struct AsyncRequest {
    public struct Body {
        internal enum Mode {
            case asyncSequence((ByteBufferAllocator) async throws -> (IOData?))
//            case asyncSequenceFactory(() -> Mode) // typealias (ByteBufferAllocator) async throws -> IOData?
            case sequence((ByteBufferAllocator) throws -> ByteBuffer)
            case byteBuffer(ByteBuffer)
        }

        var mode: Mode

        private init(_ mode: Mode) {
            self.mode = mode
        }

        static func byteBuffer(_ byteBuffer: ByteBuffer) -> Body {
            self.init(.byteBuffer(byteBuffer))
        }

        static func bytes<S: Sequence>(_ sequence: S) -> Body where S.Element == UInt8 {
            self.init(.asyncSequence { allocator in
                if let buffer = sequence.withContiguousStorageIfAvailable({ allocator.buffer(bytes: $0) }) {
                    // fastpath
                    return .byteBuffer(buffer)
                }
                // potentially really slow path
                return .byteBuffer(allocator.buffer(bytes: sequence))
            })
        }

        static func stream<S: AsyncSequence>(_ sequence: S) -> Body where S.Element == ByteBuffer {
            var iterator = sequence.makeAsyncIterator()
            let body = self.init(.asyncSequence { _ -> IOData? in
                if let byteBuffer = try await iterator.next() {
                    return .byteBuffer(byteBuffer)
                }
                return nil
            })
            return body
        }

        static func stream<S: AsyncSequence>(_ sequence: S) -> Body where S.Element == FileRegion {
            var iterator = sequence.makeAsyncIterator()
            let body = self.init(.asyncSequence { _ in
                if let fileRegion = try await iterator.next() {
                    return .fileRegion(fileRegion)
                }
                return .none
            })
            return body
        }

        static func stream<S: AsyncSequence>(_ sequence: S) -> Body where S.Element == UInt8 {
            var iterator = sequence.makeAsyncIterator()
            let body = self.init(.asyncSequence { allocator -> IOData? in
                var buffer = allocator.buffer(capacity: 1024) // TODO: Magic number
                while buffer.writableBytes > 0, let byte = try await iterator.next() {
                    buffer.writeInteger(byte)
                }
                if buffer.readableBytes > 0 {
                    return .byteBuffer(buffer)
                }
                return nil
            })
            return body
        }
    }

    var url: String // TBD: URL?
    var method: HTTPMethod
    var headers: HTTPHeaders

    var body: Body?

    init(url: String) {
        self.url = url
        self.method = .GET
        self.headers = .init()
        self.body = .none
    }
}

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
public struct AsyncResponse {
    public var version: HTTPVersion
    public var status: HTTPResponseStatus
    public var headers: HTTPHeaders
    public var body: Body

    public struct Body {
        private let bag: AsyncRequestBag

        fileprivate init(_ bag: AsyncRequestBag) {
            self.bag = bag
        }
    }

    init(
        bag: AsyncRequestBag,
        version: HTTPVersion,
        status: HTTPResponseStatus,
        headers: HTTPHeaders
    ) {
        self.body = .init(bag)
        self.version = version
        self.status = status
        self.headers = headers
    }
}

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
extension AsyncResponse.Body: AsyncSequence {
    public typealias Element = ByteBuffer
    public typealias AsyncIterator = Iterator

    public struct Iterator: AsyncIteratorProtocol {
        public typealias Element = ByteBuffer

        private let stream: IteratorStream

        fileprivate init(stream: IteratorStream) {
            self.stream = stream
        }

        public func next() async throws -> ByteBuffer? {
            try await self.stream.next()
        }
    }

    public func makeAsyncIterator() -> Iterator {
        Iterator(stream: IteratorStream(bag: self.bag))
    }

    internal class IteratorStream {
        struct ID: Hashable {
            private let objectID: ObjectIdentifier

            init(_ object: IteratorStream) {
                self.objectID = ObjectIdentifier(object)
            }
        }

        var id: ID { ID(self) }
        private let bag: AsyncRequestBag

        init(bag: AsyncRequestBag) {
            self.bag = bag
        }

        deinit {
            self.bag.cancelResponseStream(streamID: self.id)
        }

        func next() async throws -> ByteBuffer? {
            try await self.bag.nextResponsePart(streamID: self.id)
        }
    }
}

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
extension HTTPClient {
    func execute(_ request: AsyncRequest, deadline: NIODeadline, logger: Logger) async throws -> AsyncResponse {
        let bag = AsyncRequestBag(
            request: request,
            requestOptions: .init(idleReadTimeout: nil, ignoreUncleanSSLShutdown: false),
            logger: logger,
            connectionDeadline: .now() + .seconds(10),
            preferredEventLoop: self.eventLoopGroup.next()
        )

        return try await withTaskCancellationHandler {
            bag.cancel()
        } operation: { () -> AsyncResponse in
            // first register the completion
            async let result = bag.result()

            // second throw it onto the connection pool for execution
            self.poolManager.executeRequest(bag)

            // third await result
            return try await result
        }
    }
}

// redirect!

// connection pool manager -> shutdown
// config objects ... client.config (CoW struct)
// request
