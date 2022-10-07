@testable import AsyncHTTPClient
import NIOCore
import NIOPosix
import XCTest
import Logging

@available(macOS 13.0, *)
final class NewHTTPClientTest: XCTestCase {

    var eventLoopGroup: MultiThreadedEventLoopGroup!
    var transport: NIOTransport!

    override func setUp() {
        super.setUp()

        self.eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.addTeardownBlock {
            XCTAssertNoThrow(try self.eventLoopGroup.syncShutdownGracefully())
        }

        self.transport = NIOTransport(eventLoopGroup: self.eventLoopGroup, backgroundActivityLogger: nil)
        self.addTeardownBlock {
            try await self.transport.shutdown()
        }
    }

    func testTimeout() {
        XCTAsyncTest(timeout: 5) {
            let bin = HTTPBin(.http2(compress: false))
            defer { XCTAssertNoThrow(try bin.shutdown()) }
            var client = NewHTTPClient(transport: self.transport)
            client.configuration.tlsConfiguration.certificateVerification = .none
            let logger = Logger(label: "HTTPClient", factory: StreamLogHandler.standardOutput(label:))
            let request = HTTPClientRequest(url: "https://localhost:\(bin.port)/wait")

            await XCTAssertThrowsError(try await client.execute(request, timeout: .milliseconds(100), logger: logger)) { error in
                guard let error = error as? HTTPClientError else {
                    return XCTFail("unexpected error \(error)")
                }
                // a race between deadline and connect timer can result in either error
                XCTAssertTrue([.deadlineExceeded, .connectTimeout].contains(error))
            }
        }
    }

    func testPoolUniqueness() {
        XCTAsyncTest(timeout: 5) {
            let bin = HTTPBin(.http2(compress: false))
            defer { XCTAssertNoThrow(try bin.shutdown()) }
            var client = NewHTTPClient(transport: self.transport)
            client.configuration.tlsConfiguration.certificateVerification = .none
            let logger = Logger(label: "HTTPClient", factory: StreamLogHandler.standardOutput(label:))
            let request = HTTPClientRequest(url: "https://localhost:\(bin.port)/")

            _ = try await client.execute(request, timeout: .seconds(2), logger: logger)
            _ = try await client.execute(request, timeout: .seconds(2), logger: logger)

            XCTAssertEqual(bin.activeConnections, 1)
            XCTAssertEqual(bin.createdConnections, 1)

            var newClient = client
            newClient.configuration.connectionPoolConfiguration.uniquenessID = "foo"

            _ = try await newClient.execute(request, timeout: .seconds(2), logger: logger)
            XCTAssertEqual(bin.activeConnections, 2)
            XCTAssertEqual(bin.createdConnections, 2)
        }
    }
}
