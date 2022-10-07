import NIOSSL
import Logging

@available(macOS 13.0, iOS 16.0, watchOS 6.0, tvOS 13.0, *)
protocol Middleware {
    func run(
        _ request: HTTPClientRequest,
        logger: Logger,
        next: (HTTPClientRequest, Logger) async throws -> HTTPClientResponse
    ) async throws -> HTTPClientResponse
}

@available(macOS 13.0, iOS 16.0, watchOS 6.0, tvOS 13.0, *)
struct MiddlewareStack {
    var stack: [any Middleware]

    func run(
        _ request: HTTPClientRequest,
        logger: Logger,
        finally: (HTTPClientRequest, Logger) async throws -> HTTPClientResponse
    ) async throws -> HTTPClientResponse {
        try await Self.recursive(stack: stack[...], request: request, logger: logger, finally: finally)
    }

    private static func recursive(
        stack: ArraySlice<any Middleware>,
        request: HTTPClientRequest,
        logger: Logger,
        finally: (HTTPClientRequest, Logger) async throws -> HTTPClientResponse
    ) async throws -> HTTPClientResponse {
        guard let first = stack.first else {
            return try await finally(request, logger)
        }

        return try await first.run(request, logger: logger) { request, logger in
            let nextStack = stack.dropFirst()
            return try await recursive(stack: nextStack, request: request, logger: logger, finally: finally)
        }
    }
}

@available(macOS 13.0, iOS 16.0, watchOS 6.0, tvOS 13.0, *)
extension NewHTTPClient {

    func execute(
        _ request: HTTPClientRequest,
        logger: Logger
    ) async throws -> HTTPClientResponse {
        fatalError()
    }

    func execute<Clock: _Concurrency.Clock, Instant>(
        _ request: HTTPClientRequest,
        deadline: Instant,
        clock: Clock,
        logger: Logger
    ) async throws -> HTTPClientResponse where Clock.Instant == Instant, Instant.Duration == Duration {
        try await withThrowingTaskGroup(of: HTTPClientResponse.self, returning: HTTPClientResponse.self) { taskGroup in
            taskGroup.addTask { () async throws -> HTTPClientResponse in
                try await Task.sleep(until: deadline, clock: clock)
                throw HTTPClientError.deadlineExceeded
            }

            taskGroup.addTask {
                try await self.execute(request, logger: logger)
            }

            let result = await taskGroup.nextResult()
            taskGroup.cancelAll()

            return try result!.get()
        }
    }
}



@available(macOS 13.0, iOS 16.0, watchOS 6.0, tvOS 13.0, *)
public struct HTTPClientConfiguration: @unchecked Sendable {
    private var storage: Storage

    public var tlsConfiguration: TLSConfiguration {
        get { self.storage.tlsConfiguration }
        set {
            if !isKnownUniquelyReferenced(&self.storage) {
                self.storage = self.storage.copy()
            }
            self.storage.tlsConfiguration = newValue
        }
    }

    public var redirectConfiguration: RedirectConfiguration {
        get { self.storage.redirectConfiguration }
        set {
            if !isKnownUniquelyReferenced(&self.storage) {
                self.storage = self.storage.copy()
            }
            self.storage.redirectConfiguration = newValue
        }
    }

    public var httpVersion: HTTPClient.Configuration.HTTPVersion {
        get { self.storage.httpVersion }
        set {
            if !isKnownUniquelyReferenced(&self.storage) {
                self.storage = self.storage.copy()
            }
            self.storage.httpVersion = newValue
        }
    }

    public var proxy: HTTPClient.Configuration.Proxy? {
        get { self.storage.proxy }
        set {
            if !isKnownUniquelyReferenced(&self.storage) {
                self.storage = self.storage.copy()
            }
            self.storage.proxy = newValue
        }
    }

    public var decompressionConfiguration: DecompressionConfiguration {
        get { self.storage.decompressionConfiguration }
        set {
            if !isKnownUniquelyReferenced(&self.storage) {
                self.storage = self.storage.copy()
            }
            self.storage.decompressionConfiguration = newValue
        }
    }

    public init() {
        self.storage = Storage(
            tlsConfiguration: .makeClientConfiguration(),
            redirectConfiguration: .follow(max: 5, allowCycles: false),
            httpVersion: .automatic,
            proxy: nil,
            decompressionConfiguration: .disabled
        )
    }
}

@available(macOS 13.0, iOS 16.0, watchOS 6.0, tvOS 13.0, *)
extension HTTPClientConfiguration {
    private final class Storage {
        var tlsConfiguration: TLSConfiguration
        var redirectConfiguration: RedirectConfiguration
        var httpVersion: HTTPClient.Configuration.HTTPVersion
        var proxy: HTTPClient.Configuration.Proxy?
        var decompressionConfiguration: DecompressionConfiguration

        init(
            tlsConfiguration: TLSConfiguration,
            redirectConfiguration: RedirectConfiguration,
            httpVersion: HTTPClient.Configuration.HTTPVersion,
            proxy: HTTPClient.Configuration.Proxy?,
            decompressionConfiguration: DecompressionConfiguration
        ) {
            self.tlsConfiguration = tlsConfiguration
            self.redirectConfiguration = redirectConfiguration
            self.httpVersion = httpVersion
            self.proxy = proxy
            self.decompressionConfiguration = decompressionConfiguration
        }

        func copy() -> Self {
            Self.init(
                tlsConfiguration: self.tlsConfiguration,
                redirectConfiguration: self.redirectConfiguration,
                httpVersion: self.httpVersion,
                proxy: self.proxy,
                decompressionConfiguration: self.decompressionConfiguration
            )
        }
    }
}
