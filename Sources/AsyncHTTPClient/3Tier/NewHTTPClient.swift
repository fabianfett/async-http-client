import NIOSSL
import NIOHTTPCompression
import NIOCore
import Logging

@available(macOS 13.0, iOS 16.0, watchOS 6.0, tvOS 13.0, *)
public protocol HTTPClientProtocol {
    func execute<Clock: _Concurrency.Clock, Instant>(
        _ request: HTTPClientRequest,
        deadline: Instant,
        clock: Clock,
        logger: Logger
    ) async throws -> HTTPClientResponse where Clock.Instant == Instant, Instant.Duration == Duration
}

@available(macOS 13.0, iOS 16.0, watchOS 6.0, tvOS 13.0, *)
public protocol HTTPTransport: Sendable {
    
    func execute<Clock: _Concurrency.Clock, Instant: Swift.InstantProtocol>(
        _ request: HTTPClientRequest,
        configuration: HTTPClientConfiguration,
        deadline: Instant,
        clock: Clock,
        logger: Logger
    ) async throws -> HTTPClientResponse where Clock.Instant == Instant, Instant.Duration == Duration

    func shutdown() async throws
}

@available(macOS 13.0, iOS 16.0, watchOS 6.0, tvOS 13.0, *)
public struct NewHTTPClient<Transport: HTTPTransport>: Sendable {
    public var configuration: HTTPClientConfiguration
    public var transport: Transport

    public init(transport: Transport) {
        self.transport = transport
        self.configuration = .init()
    }
}

/// Specifies redirect processing settings.
public struct RedirectConfiguration: NIOSendable {
    var mode: HTTPClient.Configuration.RedirectConfiguration.Mode

    init() {
        self.mode = .follow(max: 5, allowCycles: false)
    }

    init(configuration: HTTPClient.Configuration.RedirectConfiguration.Mode) {
        self.mode = configuration
    }

    /// Redirects are not followed.
    public static let disallow = RedirectConfiguration(configuration: .disallow)

    /// Redirects are followed with a specified limit.
    ///
    /// - parameters:
    ///     - max: The maximum number of allowed redirects.
    ///     - allowCycles: Whether cycles are allowed.
    ///
    /// - warning: Cycle detection will keep all visited URLs in memory which means a malicious server could use this as a denial-of-service vector.
    public static func follow(max: Int, allowCycles: Bool) -> RedirectConfiguration { return .init(configuration: .follow(max: max, allowCycles: allowCycles)) }
}

public struct DecompressionConfiguration: Hashable, Sendable {
    enum Base: Hashable, Sendable {
        case disabled
        case enabled(DecompressionLimit)
    }

    public struct DecompressionLimit: Hashable, Sendable {
        var base: Base

        enum Base: Hashable, Sendable {
            case none
            case size(Int)
            case ratio(Int)
        }

        /// No limit will be set.
        /// - warning: Setting `limit` to `.none` leaves you vulnerable to denial of service attacks.
        public static let none = DecompressionLimit(base: .none)
        /// Limit will be set on the request body size.
        public static func size(_ value: Int) -> DecompressionLimit {
            DecompressionLimit(base: .size(value))
        }
        /// Limit will be set on a ratio between compressed body size and decompressed result.
        public static func ratio(_ value: Int) -> DecompressionLimit {
            DecompressionLimit(base: .ratio(value))
        }
    }

    var base: Base

    /// Decompression is disabled.
    public static let disabled = DecompressionConfiguration(base: .disabled)
    /// Decompression is enabled.
    public static func enabled(limit: DecompressionLimit) -> DecompressionConfiguration {
        DecompressionConfiguration(base: .enabled(limit))
    }
}
