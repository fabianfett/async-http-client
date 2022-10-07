import NIOCore
import NIOSSL

extension HTTPConnectionPool {
    struct Configuration {

        var scheme: Scheme
        var connectionTarget: ConnectionTarget

        // MARK: - Transport wide settings -

        /// The time an idle connection shall remain open, before being closed
        var idleTimeout: TimeAmount

        /// The max time allowed to create a new connection
        ///
        /// This setting should be calculated adhoc and not be set globally
        var connectTimeout: TimeAmount

        /// The max number of concurrent general purpose HTTP1 connections
        var concurrentHTTP1ConnectionsPerHostSoftLimit: Int

        // MARK: - NewHTTPClient wide settings -

        /// The TLS configuration to use
        var tlsConfiguration: TLSConfiguration

        /// The decompression strategy
        var decompression: HTTPClient.Decompression

        /// Shall the connection be started using a proxy
        var proxy: HTTPClient.Configuration.Proxy?

        /// The http version to support
        var httpVersion: HTTPClient.Configuration.HTTPVersion

        var networkFrameworkWaitForConnectivity: Bool
    }
}

extension HTTPConnectionPool.Configuration {

    init(request: HTTPClient.Request, clientConfiguration: HTTPClient.Configuration) {
        self.scheme = request.deconstructedURL.scheme
        self.connectionTarget = request.deconstructedURL.connectionTarget

        self.tlsConfiguration = request.tlsConfiguration ?? clientConfiguration.tlsConfiguration ?? .makeClientConfiguration()
        self.decompression = clientConfiguration.decompression
        self.proxy = clientConfiguration.proxy
        self.httpVersion = clientConfiguration.httpVersion
        self.idleTimeout = clientConfiguration.connectionPool.idleTimeout
        self.connectTimeout = clientConfiguration.timeout.connect ?? HTTPConnectionPool.fallbackConnectTimeout
        self.concurrentHTTP1ConnectionsPerHostSoftLimit = clientConfiguration.connectionPool.concurrentHTTP1ConnectionsPerHostSoftLimit
        self.networkFrameworkWaitForConnectivity = clientConfiguration.networkFrameworkWaitForConnectivity
    }
}

@available(macOS 10.15, iOS 13.0, watchOS 6.0, tvOS 13.0, *)
extension HTTPConnectionPool.Configuration {
    init(request: HTTPClientRequest.Prepared, clientConfiguration: HTTPClient.Configuration) {
        self.scheme = request.poolKey.scheme
        self.connectionTarget = request.poolKey.connectionTarget

        self.tlsConfiguration = clientConfiguration.tlsConfiguration ?? .makeClientConfiguration()
        self.decompression = clientConfiguration.decompression
        self.proxy = clientConfiguration.proxy
        self.httpVersion = clientConfiguration.httpVersion
        self.idleTimeout = clientConfiguration.connectionPool.idleTimeout
        self.connectTimeout = clientConfiguration.timeout.connect ?? HTTPConnectionPool.fallbackConnectTimeout
        self.concurrentHTTP1ConnectionsPerHostSoftLimit = clientConfiguration.connectionPool.concurrentHTTP1ConnectionsPerHostSoftLimit
        self.networkFrameworkWaitForConnectivity = clientConfiguration.networkFrameworkWaitForConnectivity
    }
}


@available(macOS 13, iOS 13.0, watchOS 6.0, tvOS 13.0, *)
extension HTTPConnectionPool.Configuration {
    init(request: HTTPClientRequest.Prepared, configuration: HTTPClientConfiguration) {
        self.scheme = request.poolKey.scheme
        self.connectionTarget = request.poolKey.connectionTarget

        self.tlsConfiguration = configuration.tlsConfiguration
        self.decompression = .init(configuration.decompressionConfiguration)
        self.proxy = configuration.proxy
        self.httpVersion = configuration.httpVersion
        self.idleTimeout = .seconds(90)
        self.connectTimeout = HTTPConnectionPool.fallbackConnectTimeout
        self.concurrentHTTP1ConnectionsPerHostSoftLimit = 8
        self.networkFrameworkWaitForConnectivity = true
    }
}
