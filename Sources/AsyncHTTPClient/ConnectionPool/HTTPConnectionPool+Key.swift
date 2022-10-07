import NIOSSL

extension HTTPConnectionPool {

    struct Key: Hashable, Sendable {

        var scheme: Scheme
        var connectionTarget: ConnectionTarget

        var uniquenessID: String?

        private var tlsConfiguration: BestEffortHashableTLSConfiguration?
        var decompression: DecompressionConfiguration

        var proxy: HTTPClient.Configuration.Proxy?

        var httpVersion: HTTPClient.Configuration.HTTPVersion

        var networkFrameworkWaitForConnectivity: Bool

        init(
            scheme: Scheme,
            connectionTarget: ConnectionTarget,
            uniquenessID: String?,
            tlsConfiguration: TLSConfiguration,
            decompression: DecompressionConfiguration,
            proxy: HTTPClient.Configuration.Proxy?,
            httpVersion: HTTPClient.Configuration.HTTPVersion,
            networkFrameworkWaitForConnectivity: Bool
        ) {
            self.scheme = scheme
            self.connectionTarget = connectionTarget
            self.uniquenessID = uniquenessID
            self.tlsConfiguration = BestEffortHashableTLSConfiguration(wrapping: tlsConfiguration)
            self.decompression = decompression
            self.proxy = proxy
            self.httpVersion = httpVersion
            self.networkFrameworkWaitForConnectivity = networkFrameworkWaitForConnectivity
        }

        var description: String {
            var hasher = Hasher()
            self.tlsConfiguration?.hash(into: &hasher)
            let hash = hasher.finalize()
            let hostDescription: String
            switch self.connectionTarget {
            case .ipAddress(let serialization, let addr):
                hostDescription = "\(serialization):\(addr.port!)"
            case .domain(let domain, port: let port):
                hostDescription = "\(domain):\(port)"
            case .unixSocket(let socketPath):
                hostDescription = socketPath
            }
            return "\(self.scheme)://\(hostDescription) TLS-hash: \(hash)"
        }
    }
}

extension HTTPConnectionPool.Key {
    init(request: HTTPClient.Request, clientConfiguration: HTTPClient.Configuration) {
        self.init(
            scheme: request.deconstructedURL.scheme,
            connectionTarget: request.deconstructedURL.connectionTarget,
            uniquenessID: nil,
            tlsConfiguration: request.tlsConfiguration ?? clientConfiguration.tlsConfiguration ?? .makeClientConfiguration(),
            decompression: .init(clientConfiguration.decompression),
            proxy: clientConfiguration.proxy,
            httpVersion: clientConfiguration.httpVersion,
            networkFrameworkWaitForConnectivity: clientConfiguration.networkFrameworkWaitForConnectivity
        )
    }
}

@available(macOS 13, iOS 13.0, watchOS 6.0, tvOS 13.0, *)
extension HTTPConnectionPool.Key {
    init(request: HTTPClientRequest.Prepared, configuration: HTTPClientConfiguration) {
        self.init(
            scheme: request.poolKey.scheme,
            connectionTarget: request.poolKey.connectionTarget,
            uniquenessID: nil,
            tlsConfiguration: configuration.tlsConfiguration,
            decompression: configuration.decompressionConfiguration,
            proxy: configuration.proxy,
            httpVersion: configuration.httpVersion,
            networkFrameworkWaitForConnectivity: true
        )
    }
}

@available(macOS 10.15, iOS 13.0, watchOS 6.0, tvOS 13.0, *)
extension HTTPConnectionPool.Key {
    init(request: HTTPClientRequest.Prepared, clientConfiguration: HTTPClient.Configuration) {
        self.init(
            scheme: request.poolKey.scheme,
            connectionTarget: request.poolKey.connectionTarget,
            uniquenessID: nil,
            tlsConfiguration: clientConfiguration.tlsConfiguration ?? .makeClientConfiguration(),
            decompression: .init(clientConfiguration.decompression),
            proxy: clientConfiguration.proxy,
            httpVersion: clientConfiguration.httpVersion,
            networkFrameworkWaitForConnectivity: clientConfiguration.networkFrameworkWaitForConnectivity
        )
    }
}

extension DecompressionConfiguration {
    init(_ config: HTTPClient.Decompression) {
        #warning("HACK, HACK")
        self = .disabled
    }
}

extension HTTPClient.Decompression {
    init(_ config: DecompressionConfiguration) {
        switch config.base {
        case .disabled:
            self = .disabled
        case .enabled(let value):
            switch value.base {
            case .none:
                self = .enabled(limit: .none)
            case .ratio(let value):
                self = .enabled(limit: .ratio(value))
            case .size(let value):
                self = .enabled(limit: .size(value))
            }
        }
    }
}
