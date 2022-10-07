import NIOCore
import Logging

fileprivate let loggingDisabled = Logger(label: "AHC-do-not-log", factory: { _ in SwiftLogNoOpLogHandler() })

@available(macOS 13, iOS 13.0, watchOS 6.0, tvOS 13.0, *)
extension NewHTTPClient {
    /// Execute arbitrary HTTP requests.
    ///
    /// - Parameters:
    ///   - request: HTTP request to execute.
    ///   - deadline: Point in time by which the request must complete.
    ///   - logger: The logger to use for this request.
    /// - Returns: The response to the request. Note that the `body` of the response may not yet have been fully received.
    public func execute<Clock: _Concurrency.Clock, Instant>(
        _ request: HTTPClientRequest,
        deadline: Instant,
        clock: Clock,
        logger: Logger? = nil
    ) async throws -> HTTPClientResponse where Clock.Instant == Instant, Instant.Duration == Duration {
        try await self.executeAndFollowRedirectsIfNeeded(
            request,
            deadline: deadline,
            clock: clock,
            logger: logger ?? loggingDisabled,
            redirectState: RedirectState(self.configuration.redirectConfiguration.mode, initialURL: request.url)
        )
    }
}

// MARK: Connivence methods

@available(macOS 13, iOS 13.0, watchOS 6.0, tvOS 13.0, *)
extension NewHTTPClient {
    /// Execute arbitrary HTTP requests.
    ///
    /// - Parameters:
    ///   - request: HTTP request to execute.
    ///   - timeout: time the the request has to complete.
    ///   - logger: The logger to use for this request.
    /// - Returns: The response to the request. Note that the `body` of the response may not yet have been fully received.
    public func execute(
        _ request: HTTPClientRequest,
        timeout: Duration,
        logger: Logger? = nil
    ) async throws -> HTTPClientResponse {
        let clock = ContinuousClock()
        return try await self.execute(
            request,
            deadline: clock.now + timeout,
            clock: clock,
            logger: logger
        )
    }
}


@available(macOS 13, iOS 13.0, watchOS 6.0, tvOS 13.0, *)
extension NewHTTPClient {
    private func executeAndFollowRedirectsIfNeeded<Clock: _Concurrency.Clock, Instant>(
        _ request: HTTPClientRequest,
        deadline: Instant,
        clock: Clock,
        logger: Logger,
        redirectState: RedirectState?
    ) async throws -> HTTPClientResponse where Clock.Instant == Instant, Instant.Duration == Duration {
        var currentRequest = request
        var currentRedirectState = redirectState

        // this loop is there to follow potential redirects
        while true {
            let preparedRequest = try HTTPClientRequest.Prepared(currentRequest)
            let response = try await self.transport.execute(
                currentRequest,
                configuration: self.configuration,
                deadline: deadline,
                clock: clock,
                logger: logger
            )

            guard var redirectState = currentRedirectState else {
                // a `nil` redirectState means we should not follow redirects
                return response
            }

            guard let redirectURL = response.headers.extractRedirectTarget(
                status: response.status,
                originalURL: preparedRequest.url,
                originalScheme: preparedRequest.poolKey.scheme
            ) else {
                // response does not want a redirect
                return response
            }

            // validate that we do not exceed any limits or are running circles
            try redirectState.redirect(to: redirectURL.absoluteString)
            currentRedirectState = redirectState

            let newRequest = currentRequest.followingRedirect(
                from: preparedRequest.url,
                to: redirectURL,
                status: response.status
            )

            guard newRequest.body.canBeConsumedMultipleTimes else {
                // we already send the request body and it cannot be send again
                return response
            }

            currentRequest = newRequest
        }
    }
}

