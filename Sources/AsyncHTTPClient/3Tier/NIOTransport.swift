import NIOCore
import NIOPosix
import NIOSSL
import Logging

@available(macOS 13.0, iOS 16.0, watchOS 6.0, tvOS 13.0, *)
public final class NIOTransport: HTTPTransport, @unchecked Sendable {
    let eventLoopGroup: MultiThreadedEventLoopGroup
    let poolManager: HTTPConnectionPool.Manager

    public init(eventLoopGroup: MultiThreadedEventLoopGroup, backgroundActivityLogger: Logger?) {
        self.eventLoopGroup = eventLoopGroup
        let logger = backgroundActivityLogger ?? Logger(label: "noop", factory: { _ in SwiftLogNoOpLogHandler() })
        self.poolManager = HTTPConnectionPool.Manager(eventLoopGroup: eventLoopGroup, configuration: .init(), backgroundActivityLogger: logger)
    }

    public func execute<Clock: _Concurrency.Clock, Instant>(
        _ request: HTTPClientRequest,
        configuration: HTTPClientConfiguration,
        deadline: Instant,
        clock: Clock,
        logger: Logging.Logger
    ) async throws -> HTTPClientResponse where Instant == Clock.Instant, Instant.Duration == Duration {
        let cancelHandler = TransactionCancelHandler()

        let request = try HTTPClientRequest.Prepared(request)

        return try await withTaskCancellationHandler(operation: { () async throws -> HTTPClientResponse in
            let eventLoop = self.eventLoopGroup.any()

            let timeoutTask = Task {
                // Throws if cancelled. Which is fine.
                try await Task.sleep(until: deadline, clock: clock)
                cancelHandler.cancel(reason: .deadlineExceeded)
            }
            defer {
                timeoutTask.cancel()
            }

            let durationTillDeadline = clock.now.duration(to: deadline)
            let newDeadline = NIODeadline.now() + .seconds(durationTillDeadline.components.seconds) + .nanoseconds(durationTillDeadline.components.attoseconds / 1_000_000_000)
            print("Deadline now: \(NIODeadline.now()), connection: \(newDeadline)")

            return try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<HTTPClientResponse, Swift.Error>) -> Void in
                let transaction = Transaction(
                    request: request,
                    requestOptions: .init(idleReadTimeout: nil),
                    logger: logger,
                    connectionDeadline: newDeadline,
                    preferredEventLoop: eventLoop,
                    responseContinuation: continuation
                )

                cancelHandler.registerTransaction(transaction)

                self.poolManager.executeRequest(
                    transaction,
                    key: .init(request: request, configuration: configuration),
                    configuration: .init(request: request, configuration: configuration)
                )
            }
        }, onCancel: {
            cancelHandler.cancel(reason: .taskCanceled)
        })
    }

    public func shutdown() async throws {
        let promise = self.eventLoopGroup.any().makePromise(of: Bool.self)
        self.poolManager.shutdown(promise: promise)
        _ = try await promise.futureResult.get()
    }
}
