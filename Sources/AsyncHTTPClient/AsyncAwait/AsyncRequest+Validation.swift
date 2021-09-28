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

import struct Foundation.URL
import NIOHTTP1

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
extension AsyncRequest {
    
    struct ValidationResult {
        let requestFramingMetadata: RequestFramingMetadata
        let poolKey: ConnectionPool.Key
        let head: HTTPRequestHead
    }
    
    func validate() throws -> ValidationResult {
        
        guard let url = URL(string: self.url) else {
            throw HTTPClientError.invalidURL
        }
        
        guard let urlScheme = url.scheme?.lowercased() else {
            throw HTTPClientError.emptyScheme
        }
        
        let kind = try HTTPClient.Request.Kind(forScheme: urlScheme)
        let useTLS: Bool = urlScheme == "https" || urlScheme == "https+unix"
        
        let poolKey = try ConnectionPool.Key(
            scheme: .init(string: urlScheme),
            host: kind.hostFromURL(url),
            port: url.port ?? (useTLS ? 443 : 80),
            unixPath: kind.socketPathFromURL(url),
            tlsConfiguration: nil
        )
        
        try self.headers.validateFieldNames()
        
        var head = HTTPRequestHead(version: .http1_1, method: self.method, uri: url.uri, headers: self.headers)
        
        // if no host header was set, let's pick
        if !head.headers.contains(name: "host") {
            guard let urlHost = url.host else {
                throw HTTPClientError.emptyHost
            }
            head.headers.add(name: "host", value: urlHost)
        }
        
        let encodings = head.headers[canonicalForm: "Transfer-Encoding"].map { $0.lowercased() }
        if encodings.contains("identity") {
            throw HTTPClientError.identityCodingIncorrectlyPresent
        }
        
        head.headers.remove(name: "Transfer-Encoding")
        
        guard let body = self.body else {
            head.headers.remove(name: "Content-Length")
            // if we don't have a body we might not need to send the Content-Length field
            // https://tools.ietf.org/html/rfc7230#section-3.3.2
            switch method {
            case .GET, .HEAD, .DELETE, .CONNECT, .TRACE:
                // A user agent SHOULD NOT send a Content-Length header field when the request
                // message does not contain a payload body and the method semantics do not
                // anticipate such a body.
                return ValidationResult(
                    requestFramingMetadata: .init(connectionClose: !head.isKeepAlive, body: .none),
                    poolKey: poolKey,
                    head: head
                )
            default:
                // A user agent SHOULD send a Content-Length in a request message when
                // no Transfer-Encoding is sent and the request method defines a meaning
                // for an enclosed payload body.
                head.headers.add(name: "Content-Length", value: "0")
                return ValidationResult(
                    requestFramingMetadata: .init(connectionClose: !head.isKeepAlive, body: .none),
                    poolKey: poolKey,
                    head: head
                )
            }
        }
        
        if case .TRACE = method {
            // A client MUST NOT send a message body in a TRACE request.
            // https://tools.ietf.org/html/rfc7230#section-4.3.8
            throw HTTPClientError.traceRequestWithBody
        }

        guard (encodings.lazy.filter { $0 == "chunked" }.count <= 1) else {
            throw HTTPClientError.chunkedSpecifiedMultipleTimes
        }

        if encodings.isEmpty {
            switch self.body {
            case .some(.byteBuffer(let byteBuffer)):
                head.headers.add(name: "content-length", value: "\(byteBuffer.readableBytes)")
            case .some(bytes(let sequence)):
                // if we have a content length header, we assume this was set correctly
                if head.headers.contains(name: "content-length") {
                    
                } else {
                    head.headers.add(name: "transfer-encoding", value: "chunked")
                }
                
            }
            
            
        }
    }
}
