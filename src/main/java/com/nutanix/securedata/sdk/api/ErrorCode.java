/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Typed error codes the SDK surfaces for read and write operations.
 */
package com.nutanix.securedata.sdk.api;

/**
 * Typed status codes for failed read and write ranges, available via
 * {@link ReadError#errorCode()} and {@link WriteError#errorCode()}.
 *
 * <p>Values correspond to the server's reported error codes so applications can
 * branch on a stable enum rather than message text. Retry behavior is determined
 * by the caller.
 */
public enum ErrorCode {
    /** No error. Not expected on a failure callback; present for completeness. */
    NO_ERROR,
    /** Server hit an unexpected internal condition. */
    INTERNAL_ERROR,
    /** Request was malformed (for example a bad offset, length, or payload). */
    INVALID_ARGUMENT,
    /** The requested disk could not be found. */
    DISK_NOT_FOUND,
    /** The underlying disk read failed. */
    DISK_READ_ERROR,
    /** The underlying disk write failed. */
    DISK_WRITE_ERROR,
    /** The bearer token was rejected or lacks access. */
    PERMISSION_DENIED,
    /** A concurrent request was rejected on a stream that allows only one. */
    CONCURRENT_REQUEST_NOT_SUPPORTED,
    /**
     * The server refused the operation because it is at capacity — most often its
     * process-wide, shared limit on active read or write streams. This is a
     * back-off signal, not a data or region failure: the region was never touched,
     * so the whole operation should be retried after a delay (ideally with
     * exponential backoff) rather than treated as a permanent error.
     */
    SERVER_BUSY,
    /** Server explicitly asked the client to retry. */
    RETRY,
    /**
     * A time budget elapsed before the range completed: either the optional
     * overall transfer deadline, or the inactivity (connect/idle) timeout that
     * fires when the stream stops making progress. Distinct from a server
     * failure — the range's state is unknown, so it is typically safe to retry.
     */
    DEADLINE_EXCEEDED,
    /**
     * The RPC was cancelled before the range completed (by the caller, the
     * transport, or the peer). Like {@link #DEADLINE_EXCEEDED}, the range's state
     * is unknown rather than known-failed.
     */
    CANCELLED,
    /**
     * The server returned a code this SDK version does not recognize. Upgrade the
     * SDK to interpret newer codes.
     */
    UNKNOWN;
}
