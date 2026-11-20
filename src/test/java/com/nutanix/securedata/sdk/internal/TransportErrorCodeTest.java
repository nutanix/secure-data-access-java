/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Unit tests for mapping gRPC transport status codes to SDK error codes.
 */
package com.nutanix.securedata.sdk.internal;

import com.nutanix.securedata.sdk.api.ErrorCode;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins the translation of a transport-level failure (the gRPC status on a
 * stream's {@code onError}) to a typed {@link ErrorCode}. Reporting every stream
 * failure as {@link ErrorCode#INTERNAL_ERROR} would hide the real cause and give
 * callers no basis to decide whether to retry, so the closest typed code is used.
 */
class TransportErrorCodeTest {

    @Test
    void permissionAndAuthMapToPermissionDenied() {
        assertEquals(ErrorCode.PERMISSION_DENIED,
                GrpcVDiskClientSupport.transportErrorCode(Status.PERMISSION_DENIED.asRuntimeException()));
        assertEquals(ErrorCode.PERMISSION_DENIED,
                GrpcVDiskClientSupport.transportErrorCode(Status.UNAUTHENTICATED.asRuntimeException()));
    }

    @Test
    void overloadAndUnavailableMapToServerBusy() {
        assertEquals(ErrorCode.SERVER_BUSY,
                GrpcVDiskClientSupport.transportErrorCode(Status.RESOURCE_EXHAUSTED.asRuntimeException()));
        assertEquals(ErrorCode.SERVER_BUSY,
                GrpcVDiskClientSupport.transportErrorCode(Status.UNAVAILABLE.asRuntimeException()));
    }

    @Test
    void notFoundMapsToDiskNotFound() {
        assertEquals(ErrorCode.DISK_NOT_FOUND,
                GrpcVDiskClientSupport.transportErrorCode(Status.NOT_FOUND.asRuntimeException()));
    }

    @Test
    void badRequestStatusesMapToInvalidArgument() {
        assertEquals(ErrorCode.INVALID_ARGUMENT,
                GrpcVDiskClientSupport.transportErrorCode(Status.INVALID_ARGUMENT.asRuntimeException()));
        assertEquals(ErrorCode.INVALID_ARGUMENT,
                GrpcVDiskClientSupport.transportErrorCode(Status.OUT_OF_RANGE.asRuntimeException()));
        assertEquals(ErrorCode.INVALID_ARGUMENT,
                GrpcVDiskClientSupport.transportErrorCode(Status.FAILED_PRECONDITION.asRuntimeException()));
    }

    @Test
    void timeoutAndCancellationGetTheirOwnCodes() {
        // A bulk transfer that times out or is cancelled must be distinguishable
        // from a genuine server bug so the caller can retry rather than escalate.
        assertEquals(ErrorCode.DEADLINE_EXCEEDED,
                GrpcVDiskClientSupport.transportErrorCode(Status.DEADLINE_EXCEEDED.asRuntimeException()));
        assertEquals(ErrorCode.CANCELLED,
                GrpcVDiskClientSupport.transportErrorCode(Status.CANCELLED.asRuntimeException()));
    }

    @Test
    void unmappedStatusesFallBackToInternalError() {
        assertEquals(ErrorCode.INTERNAL_ERROR,
                GrpcVDiskClientSupport.transportErrorCode(Status.INTERNAL.asRuntimeException()));
        assertEquals(ErrorCode.INTERNAL_ERROR,
                GrpcVDiskClientSupport.transportErrorCode(Status.DATA_LOSS.asRuntimeException()));
    }

    @Test
    void nonStatusThrowableFallsBackToInternalError() {
        assertEquals(ErrorCode.INTERNAL_ERROR,
                GrpcVDiskClientSupport.transportErrorCode(new IOException("connection reset")));
    }
}
