/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Unit tests for mapping a terminal write-stream failure to a per-region error.
 */
package com.nutanix.securedata.sdk.internal;

import com.nutanix.securedata.sdk.api.ErrorCode;
import com.nutanix.securedata.sdk.api.WriteError;
import com.nutanix.securedata.sdk.api.WriteRegion;
import com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.StreamFailure;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Covers how a region that never received its own ack is reported. When the write
 * stream fails as a whole, each such region must carry the real reason — a typed
 * admission code or a mapped transport status — rather than a blanket internal
 * error, so callers can decide whether to retry.
 */
class TerminalWriteErrorTest {

    private static final WriteRegion REGION = WriteRegion.ofData(4096, 512);

    @Test
    void preservesStreamFailureReason() {
        StreamFailure failure = new StreamFailure(ErrorCode.PERMISSION_DENIED, "not allowed");

        WriteError error = GrpcVDiskClientSupport.terminalWriteError(failure, REGION, "unused default");

        assertEquals(ErrorCode.PERMISSION_DENIED, error.errorCode());
        assertEquals("not allowed", error.message());
    }

    @Test
    void keepsContextualMessageWhenStreamFailureMessageEmpty() {
        StreamFailure failure = new StreamFailure(ErrorCode.SERVER_BUSY, "");

        WriteError error = GrpcVDiskClientSupport.terminalWriteError(
                failure, REGION, "Write stream closed before region was sent");

        assertEquals(ErrorCode.SERVER_BUSY, error.errorCode(),
                "the typed reason survives even without a server message");
        assertEquals("Write stream closed before region was sent", error.message());
    }

    @Test
    void mapsTransportStatusCause() {
        WriteError error = GrpcVDiskClientSupport.terminalWriteError(
                Status.UNAVAILABLE.asRuntimeException(), REGION, "No write response received");

        assertEquals(REGION.offset(), error.offset());
        assertEquals(REGION.length(), error.length());
        assertEquals(ErrorCode.SERVER_BUSY, error.errorCode(),
                "a transport status cause must be mapped, not reported as internal");
        assertEquals("No write response received", error.message());
    }

    @Test
    void mapsPermissionDeniedCause() {
        WriteError error = GrpcVDiskClientSupport.terminalWriteError(
                Status.PERMISSION_DENIED.asRuntimeException(),
                REGION,
                "Write stream closed before region was sent");

        assertEquals(ErrorCode.PERMISSION_DENIED, error.errorCode());
        assertEquals("Write stream closed before region was sent", error.message());
    }

    @Test
    void fallsBackToInternalWhenNoCause() {
        WriteError error = GrpcVDiskClientSupport.terminalWriteError(null, REGION, "No write response received");

        assertEquals(ErrorCode.INTERNAL_ERROR, error.errorCode());
        assertEquals("No write response received", error.message());
    }
}
