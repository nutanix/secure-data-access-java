/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Unit tests for mapping a terminal read-stream failure to a per-region error.
 */
package com.nutanix.securedata.sdk.internal;

import com.nutanix.securedata.sdk.api.ErrorCode;
import com.nutanix.securedata.sdk.api.ReadError;
import com.nutanix.securedata.sdk.api.ReadRegion;
import com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.StreamFailure;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Covers how a region is reported from a whole-stream read failure captured while
 * the call was live. Mirrors {@link TerminalWriteErrorTest}: a refusal or send
 * failure must carry the real reason — a typed server code or a mapped transport
 * status — rather than a blanket internal error, so callers can decide whether to
 * back off and retry.
 */
class TerminalReadErrorTest {

    private static final ReadRegion REGION = new ReadRegion(4096, 512);

    @Test
    void preservesStreamFailureReason() {
        StreamFailure failure = new StreamFailure(ErrorCode.SERVER_BUSY, "server busy");

        ReadError error = GrpcVDiskClientSupport.terminalReadError(failure, REGION, "unused default");

        assertEquals(REGION.offset(), error.offset());
        assertEquals(REGION.length(), error.length());
        assertEquals(ErrorCode.SERVER_BUSY, error.errorCode());
        assertEquals("server busy", error.message());
    }

    @Test
    void keepsContextualMessageWhenStreamFailureMessageEmpty() {
        StreamFailure failure = new StreamFailure(ErrorCode.PERMISSION_DENIED, "");

        ReadError error = GrpcVDiskClientSupport.terminalReadError(
                failure, REGION, "Read stream failed");

        assertEquals(ErrorCode.PERMISSION_DENIED, error.errorCode(),
                "the typed reason survives even without a server message");
        assertEquals("Read stream failed", error.message());
    }

    @Test
    void mapsTransportStatusCause() {
        ReadError error = GrpcVDiskClientSupport.terminalReadError(
                Status.UNAVAILABLE.asRuntimeException(), REGION, "Read stream failed");

        assertEquals(ErrorCode.SERVER_BUSY, error.errorCode(),
                "a transport status cause must be mapped, not reported as internal");
        assertEquals("Read stream failed", error.message());
    }

    @Test
    void fallsBackToInternalWhenNoCause() {
        ReadError error = GrpcVDiskClientSupport.terminalReadError(null, REGION, "Read stream failed");

        assertEquals(ErrorCode.INTERNAL_ERROR, error.errorCode());
        assertEquals("Read stream failed", error.message());
    }
}
