/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Unit tests for treating unsequenced write responses as stream-level failures.
 */
package com.nutanix.securedata.sdk.internal;

import com.nutanix.api.dataprotection.streaming.v1.VDiskErrorCode;
import com.nutanix.api.dataprotection.streaming.v1.VDiskReadRet;
import com.nutanix.api.dataprotection.streaming.v1.VDiskWriteRet;
import com.nutanix.securedata.sdk.api.ErrorCode;
import com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.StreamFailure;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Guards preservation of Stargate's connection-admission rejection.
 *
 * <p>Stargate reports admission failures (server busy, permission denied,
 * concurrent writer, ...) as a {@code VDiskWriteRet} with no sequence number and
 * then cancels the stream. That response matches no pending region, so if it were
 * discarded the transport cancellation that follows would reduce every
 * outstanding and unsent region to a generic internal error, hiding the real
 * cause. These tests pin the capture that keeps the typed reason alive.
 */
class UnsequencedWriteFailureTest {

    @Test
    void unsequencedErrorIsCapturedAsStreamFailure() {
        VDiskWriteRet admissionReject = VDiskWriteRet.newBuilder()
                .setErrorCode(VDiskErrorCode.kServerBusy)
                .setErrorMessage("connection admission failed")
                .build();

        StreamFailure failure = GrpcVDiskClientSupport.streamFailureFor(admissionReject);

        assertEquals(ErrorCode.SERVER_BUSY, failure.code(),
                "an unsequenced typed error must be retained as a stream-level failure");
        assertEquals("connection admission failed", failure.getMessage());
    }

    @Test
    void unsequencedErrorWithNewerCodeIsCapturedAsUnknown() throws Exception {
        // A code newer than this SDK build (see WireErrorCodeCompatibilityTest):
        // it lands in the unknown-field set, but must still be surfaced.
        VDiskWriteRet parsed = VDiskWriteRet.parseFrom(new byte[] {16, 99});

        StreamFailure failure = GrpcVDiskClientSupport.streamFailureFor(parsed);

        assertEquals(ErrorCode.UNKNOWN, failure.code(),
                "a newer unsequenced error must not collapse to success");
    }

    @Test
    void unsequencedSuccessIsIgnored() {
        VDiskWriteRet strayOk = VDiskWriteRet.newBuilder()
                .setErrorCode(VDiskErrorCode.kNoError)
                .build();

        assertNull(GrpcVDiskClientSupport.streamFailureFor(strayOk),
                "an unsequenced kNoError is not a failure and must be ignored");
    }

    @Test
    void sequencedResponseIsNotTreatedAsStreamFailure() {
        // A response that echoes a real sequence number belongs to a region (or is
        // a stray duplicate); it must never become the whole stream's reason.
        VDiskWriteRet sequencedError = VDiskWriteRet.newBuilder()
                .setSequenceNumber(7)
                .setErrorCode(VDiskErrorCode.kServerBusy)
                .build();

        assertNull(GrpcVDiskClientSupport.streamFailureFor(sequencedError),
                "a sequenced response must not be captured as a stream-level failure");
    }

    @Test
    void readRetIsUnaffected() {
        // streamFailureFor is write-only; a VDiskReadRet is never passed to it.
        // This just documents that the two response types stay separate.
        VDiskReadRet readError = VDiskReadRet.newBuilder()
                .setErrorCode(VDiskErrorCode.kDiskReadError)
                .build();
        assertEquals(VDiskErrorCode.kDiskReadError, readError.getErrorCode());
    }
}
