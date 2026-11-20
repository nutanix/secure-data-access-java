/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Unit tests for forward-compatible decoding of unknown wire error codes.
 */
package com.nutanix.securedata.sdk.internal;

import com.nutanix.api.dataprotection.streaming.v1.VDiskErrorCode;
import com.nutanix.api.dataprotection.streaming.v1.VDiskReadRet;
import com.nutanix.api.dataprotection.streaming.v1.VDiskWriteRet;
import com.nutanix.securedata.sdk.api.ErrorCode;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Forward-compatibility guard for the {@code error_code} enum.
 *
 * <p>{@code VDiskErrorCode} is a proto2 (closed) enum, so when a newer server
 * sends a code this SDK build does not know, the generated parser does not store
 * it in the typed field — it drops the raw value into the message's unknown-field
 * set and {@code getErrorCode()} then returns the field default, {@code kNoError}.
 * Read naively, that turns a newer server <em>error</em> into a false success on
 * both the read and write paths.
 *
 * <p>These tests first pin down that hazardous generated-code behavior (so we are
 * alerted if a future protobuf/proto-syntax change alters it), then verify that
 * {@link GrpcVDiskClient#effectiveErrorCodeNumber} and
 * {@link GrpcVDiskClient#toErrorCode(int)} recover the real value and surface it
 * as {@link ErrorCode#UNKNOWN} instead of success.
 */
class WireErrorCodeCompatibilityTest {

    // A code beyond the highest one this SDK build knows (kRetry = 9). Stands in
    // for whatever a newer server might send.
    private static final int UNKNOWN_WIRE_CODE = 99;

    /**
     * Encodes {@code error_code = value} onto the wire and parses it back, so the
     * message goes through the real generated parser exactly as a server response
     * would. Using a builder cannot reproduce this: builders reject enum values
     * with no declared constant, whereas the wire parser silently diverts them to
     * the unknown-field set.
     */
    private static byte[] encodeErrorCode(int value) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // field 2, wire type 0 (varint): tag = (2 << 3) | 0 = 16.
        out.write(16);
        // Varint-encode the value.
        int v = value;
        while ((v & ~0x7F) != 0) {
            out.write((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        out.write(v);
        return out.toByteArray();
    }

    @Test
    void proto2DropsUnknownEnumValueAndReportsKNoError_writeRet() throws Exception {
        VDiskWriteRet parsed = VDiskWriteRet.parseFrom(encodeErrorCode(UNKNOWN_WIRE_CODE));

        // The hazard: the typed accessor hides the real value behind the default.
        assertEquals(VDiskErrorCode.kNoError, parsed.getErrorCode(),
                "proto2 closed enum should report the default for an unknown value");
        assertFalse(parsed.hasErrorCode(),
                "an unknown enum value should not be recorded as a set typed field");
        assertTrue(parsed.getUnknownFields().hasField(VDiskWriteRet.ERROR_CODE_FIELD_NUMBER),
                "the raw value should be preserved in the unknown-field set");
    }

    @Test
    void unknownWriteErrorIsSurfacedNotTreatedAsSuccess() throws Exception {
        VDiskWriteRet parsed = VDiskWriteRet.parseFrom(encodeErrorCode(UNKNOWN_WIRE_CODE));

        int effective = GrpcVDiskClientSupport.effectiveErrorCodeNumber(
                parsed.getErrorCode(), parsed.getUnknownFields());

        assertEquals(UNKNOWN_WIRE_CODE, effective,
                "the raw wire value must be recovered from the unknown-field set");
        assertNotEquals(VDiskErrorCode.kNoError_VALUE, effective,
                "a newer server error must not collapse to success");
        assertEquals(ErrorCode.UNKNOWN, GrpcVDiskClientSupport.toErrorCode(effective),
                "an unrecognized code must map to the stable public UNKNOWN");
    }

    @Test
    void unknownReadErrorIsSurfacedNotTreatedAsSuccess() throws Exception {
        VDiskReadRet parsed = VDiskReadRet.parseFrom(encodeErrorCode(UNKNOWN_WIRE_CODE));

        int effective = GrpcVDiskClientSupport.effectiveErrorCodeNumber(
                parsed.getErrorCode(), parsed.getUnknownFields());

        assertEquals(UNKNOWN_WIRE_CODE, effective);
        assertNotEquals(VDiskErrorCode.kNoError_VALUE, effective);
        assertEquals(ErrorCode.UNKNOWN, GrpcVDiskClientSupport.toErrorCode(effective));
    }

    @Test
    void knownErrorCodesPassThroughUnchanged() {
        for (VDiskErrorCode code : VDiskErrorCode.values()) {
            VDiskWriteRet msg = VDiskWriteRet.newBuilder().setErrorCode(code).build();
            int effective = GrpcVDiskClientSupport.effectiveErrorCodeNumber(
                    msg.getErrorCode(), msg.getUnknownFields());
            assertEquals(code.getNumber(), effective,
                    "known code " + code + " should be read straight from the typed field");
        }
    }

    @Test
    void kNoErrorIsTreatedAsSuccess() {
        VDiskWriteRet msg = VDiskWriteRet.newBuilder()
                .setErrorCode(VDiskErrorCode.kNoError)
                .build();
        int effective = GrpcVDiskClientSupport.effectiveErrorCodeNumber(
                msg.getErrorCode(), msg.getUnknownFields());
        assertEquals(VDiskErrorCode.kNoError_VALUE, effective);
        assertEquals(ErrorCode.NO_ERROR, GrpcVDiskClientSupport.toErrorCode(effective));
    }
}
