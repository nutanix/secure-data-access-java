/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Stateless helpers for the gRPC client: channel setup, error mapping, and flow-control gates.
 */
package com.nutanix.securedata.sdk.internal;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import com.nutanix.api.dataprotection.streaming.v1.DiskDataRange;
import com.nutanix.api.dataprotection.streaming.v1.VDiskErrorCode;
import com.nutanix.api.dataprotection.streaming.v1.VDiskReadRet;
import com.nutanix.api.dataprotection.streaming.v1.VDiskWriteArg;
import com.nutanix.api.dataprotection.streaming.v1.VDiskWriteRet;
import com.nutanix.securedata.sdk.api.ErrorCode;
import com.nutanix.securedata.sdk.api.ReadError;
import com.nutanix.securedata.sdk.api.ReadHandler;
import com.nutanix.securedata.sdk.api.ReadRegion;
import com.nutanix.securedata.sdk.api.RecoveredChunk;
import com.nutanix.securedata.sdk.api.VDiskClientConfig;
import com.nutanix.securedata.sdk.api.WriteAck;
import com.nutanix.securedata.sdk.api.WriteError;
import com.nutanix.securedata.sdk.api.WriteHandler;
import com.nutanix.securedata.sdk.api.WriteRegion;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Stateless helpers backing {@link GrpcVDiskClient}: channel/credentials
 * construction, wire error-code mapping, per-callback exception containment, and
 * the small in-flight byte gate. Kept separate so the client class holds only its
 * connection state and the read/write flow, and so these pure helpers can be unit
 * tested without standing up a client.
 */
final class GrpcVDiskClientSupport {

    private static final Logger LOG = Logger.getLogger(GrpcVDiskClientSupport.class.getName());

    private static final Metadata.Key<String> AUTHORIZATION_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    // Wire field number of error_code, shared by VDiskReadRet and VDiskWriteRet.
    // Used to recover an unrecognized (newer) enum value from the unknown-field
    // set; see effectiveErrorCodeNumber.
    private static final int ERROR_CODE_FIELD_NUMBER = VDiskWriteRet.ERROR_CODE_FIELD_NUMBER;

    private GrpcVDiskClientSupport() {
    }

    /**
     * Creates a TLS gRPC channel with server-certificate verification skipped.
     *
     * <p>Authorization is carried by the bearer token, not the server
     * certificate, so the client trusts any server cert and needs no CA material
     * or authority override.
     */
    static ManagedChannel buildChannel(VDiskClientConfig config) {
        try {
            SslContext sslContext = GrpcSslContexts.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();

            NettyChannelBuilder builder = NettyChannelBuilder
                    .forAddress(config.host(), config.port())
                    .sslContext(sslContext)
                    // Allow large vdisk read chunks (server max is 100 MiB).
                    .maxInboundMessageSize(128 * 1024 * 1024);

            // Keepalive bounds a stalled or dead stream at the transport layer
            // instead of a whole-call deadline that any large transfer would blow
            // through: while a read or write call is active, gRPC pings the server
            // every keepAliveTime and fails the call if no ack arrives within
            // keepAliveTimeout. keepAliveWithoutCalls stays at its default (false),
            // so pings flow only during active calls and never storm an idle
            // connection. A non-positive keepAliveTime leaves keepalive off.
            long keepAliveTimeNanos = config.keepAliveTime().toNanos();
            if (keepAliveTimeNanos > 0) {
                builder.keepAliveTime(keepAliveTimeNanos, TimeUnit.NANOSECONDS)
                        .keepAliveTimeout(config.keepAliveTimeout().toNanos(), TimeUnit.NANOSECONDS);
            }

            return builder.build();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build TLS channel", e);
        }
    }

    /**
     * Builds call credentials that inject a bearer token on each RPC.
     *
     * <p>The token is attached as {@code authorization: Bearer <token>} metadata.
     */
    static CallCredentials bearerToken(String token) {
        return new CallCredentials() {
            @Override
            public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor,
                                             MetadataApplier applier) {
                appExecutor.execute(() -> {
                    Metadata headers = new Metadata();
                    headers.put(AUTHORIZATION_KEY, "Bearer " + token);
                    applier.apply(headers);
                });
            }
        };
    }

    /**
     * Builds a {@link ReadError} from a failed read response.
     *
     * <p>A data-path failure carries the failing {@code [offset, length)} in its
     * range vector; use it verbatim so the caller can retry exactly that range.
     * Setup-time failures carry no range, so fall back to the originating
     * region's bounds.
     *
     * @param errorCodeNumber the effective wire error value (see
     *                        {@link #effectiveErrorCodeNumber}), so a code newer
     *                        than this SDK build is surfaced rather than masked
     */
    static ReadError toReadError(ReadRegion region, VDiskReadRet value, int errorCodeNumber) {
        long offset = region.offset();
        long length = region.length();
        if (value.getRangeVecCount() > 0) {
            DiskDataRange range = value.getRangeVec(0);
            offset = range.getOffset();
            length = range.getLength();
        }
        return new ReadError(
                offset, length, toErrorCode(errorCodeNumber), value.getErrorMessage());
    }

    /**
     * Builds the terminal {@link ReadError} for a region from the stream's
     * captured failure cause, mirroring {@link #terminalWriteError}.
     *
     * <p>The cause is the whole-stream reason captured first-wins while the stream
     * was live: a {@link StreamFailure} (a refusal with a typed server code, or a
     * send-side failure wrapped with its context), any other transport
     * {@link Throwable} (a gRPC status, mapped by {@link #transportErrorCode}), or
     * {@code null} when the stream ended cleanly yet a region was still unreported.
     * A {@link StreamFailure}'s message is used when present; otherwise the
     * caller's contextual default is kept.
     */
    static ReadError terminalReadError(
            Throwable cause, ReadRegion region, String defaultMessage) {
        if (cause instanceof StreamFailure failure) {
            String message = failure.getMessage() == null || failure.getMessage().isEmpty()
                    ? defaultMessage
                    : failure.getMessage();
            return new ReadError(region.offset(), region.length(), failure.code(), message);
        }
        if (cause != null) {
            return new ReadError(region.offset(), region.length(),
                    transportErrorCode(cause), defaultMessage);
        }
        return new ReadError(
                region.offset(), region.length(), ErrorCode.INTERNAL_ERROR, defaultMessage);
    }

    /**
     * The effective {@code error_code} wire value, robust to proto2 closed-enum
     * behavior.
     *
     * <p>The {@code error_code} field is a proto2 (closed) enum, so a value newer
     * than this SDK build's {@link VDiskErrorCode} is <em>not</em> stored in the
     * typed field: the parser drops it into the message's unknown-field set and
     * {@code getErrorCode()} then reports the field default, {@code kNoError}.
     * Left unhandled, that turns a newer server error into a false success. This
     * reads the raw value back from the unknown-field set when present, so an
     * unrecognized code is surfaced (as {@link ErrorCode#UNKNOWN}) instead of
     * being mistaken for success.
     *
     * @param mapped        the typed enum from {@code getErrorCode()} (the default
     *                      {@code kNoError} when the wire value was unrecognized)
     * @param unknownFields the message's unknown-field set
     * @return the numeric wire value actually sent by the server
     */
    static int effectiveErrorCodeNumber(VDiskErrorCode mapped, UnknownFieldSet unknownFields) {
        if (unknownFields.hasField(ERROR_CODE_FIELD_NUMBER)) {
            // getVarintList: Grab the raw number(s).
            List<Long> varints = unknownFields.getField(ERROR_CODE_FIELD_NUMBER).getVarintList();
            if (!varints.isEmpty()) {
                // Last value wins, matching proto's repeated-scalar merge rule.
                return varints.get(varints.size() - 1).intValue();
            }
        }
        return mapped.getNumber();
    }

    /**
     * Maps a raw {@code error_code} wire value to the SDK's public
     * {@link ErrorCode}.
     *
     * <p>Any value this SDK build does not recognize — including a newer code that
     * arrived via {@link #effectiveErrorCodeNumber} — is reported as
     * {@link ErrorCode#UNKNOWN} so the public API stays stable when the server
     * adds new codes.
     */
    static ErrorCode toErrorCode(int errorCodeNumber) {
        VDiskErrorCode code = VDiskErrorCode.forNumber(errorCodeNumber);
        if (code == null) {
            // Runtime forward compatibility: an older SDK talking to a newer
            // server can receive a numeric code with no generated constant. It is
            // surfaced as UNKNOWN (never mistaken for another reason) and logged
            // so the gap is visible and the operator knows to upgrade the SDK.
            LOG.warning("Unrecognized VDiskErrorCode number " + errorCodeNumber
                    + "; reporting as UNKNOWN. Upgrade the SDK to interpret newer server codes.");
            return ErrorCode.UNKNOWN;
        }
        return switch (code) {
            case kNoError -> ErrorCode.NO_ERROR;
            case kInternalError -> ErrorCode.INTERNAL_ERROR;
            case kInvalidArgument -> ErrorCode.INVALID_ARGUMENT;
            case kDiskNotFound -> ErrorCode.DISK_NOT_FOUND;
            case kDiskReadError -> ErrorCode.DISK_READ_ERROR;
            case kDiskWriteError -> ErrorCode.DISK_WRITE_ERROR;
            case kPermissionDenied -> ErrorCode.PERMISSION_DENIED;
            case kConcurrentRequestNotSupported -> ErrorCode.CONCURRENT_REQUEST_NOT_SUPPORTED;
            case kServerBusy -> ErrorCode.SERVER_BUSY;
            case kRetry -> ErrorCode.RETRY;
            default -> ErrorCode.UNKNOWN;
        };
    }

    /**
     * Maps a transport-level failure — the {@link Throwable} delivered to a
     * stream's {@code onError} — to the SDK's public {@link ErrorCode}.
     *
     * <p>Such a failure is almost always a gRPC {@link io.grpc.StatusRuntimeException}
     * carrying a real {@link Status.Code} (the connection was refused, the token
     * was rejected, the server was overloaded, ...). Blindly reporting every one
     * as {@link ErrorCode#INTERNAL_ERROR} would hide that reason and rob callers
     * of a basis for retry decisions, so the status is translated to its closest
     * typed code. A non-status throwable, or a code with no clear counterpart,
     * falls back to {@link ErrorCode#INTERNAL_ERROR}.
     *
     * <p>{@code DEADLINE_EXCEEDED} and {@code CANCELLED} get their own typed codes
     * rather than collapsing to {@link ErrorCode#INTERNAL_ERROR}: on a long bulk
     * transfer a timeout or cancellation is an expected, retryable outcome the
     * caller must be able to tell apart from a genuine server bug.
     */
    static ErrorCode transportErrorCode(Throwable t) {
        return switch (Status.fromThrowable(t).getCode()) {
            case INVALID_ARGUMENT, OUT_OF_RANGE, FAILED_PRECONDITION -> ErrorCode.INVALID_ARGUMENT;
            case NOT_FOUND -> ErrorCode.DISK_NOT_FOUND;
            case PERMISSION_DENIED, UNAUTHENTICATED -> ErrorCode.PERMISSION_DENIED;
            case RESOURCE_EXHAUSTED, UNAVAILABLE -> ErrorCode.SERVER_BUSY;
            case DEADLINE_EXCEEDED -> ErrorCode.DEADLINE_EXCEEDED;
            case CANCELLED -> ErrorCode.CANCELLED;
            default -> ErrorCode.INTERNAL_ERROR;
        };
    }

    /**
     * Emits one {@link RecoveredChunk} per range in the response.
     *
     * <p>{@code VDiskReadRet.data} is the concatenation of the bytes for every
     * non-zero range in {@code range_vec}, in order; zero-fill ranges contribute
     * nothing. Each non-zero range therefore draws its own {@code length} bytes
     * as a cumulative slice of the payload — never the whole payload — so sparse
     * multi-range responses are reconstructed correctly. Slices are taken with
     * {@link ByteString#substring} (a zero-copy view), so no bytes are copied.
     * Each chunk goes out through {@link #safeOnChunk} so a throwing handler is
     * contained per chunk.
     *
     * <p>Before emitting anything, the ranges are validated to account for
     * exactly the payload; on a mismatch nothing is emitted and {@code false} is
     * returned so the caller can report the malformed response without ever
     * forwarding wrongly-sliced or partial chunks.
     *
     * @return {@code true} if the response was well-formed and its chunks were
     *     emitted; {@code false} if the payload and ranges disagreed
     */
    static boolean emitRecoveredChunks(VDiskReadRet value, ReadHandler handler) {
        ByteString payload = value.getData();
        int payloadSize = payload.size();

        // Sum the bytes the non-zero ranges claim and require it to match the
        // payload exactly: no underrun (a range would read past the payload) and
        // no leftover (bytes the ranges never account for). Summed as a long so a
        // bogus set of lengths cannot overflow into a false match.
        long claimed = 0;
        for (DiskDataRange range : value.getRangeVecList()) {
            if (!range.getZeroData()) {
                claimed += range.getLength();
            }
        }
        if (claimed != payloadSize) {
            LOG.warning("Malformed read response: non-zero ranges claim " + claimed
                    + " bytes but payload holds " + payloadSize);
            return false;
        }

        int consumed = 0;
        for (DiskDataRange range : value.getRangeVecList()) {
            ByteString data;
            if (range.getZeroData()) {
                data = ByteString.EMPTY;
            } else {
                int end = consumed + range.getLength();
                data = payload.substring(consumed, end);
                consumed = end;
            }
            safeOnChunk(handler, new RecoveredChunk(
                    range.getOffset(), range.getLength(), data, range.getZeroData()));
        }
        return true;
    }

    /**
     * Delivers a recovered chunk without letting a handler exception escape.
     *
     * <p>Callback exceptions belong to the SDK: a {@link ReadHandler} that throws
     * on the transport thread is logged and suppressed so it cannot break the
     * stream bookkeeping the read relies on. A suppressed exception means that
     * chunk may go unpersisted, so callers should signal problems through their
     * own state rather than by throwing.
     */
    static void safeOnChunk(ReadHandler handler, RecoveredChunk chunk) {
        try {
            handler.onChunk(chunk);
        } catch (Throwable t) {
            LOG.log(Level.WARNING, "ReadHandler.onChunk threw; suppressing to protect the read", t);
        }
    }

    /**
     * Delivers a read failure without letting a handler exception escape, for the
     * same reason as {@link #safeOnChunk}. Its callers complete the region's
     * future right after, so a suppressed exception cannot leave the read wedged.
     */
    static void safeOnError(ReadHandler handler, ReadError error) {
        try {
            handler.onError(error);
        } catch (Throwable t) {
            LOG.log(Level.WARNING, "ReadHandler.onError threw; suppressing to protect the read", t);
        }
    }

    /**
     * Builds one write arg for a region. {@code zeroData} mirrors
     * {@link WriteRegion#zeroData()}: a caller-declared zero-fill hole carries no
     * payload, while a data region attaches its bytes.
     */
    static VDiskWriteArg buildWriteArg(
            long sequenceNumber, WriteRegion region, boolean zeroData, ByteString data) {
        VDiskWriteArg.Builder argBuilder = VDiskWriteArg.newBuilder()
                .setSequenceNumber(sequenceNumber)
                .addRangeVec(DiskDataRange.newBuilder()
                        .setOffset(region.offset())
                        .setLength(region.length())
                        .setZeroData(zeroData)
                        .build());
        if (!zeroData) {
            // The provider's ByteString is immutable, so it can back the proto
            // directly without a defensive copy.
            argBuilder.setData(data);
        }
        return argBuilder.build();
    }

    /**
     * Delivers a write acknowledgement without letting a handler exception escape.
     *
     * <p>Callback exceptions belong to the SDK: a {@link WriteHandler} that throws
     * on the transport thread would otherwise cancel the shared write stream and
     * sink every other region. It is logged and suppressed instead.
     */
    static void safeOnSuccess(WriteHandler handler, WriteAck ack) {
        try {
            handler.onSuccess(ack);
        } catch (Throwable t) {
            LOG.log(Level.WARNING, "WriteHandler.onSuccess threw; suppressing to protect the write", t);
        }
    }

    /**
     * Delivers a write failure without letting a handler exception escape, for the
     * same reason as {@link #safeOnSuccess}.
     */
    static void safeOnError(WriteHandler handler, WriteError error) {
        try {
            handler.onError(error);
        } catch (Throwable t) {
            LOG.log(Level.WARNING, "WriteHandler.onError threw; suppressing to protect the write", t);
        }
    }

    /**
     * Bytes a region reserves from the write byte budget: a data region reserves
     * its full length (pulled into memory before sending), a zero-fill region
     * reserves nothing. Computed purely from the immutable region, so the sender's
     * reservation and the ack/drain release always agree.
     */
    static long budgetReservationFor(WriteRegion region) {
        return region.zeroData() ? 0L : region.length();
    }

    /**
     * Returns the stream-level failure to raise for a write response that matched
     * no pending region, or {@code null} if it should merely be ignored.
     *
     * <p>Sequence numbers start at 1, so an unsequenced ({@code seq == 0})
     * response that carries an error is Stargate's connection-admission
     * rejection, sent just before it cancels the stream. Its typed reason (server
     * busy, permission denied, concurrent writer, ...) is surfaced so outstanding
     * and unsent regions can later be failed with the real cause. Any other
     * unmatched response — a stray or duplicate delivery — returns {@code null}.
     */
    static StreamFailure streamFailureFor(VDiskWriteRet value) {
        int errorCodeNumber = effectiveErrorCodeNumber(
                value.getErrorCode(), value.getUnknownFields());
        if (value.getSequenceNumber() == 0 && errorCodeNumber != VDiskErrorCode.kNoError_VALUE) {
            return new StreamFailure(toErrorCode(errorCodeNumber), value.getErrorMessage());
        }
        return null;
    }

    /**
     * Builds the terminal {@link WriteError} for a region that never got its own
     * ack (left in {@code pending} or never sent), from the stream's failure
     * cause.
     *
     * <p>The cause is the whole-stream reason preserved by {@code streamClosed}:
     * a {@link StreamFailure} (an admission rejection with a typed server code),
     * any other transport {@link Throwable} (a gRPC status, mapped by
     * {@link #transportErrorCode}), or {@code null} when the stream ended cleanly
     * yet a region was still unacked. In every case the real cause is surfaced so
     * callers see it (server busy, permission denied, ...) rather than a blanket
     * internal error. A {@link StreamFailure}'s server message is used when
     * present; otherwise the caller's contextual default is kept.
     */
    static WriteError terminalWriteError(
            Throwable streamFailureCause, WriteRegion region, String defaultMessage) {
        if (streamFailureCause instanceof StreamFailure failure) {
            String message = failure.getMessage() == null || failure.getMessage().isEmpty()
                    ? defaultMessage
                    : failure.getMessage();
            return new WriteError(region.offset(), region.length(), failure.code(), message);
        }
        if (streamFailureCause != null) {
            return new WriteError(region.offset(), region.length(),
                    transportErrorCode(streamFailureCause), defaultMessage);
        }
        return new WriteError(
                region.offset(), region.length(), ErrorCode.INTERNAL_ERROR, defaultMessage);
    }

    /**
     * A stream-level write failure carrying a typed server code, delivered through
     * {@code streamClosed}'s exceptional completion. Stargate reports a
     * connection-admission rejection as an unsequenced {@code VDiskWriteRet} and
     * then cancels the stream; this preserves that typed reason so the generic
     * cancellation cannot mask it. It is a carrier only — never thrown to unwind
     * the stack — so it fills in no stack trace.
     */
    static final class StreamFailure extends RuntimeException {
        private final ErrorCode code;

        StreamFailure(ErrorCode code, String message) {
            super(message, null, false /* enableSuppression */, false /* writableStackTrace */);
            this.code = code;
        }

        ErrorCode code() {
            return code;
        }
    }

    /**
     * A bytes-denominated gate that bounds the aggregate unacked write payload in
     * flight, and thus peak client memory. Unlike a
     * {@link java.util.concurrent.Semaphore}, whose permit count is an {@code int}
     * and so caps at ~2 GiB, this holds the used total as a {@code long}, so a
     * byte budget larger than {@link Integer#MAX_VALUE} is supported.
     *
     * <p>The sender reserves a region's bytes before pulling them and releases the
     * same amount when the region is acked (or when it is dropped without being
     * sent). {@link #abort()} wakes any waiter when the stream tears down, so a
     * sender parked for room on a stream that will never ack again is not stuck;
     * the woken reservation is still counted and released by the caller, keeping
     * the accounting balanced.
     */
    static final class ByteBudget {
        private final long capacity;
        private final ReentrantLock lock = new ReentrantLock(true /* fair */);
        private final Condition hasRoom = lock.newCondition();
        private long used;
        private boolean aborted;

        ByteBudget(long capacity) {
            this.capacity = capacity;
        }

        /**
         * Blocks (uninterruptibly, to match the write path's semaphore) until the
         * budget can admit {@code bytes}, then reserves them. A single reservation
         * never exceeds {@code capacity} (the config enforces
         * {@code maxInFlightWriteBytes >= maxWriteBytesPerRequest} and the sender
         * rejects any region larger than that before reserving), so this always
         * eventually succeeds as earlier reservations drain. Once aborted it stops
         * waiting and reserves regardless, so the caller's matching release keeps
         * the total balanced.
         *
         * <p>A reservation larger than {@code capacity} could never be admitted and
         * would otherwise wait forever, so it is rejected fast rather than parked.
         * The sender's up-front size check makes this unreachable; it is a
         * defensive guard against a future caller skipping that check.
         *
         * @throws IllegalArgumentException if {@code bytes > capacity}
         */
        void acquire(long bytes) {
            if (bytes <= 0) {
                return;
            }
            if (bytes > capacity) {
                throw new IllegalArgumentException(
                        "write byte reservation " + bytes
                                + " exceeds in-flight write byte budget capacity "
                                + capacity);
            }
            lock.lock();
            try {
                while (!aborted && used + bytes > capacity) {
                    hasRoom.awaitUninterruptibly();
                }
                used += bytes;
            } finally {
                lock.unlock();
            }
        }

        /** Returns {@code bytes} to the budget and wakes any waiter. */
        void release(long bytes) {
            if (bytes <= 0) {
                return;
            }
            lock.lock();
            try {
                used -= bytes;
                hasRoom.signalAll();
            } finally {
                lock.unlock();
            }
        }

        /** Stops blocking new and pending acquirers; used when the stream tears down. */
        void abort() {
            lock.lock();
            try {
                aborted = true;
                hasRoom.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }
}
