/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * gRPC-backed VDiskClient implementing the streaming read and write paths.
 */
package com.nutanix.securedata.sdk.internal;

import com.google.protobuf.ByteString;
import com.nutanix.api.dataprotection.streaming.v1.VDiskDataSvcGrpc;
import com.nutanix.api.dataprotection.streaming.v1.VDiskErrorCode;
import com.nutanix.api.dataprotection.streaming.v1.VDiskReadArg;
import com.nutanix.api.dataprotection.streaming.v1.VDiskReadRet;
import com.nutanix.api.dataprotection.streaming.v1.VDiskWriteArg;
import com.nutanix.api.dataprotection.streaming.v1.VDiskWriteRet;
import com.nutanix.securedata.sdk.api.ErrorCode;
import com.nutanix.securedata.sdk.api.ReadError;
import com.nutanix.securedata.sdk.api.ReadHandler;
import com.nutanix.securedata.sdk.api.ReadRegion;
import com.nutanix.securedata.sdk.api.ReadRegionsRequest;
import com.nutanix.securedata.sdk.api.VDiskClient;
import com.nutanix.securedata.sdk.api.VDiskClientConfig;
import com.nutanix.securedata.sdk.api.WriteAck;
import com.nutanix.securedata.sdk.api.WriteDataProvider;
import com.nutanix.securedata.sdk.api.WriteError;
import com.nutanix.securedata.sdk.api.WriteHandler;
import com.nutanix.securedata.sdk.api.WriteRegion;
import com.nutanix.securedata.sdk.api.WriteRegionsRequest;
import com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.ByteBudget;
import com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.StreamFailure;
import io.grpc.ManagedChannel;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.bearerToken;
import static com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.buildChannel;
import static com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.buildWriteArg;
import static com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.budgetReservationFor;
import static com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.effectiveErrorCodeNumber;
import static com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.emitRecoveredChunks;
import static com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.safeOnError;
import static com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.safeOnSuccess;
import static com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.streamFailureFor;
import static com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.terminalReadError;
import static com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.terminalWriteError;
import static com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.toErrorCode;
import static com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.toReadError;
import static com.nutanix.securedata.sdk.internal.GrpcVDiskClientSupport.transportErrorCode;

public final class GrpcVDiskClient implements VDiskClient {
    private static final Logger LOG = Logger.getLogger(GrpcVDiskClient.class.getName());

    private final ManagedChannel channel;
    private final VDiskDataSvcGrpc.VDiskDataSvcStub asyncStub;
    // Optional hard cap on the whole operation, applied as the gRPC call deadline
    // (0 = unbounded). Stream liveness is otherwise bounded by channel keepalive
    // (see buildChannel), not by a per-chunk watchdog, so a large transfer is not
    // killed while it keeps making progress.
    private final long overallDeadlineMillis;
    // Bounds read streams in flight across the whole client, not just within a
    // single read() call, so concurrent read() calls on the same client share
    // one pool of permits and can never collectively exceed the server's cap.
    private final Semaphore readPermits;
    // Server-derived write caps: the most payload bytes allowed in one request,
    // how many requests may be in flight (sent but unacked) on a stream, and the
    // aggregate unacked payload bytes allowed in flight (bounds client memory).
    private final long maxWriteBytesPerRequest;
    private final int maxInFlightWriteRequests;
    private final long maxInFlightWriteBytes;
    private final AtomicLong sequenceCounter = new AtomicLong(1L);

    /**
     * Builds a transport-backed client from high-level SDK config.
     *
     * <p>This method creates the gRPC channel, applies optional bearer-token credentials,
     * and stores timeout settings that are used per request.
     */
    public GrpcVDiskClient(VDiskClientConfig config) {
        this.channel = buildChannel(config);
        VDiskDataSvcGrpc.VDiskDataSvcStub stub = VDiskDataSvcGrpc.newStub(channel);
        if (config.authToken() != null && !config.authToken().isEmpty()) {
            stub = stub.withCallCredentials(bearerToken(config.authToken()));
        }
        this.asyncStub = stub;
        this.overallDeadlineMillis = config.overallDeadline().toMillis();
        // Fair so that concurrent read() callers are served in arrival order and
        // no single call can starve another while both drain the shared pool.
        this.readPermits = new Semaphore(config.maxConcurrentReads(), true /* fair */);
        this.maxWriteBytesPerRequest = config.maxWriteBytesPerRequest();
        this.maxInFlightWriteRequests = config.maxInFlightWriteRequests();
        this.maxInFlightWriteBytes = config.maxInFlightWriteBytes();
    }

    /**
     * Returns the stub used to start a read or write call. If an overall deadline
     * is configured, it is attached here so the whole call fails with
     * {@code DEADLINE_EXCEEDED} once it elapses. If not (the default), the plain
     * stub is returned with no deadline, and channel keepalive is what bounds a
     * stalled or dead transfer instead.
     */
    private VDiskDataSvcGrpc.VDiskDataSvcStub callStub() {
        return overallDeadlineMillis > 0
                ? asyncStub.withDeadlineAfter(overallDeadlineMillis, TimeUnit.MILLISECONDS)
                : asyncStub;
    }

    /**
     * Multi-region SDK streaming read entrypoint.
     *
     * <p>Each region is read on its own stream (the server allows only one read
     * per stream). Read streams in flight are capped by the client (a conservative
     * default that sits well below the server's process-wide, shared stream
     * limit); additional regions are queued and started as earlier reads complete,
     * so an unbounded region list is accepted without piling onto that shared
     * server budget. A stream the server refuses past its limit is reported as
     * {@link com.nutanix.securedata.sdk.api.ErrorCode#SERVER_BUSY} — a back-off
     * signal to retry the whole read later, not a per-range data failure.
     * Concurrent {@code read()} calls on the same client are safe: they share the
     * per-client permit pool, so their combined in-flight streams stay under the
     * cap. Recovered data and per-range failures are delivered to {@code handler}
     * as they arrive; callbacks run concurrently on independent threads, 
     * so the handler must be thread-safe. The call returns only every region has completed.
     */
    @Override
    public void read(ReadRegionsRequest request, ReadHandler handler) {
        Objects.requireNonNull(handler, "handler is required");
        List<ReadRegion> regions = request.regions();

        // Bound the number of in-flight read streams. The calling thread walks
        // the whole region list, blocking on a free permit before launching each
        // read; each read releases its permit on completion, which lets the next
        // queued region start. Permits come from the shared per-client pool, so
        // this both unbounds the caller's region count and keeps concurrent
        // read() calls collectively under the cap.
        //
        // The handler is passed straight to each region's stream: callbacks are
        // NOT serialized, so they may run concurrently on independent gRPC
        // threads. The handler is responsible for its own thread safety.
        List<CompletableFuture<Void>> futures = new ArrayList<>(regions.size());
        for (ReadRegion region : regions) {
            readPermits.acquireUninterruptibly();
            CompletableFuture<Void> future = readRegionAsync(region, handler);
            future.whenComplete((ignored, throwable) -> readPermits.release());
            futures.add(future);
        }

        // Each per-region future always completes normally (failures are routed
        // to the handler), so waiting for all of them never throws.
        CompletableFuture
                .allOf(futures.toArray(new CompletableFuture[0]))
                .join();
    }

    /**
     * Starts one read stream for a single region and forwards its chunks and any
     * per-range failure to {@code handler} as they arrive.
     *
     * <p>The server may send chunks unordered; callers use each chunk's absolute
     * offset to place data. A per-chunk error does not terminate the stream:
     * Stargate reports the failed range and continues sending the remaining
     * chunks until EOF, so failures are forwarded to {@code handler} as they
     * arrive while good chunks keep flowing. The returned future completes only
     * on the stream's terminal event ({@code onCompleted}/{@code onError}) or a
     * send-side setup failure, and always completes normally so one region cannot
     * sink the whole read.
     */
    private CompletableFuture<Void> readRegionAsync(ReadRegion region, ReadHandler handler) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        // The whole-stream failure reason, captured first-wins exactly like the
        // write path: a refusal seen in onNext (an error with no range vector —
        // kServerBusy at the shared cap, kPermissionDenied,
        // kConcurrentRequestNotSupported) or a send-side failure below, each held
        // as a StreamFailure carrying the typed code and message. The stream is
        // cancelled or closed right after, so the terminal (onError/onCompleted)
        // reports this captured reason exactly once and the transport CANCELLED
        // that follows is treated as fallout, not a second failure. Stays null for
        // a clean stream and for per-range errors (which are reported inline as
        // they arrive and leave the stream flowing).
        AtomicReference<Throwable> failureReason = new AtomicReference<>();

        StreamObserver<VDiskReadRet> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(VDiskReadRet value) {
                if (future.isDone()) {
                    // The stream was already torn down (e.g. a send-side failure);
                    // ignore any stray delivery.
                    return;
                }
                // A per-chunk error is NOT terminal: Stargate keeps sending the
                // remaining chunks until EOF, so report the failed range and keep
                // consuming. The future is completed only by the terminal
                // onCompleted/onError below, which is also where the concurrency
                // permit is released.
                int errorCodeNumber = effectiveErrorCodeNumber(
                        value.getErrorCode(), value.getUnknownFields());
                if (errorCodeNumber != VDiskErrorCode.kNoError_VALUE) {
                    // The absence of a range vector — not the specific code — is
                    // what marks a whole-stream refusal (admission or request
                    // level: kServerBusy at the shared cap, kPermissionDenied,
                    // kConcurrentRequestNotSupported). The server attaches a range
                    // vector to every per-range data error and none to a refusal,
                    // and it cancels or closes the stream right after a refusal.
                    // Keying on the code instead would miss the non-busy refusals
                    // and cannot separate a per-range kPermissionDenied (range
                    // vector present, stream keeps flowing) from the admission one.
                    // Capture it as the stream's terminal reason (first-wins) and
                    // let the terminal report it once, so the ensuing CANCELLED is
                    // not reported a second time.
                    if (value.getRangeVecCount() == 0) {
                        failureReason.compareAndSet(null, new StreamFailure(
                                toErrorCode(errorCodeNumber), value.getErrorMessage()));
                        return;
                    }
                    // A per-range data error is not terminal: report it inline and
                    // keep consuming until EOF.
                    safeOnError(handler, toReadError(region, value, errorCodeNumber));
                    return;
                }
                // Forward the recovered chunks; each onChunk is contained. A
                // response whose payload and ranges disagree is reported as a
                // failed region (retry the whole region) without tearing down the
                // stream, so later responses still flow.
                if (!emitRecoveredChunks(value, handler)) {
                    safeOnError(handler, new ReadError(
                            region.offset(), region.length(),
                            ErrorCode.INTERNAL_ERROR,
                            "Malformed read response: payload does not match range vector"));
                }
            }

            @Override
            public void onError(Throwable t) {
                // A whole-stream reason was captured while the call was live (a
                // refusal in onNext, or a send-side failure below); report it once
                // here — the transport CANCELLED that drove this onError is fallout
                // of that reason, not a new failure.
                Throwable captured = failureReason.get();
                if (captured != null) {
                    safeOnError(handler, terminalReadError(
                            captured, region, "Read stream failed"));
                    future.complete(null);
                    return;
                }
                // Otherwise map the transport status directly: a keepalive failure
                // or overall-deadline expiry arrives here as UNAVAILABLE or
                // DEADLINE_EXCEEDED and is surfaced as the matching typed code.
                safeOnError(handler, new ReadError(
                        region.offset(), region.length(),
                        transportErrorCode(t),
                        "Read stream error: " + t));
                future.complete(null);
            }

            @Override
            public void onCompleted() {
                // A request-level refusal captured in onNext can be followed by a
                // clean half-close rather than a cancel; report the captured reason
                // once here too so it is never dropped.
                Throwable captured = failureReason.get();
                if (captured != null) {
                    safeOnError(handler, terminalReadError(
                            captured, region, "Read stream closed after failure"));
                }
                future.complete(null);
            }
        };

        // Open the stream, send the single VDiskReadArg (offset + length), then
        // immediately half-close the request side because the contract is "one
        // read request per stream." Any failure opening or feeding the stream is
        // reported to the handler and completes the future, so the future always
        // completes and the permit is always released.
        try {
            ClientCallStreamObserver<VDiskReadArg> requestObserver =
                    (ClientCallStreamObserver<VDiskReadArg>) callStub().vDiskStreamRead(responseObserver);
            try {
                requestObserver.onNext(VDiskReadArg.newBuilder()
                        .setOffset(region.offset())
                        .setLength(region.length())
                        .build());
                requestObserver.onCompleted();
            } catch (RuntimeException e) {
                // Capture the send failure as this stream's terminal reason
                // (first-wins), then cancel so the transport is torn down; the
                // CANCELLED that cancel drives into responseObserver.onError
                // reports the captured reason exactly once. Wrapping it as a
                // StreamFailure preserves the code and context message through that
                // terminal. If cancelling itself throws, report here as a fallback
                // so the region always completes — contained so a throw cannot fall
                // through to the outer catch and report the region a second time.
                StreamFailure sendFailure = new StreamFailure(transportErrorCode(e),
                        "Failed to send read request: " + e.getMessage());
                failureReason.compareAndSet(null, sendFailure);
                try {
                    requestObserver.onError(e);
                } catch (RuntimeException ignored) {
                    safeOnError(handler, terminalReadError(sendFailure, region, "Read stream failed"));
                    future.complete(null);
                }
            }
        } catch (RuntimeException e) {
            // Could not even open the stream; fail the region so its future
            // completes and the acquired read permit is released.
            safeOnError(handler, new ReadError(
                    region.offset(), region.length(),
                    transportErrorCode(e),
                    "Failed to start read stream: " + e.getMessage()));
            future.complete(null);
        }

        return future;
    }

    /**
     * Multi-region SDK streaming write entrypoint.
     *
     * <p>Each region maps to a single {@code VDiskWriteArg} tagged with an
     * SDK-internal sequence number, pipelined on a single write stream. Two
     * independent gates bound what is in flight (sent but unacked): at most
     * {@code maxInFlightWriteRequests} requests by count, and at most
     * {@code maxInFlightWriteBytes} of aggregate payload by size. The sender
     * blocks for both a free request slot and byte-budget room before pulling the
     * next region's bytes from {@code dataProvider}, so an unbounded region list
     * is queued rather than rejected and peak client memory is bounded by
     * {@code maxInFlightWriteBytes} regardless of individual region sizes — not by
     * the far larger {@code maxInFlightWriteRequests * maxWriteBytesPerRequest}
     * count window. Both a request slot and the region's byte reservation are
     * released when its ack arrives.
     *
     * <p>A region whose pulled data exceeds {@code maxWriteBytesPerRequest}
     * (which the server would reject) is not sent; it is reported via
     * {@link WriteHandler#onError}. A region declared a zero-fill hole with
     * {@link com.nutanix.securedata.sdk.api.WriteRegion#ofZeros} carries no bytes
     * on the wire; the SDK does not scan data regions for zeros, so callers that
     * know a region is empty (for example from changed-region tracking) should
     * declare it with {@code ofZeros}.
     *
     * <p>Each server ack is correlated back to its region by sequence number and
     * surfaced to {@code handler} as a {@link WriteAck} (success) or
     * {@link WriteError} (failure) keyed by the region's offset and length; the
     * sequence number never leaves the SDK. The call returns once every region
     * has reported.
     */
    @Override
    public void write(WriteRegionsRequest request, WriteDataProvider dataProvider, WriteHandler handler) {
        Objects.requireNonNull(dataProvider, "dataProvider is required");
        Objects.requireNonNull(handler, "handler is required");
        List<WriteRegion> regions = request.regions();

        // Bound in-flight (sent but unacked) requests to the server's per-stream
        // cap; the sender blocks here so extra regions queue instead of being
        // rejected. Fair so regions go out in submission order.
        Semaphore inFlight = new Semaphore(maxInFlightWriteRequests, true /* fair */);
        // Bound the aggregate unacked payload bytes as well, so peak memory is
        // capped independently of request count (otherwise the nominal window is
        // count * per-request-cap). The sender reserves a region's bytes before
        // pulling them and releases on ack, so large regions throttle by bytes.
        ByteBudget byteBudget = new ByteBudget(maxInFlightWriteBytes);
        // seq -> region: the sequence number is an SDK-internal correlation id;
        // the region's offset and length are the user-facing identity. Concurrent
        // because the sender thread inserts while the transport thread removes.
        Map<Long, WriteRegion> pending = new ConcurrentHashMap<>();
        // Completes ONLY on the real transport terminal (onError/onCompleted).
        // gRPC guarantees no onNext after a terminal callback, so the caller waits
        // on this before draining `pending`: by then the transport thread has
        // finished delivering acks and cannot race the drain into double-reporting
        // a region that onNext is concurrently acking. The early paths (unsequenced
        // rejection, sender catch) deliberately do NOT complete this — they only
        // record the failure reason and trigger a cancel/close, whose terminal
        // callback is what completes it here. Always completed normally; the reason
        // lives in failureReason, so join() never throws.
        CompletableFuture<Void> streamClosed = new CompletableFuture<>();
        // The whole-stream failure reason, recorded first-wins (compareAndSet from
        // null) by whichever path detects it first: a typed admission rejection in
        // onNext, a send failure, or the transport onError (a keepalive/deadline
        // teardown included).
        // First-wins keeps the most specific reason — e.g. an admission kServerBusy
        // over the generic CANCELLED that the ensuing cancel produces. Read by the
        // drain to fail every unacked region with the real cause; stays null when
        // the stream ends cleanly.
        AtomicReference<Throwable> failureReason = new AtomicReference<>();

        StreamObserver<VDiskWriteRet> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(VDiskWriteRet value) {
                WriteRegion region = pending.remove(value.getSequenceNumber());
                if (region == null) {
                    // No region owns this response. An unsequenced admission
                    // rejection ends the whole stream: record that typed reason
                    // (first-wins) so the drain reports it for every unacked region.
                    // The stream is still live here, so we do NOT complete
                    // streamClosed — the server's ensuing cancel delivers the real
                    // terminal onError, which completes it. Anything else is a stray
                    // or duplicate delivery and is only logged.
                    StreamFailure captured = streamFailureFor(value);
                    if (captured != null) {
                        failureReason.compareAndSet(null, captured);
                    } else {
                        LOG.warning("Ignoring VDiskWriteRet for unknown sequenceNumber="
                                + value.getSequenceNumber());
                    }
                    return;
                }
                // Free the request slot and the region's byte reservation first so
                // the sender can enqueue the next queued region regardless of what
                // the handler does. Handler calls are wrapped so a caller exception
                // cannot kill the stream or leak the slot.
                inFlight.release();
                byteBudget.release(budgetReservationFor(region));
                int errorCodeNumber = effectiveErrorCodeNumber(
                        value.getErrorCode(), value.getUnknownFields());
                if (errorCodeNumber == VDiskErrorCode.kNoError_VALUE) {
                    safeOnSuccess(handler, new WriteAck(region.offset(), region.length()));
                } else {
                    safeOnError(handler, new WriteError(region.offset(), region.length(),
                            toErrorCode(errorCodeNumber), value.getErrorMessage()));
                }
            }

            @Override
            public void onError(Throwable t) {
                LOG.warning("Write stream onError: " + t);
                // Record the transport reason (first-wins, so an earlier admission
                // reason captured in onNext is preserved), then signal the
                // terminal. Record and release before completing
                // streamClosed so a sender woken by the release sees the reason and
                // bails instead of sending onto a dead stream.
                failureReason.compareAndSet(null, t);
                inFlight.release(maxInFlightWriteRequests);
                byteBudget.abort();
                // Real terminal: no onNext can follow, so the drain may now run.
                streamClosed.complete(null);
            }

            @Override
            public void onCompleted() {
                inFlight.release(maxInFlightWriteRequests);
                byteBudget.abort();
                // Real terminal: no onNext can follow, so the drain may now run.
                // No failureReason recorded — a clean close leaves it null.
                streamClosed.complete(null);
            }
        };

        ClientCallStreamObserver<VDiskWriteArg> requestObserver =
                (ClientCallStreamObserver<VDiskWriteArg>) callStub().vDiskStreamWrite(responseObserver);

        // Index of the next region to process. Everything before it has been
        // accounted for (reported as a drop, or handed to `pending`); everything
        // from it onward is unsent. The finally uses it to fail the unsent tail so
        // every submitted region gets exactly one terminal callback.
        int sendIndex = 0;
        // Cause of a stream-level failure, read by the finally to report unacked
        // regions with the real reason; null if the stream ended cleanly.
        Throwable streamFailureCause = null;
        try {
            try {
                while (sendIndex < regions.size()) {
                    WriteRegion region = regions.get(sendIndex);
                    // Reject an oversized data region up front, before taking either
                    // gate or pulling any bytes. Its reservation is region.length(),
                    // which the byte budget can never admit (its capacity is
                    // >= maxWriteBytesPerRequest but a length beyond that would wait
                    // forever), so acquiring would deadlock until the RPC deadline
                    // aborts the whole stream. The server would reject it with
                    // kInvalidArgument anyway, so fail just this region here and keep
                    // the stream healthy for the rest.
                    if (!region.zeroData() && region.length() > maxWriteBytesPerRequest) {
                        safeOnError(handler, new WriteError(region.offset(), region.length(),
                                ErrorCode.INVALID_ARGUMENT,
                                "write region length " + region.length()
                                        + " exceeds max write bytes per request "
                                        + maxWriteBytesPerRequest));
                        sendIndex++;
                        continue;
                    }
                    // Take a request slot, then reserve the region's bytes, before
                    // pulling them, so no more than the in-flight window's worth of
                    // data (bounded by both count and bytes) is ever held — and a
                    // large region blocks here rather than after it is resident.
                    // Both acquires can block; if the stream is torn down while we
                    // wait, onError/onCompleted's release(max) and abort() wake us.
                    inFlight.acquireUninterruptibly();
                    long reservation = budgetReservationFor(region);
                    byteBudget.acquire(reservation);
                    // One post-acquire check covers both waits: if a failure reason
                    // was recorded (rejection or transport error) or the
                    // stream already reached its terminal while we were parked, stop
                    // and release what we just took. sendIndex still points at this
                    // region, so it and the rest of the list are failed as the unsent
                    // tail in the finally.
                    if (failureReason.get() != null || streamClosed.isDone()) {
                        byteBudget.release(reservation);
                        inFlight.release();
                        break;
                    }
                    // Zero-fill regions carry no bytes, so the provider is skipped.
                    // For data regions, pull the bytes now (one request's worth)
                    // and validate them before sending.
                    boolean zeroFill = region.zeroData();
                    ByteString data = null;
                    if (!zeroFill) {
                        // Pull just this region's range from the provider on the
                        // fly. A provider that throws fails only this region; the
                        // exception is contained so remaining regions still send.
                        try {
                            data = dataProvider.getData(region.offset(), region.length());
                        } catch (Throwable t) {
                            LOG.log(Level.WARNING,
                                    "WriteDataProvider.getData threw for offset="
                                            + region.offset() + " length=" + region.length(), t);
                            safeOnError(handler, new WriteError(region.offset(), region.length(),
                                    ErrorCode.INTERNAL_ERROR, "data provider failed: " + t));
                            byteBudget.release(reservation);
                            inFlight.release();
                            sendIndex++;
                            continue;
                        }
                        // The provider must return exactly region.length() bytes.
                        // region.length() was already validated against the
                        // per-request cap above, so a correctly-sized payload is
                        // within the cap; only a size mismatch (or null) is rejected.
                        if (data == null || data.size() != region.length()) {
                            safeOnError(handler, new WriteError(region.offset(), region.length(),
                                    ErrorCode.INVALID_ARGUMENT,
                                    "data provider returned "
                                            + (data == null ? "null" : data.size() + " bytes")
                                            + " for region of length " + region.length()));
                            byteBudget.release(reservation);
                            inFlight.release();
                            sendIndex++;
                            continue;
                        }
                    }
                    long sequenceNumber = sequenceCounter.getAndIncrement();
                    pending.put(sequenceNumber, region);
                    // The region is now owned by `pending`; advance before sending
                    // so that if onNext throws, this region is covered by the
                    // pending drain rather than re-reported as an unsent-tail region.
                    sendIndex++;
                    VDiskWriteArg writeArg = buildWriteArg(sequenceNumber, region, zeroFill, data);
                    requestObserver.onNext(writeArg);
                }
                // Half-close so the server can drain responses and cleanly finish
                // the stream, but only if we actually sent the whole list; if we
                // stopped early the stream is already terminating.
                if (sendIndex == regions.size()) {
                    requestObserver.onCompleted();
                }
            } catch (RuntimeException e) {
                // Ensure the stream is torn down if sending a request threw; record
                // the cause (first-wins) so the drain maps it to a typed reason for
                // unacked regions, and let the resulting terminal onError complete
                // streamClosed.
                failureReason.compareAndSet(null, e);
                requestObserver.onError(e);
            }

            // Block until the transport delivers its terminal callback before
            // draining. streamClosed is completed only by onError/onCompleted, so
            // when it returns the transport thread is finished delivering acks and
            // cannot race the drain into double-reporting a region. Every early path
            // (unsequenced rejection, send failure) triggers a cancel or half-close
            // whose terminal arrives here — and, failing that, channel keepalive or
            // the overall deadline tears the stream down — so this cannot hang
            // unless keepalive and the deadline are both disabled by config. It only
            // ever completes normally, so it never throws; the reason lives in
            // failureReason.
            streamClosed.join();
            streamFailureCause = failureReason.get();
            if (streamFailureCause != null) {
                LOG.warning("Write stream failed before completing: " + streamFailureCause);
            }
        } finally {
            // Guarantee exactly-once reporting for every submitted region:
            //  - regions handed to the server but never acked (stream closed early,
            //    errored, or the sender threw after enqueueing) live in `pending`;
            //  - regions we never got to send (stream closed while the sender was
            //    blocked, or a mid-list send failure) are the tail from sendIndex.
            // The two sets are disjoint, so no region is reported twice. The gates
            // are method-local with no waiters at this point, so they need no
            // release here; the ack path and abort() already handle all wakeups.
            //
            // Claim each pending region with remove() and act only on a non-null
            // result: onNext removes a region before it acks, so should a late ack
            // ever overlap this drain, exactly one of the two wins the removal and
            // the region gets a single terminal callback.
            for (Long sequenceNumber : pending.keySet()) {
                WriteRegion region = pending.remove(sequenceNumber);
                if (region == null) {
                    continue;
                }
                safeOnError(handler,
                        terminalWriteError(streamFailureCause, region, "No write response received"));
            }
            for (int i = sendIndex; i < regions.size(); i++) {
                safeOnError(handler, terminalWriteError(
                        streamFailureCause, regions.get(i), "Write stream closed before region was sent"));
            }
        }
    }

    /**
     * Gracefully closes the channel and force-closes on timeout/interruption.
     */
    @Override
    public void close() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }
}
