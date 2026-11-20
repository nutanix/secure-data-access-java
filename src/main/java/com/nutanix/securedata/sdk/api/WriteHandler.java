/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Callback sink for per-region write acknowledgements and errors.
 */
package com.nutanix.securedata.sdk.api;

/**
 * Sink for the results of a multi-region write. A {@code WriteHandler} exposes
 * <strong>exactly two callbacks</strong> and no more: {@link #onSuccess} for
 * regions the server acknowledged and {@link #onError} for regions that failed.
 * This is the entire contract a caller implements to observe a write; there are
 * no other hooks and no aggregate return value.
 *
 * <h2>How a write is delivered</h2>
 * <p>Writes pipeline: regions are streamed to the server and acknowledged
 * independently, so a single write may invoke {@link #onSuccess} and
 * {@link #onError} any number of times in any interleaving. Each region reports
 * exactly once — as a success or an error, never both. The write call returns
 * only after every region has reported, so once it returns no further callbacks
 * will fire.
 *
 * <h2>Threading</h2>
 * <p>Callbacks are not serialized by the SDK and may be invoked concurrently on
 * multiple threads (server acknowledgements arrive on a transport thread, while
 * a region rejected before it is sent is reported on the calling thread).
 * <strong>Implementations must be thread-safe</strong>; guard any shared mutable
 * state with your own synchronization, atomics, or concurrent collections, and
 * keep callbacks short since they run on SDK threads.
 *
 * <h2>What each callback owns</h2>
 * <p>Both {@link WriteAck} and {@link WriteError} identify their region by
 * absolute offset and length — the coordinates the caller submitted — so a
 * handler can track progress or retry failed regions directly. See
 * {@link WriteError#errorCode()} for driving a retry workflow.
 *
 * <h2>If a callback throws</h2>
 * <p>Exceptions thrown from {@link #onSuccess} or {@link #onError} are the SDK's
 * to contain, not the caller's to catch: the SDK logs and suppresses them. They
 * do <strong>not</strong> propagate out of the {@code write} call, do not cancel
 * the shared write stream, and do not stop the remaining regions from being
 * reported. A callback that throws is treated as having observed that region;
 * the region is not retried by the SDK. Do not use exceptions for control flow;
 * signal problems through your own state instead.
 *
 * <p><strong>Contract note for implementers:</strong> this interface is
 * intentionally limited to these two methods; do not rely on any additional
 * methods being added.
 */
public interface WriteHandler {
    /**
     * Invoked once per region the server acknowledged as written.
     *
     * @param ack the written region's coordinates; never {@code null}
     */
    void onSuccess(WriteAck ack);

    /**
     * Invoked once per region that failed, with the failing region's coordinates
     * and reason (see {@link WriteError}) so the caller can retry exactly it.
     *
     * @param error the failed region and its reason; never {@code null}
     */
    void onError(WriteError error);
}
