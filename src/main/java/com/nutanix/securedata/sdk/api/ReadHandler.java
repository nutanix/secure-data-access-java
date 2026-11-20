/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Callback sink for streamed recovered chunks and per-range read errors.
 */
package com.nutanix.securedata.sdk.api;

/**
 * Sink for the results of a multi-region read. A {@code ReadHandler} exposes
 * <strong>exactly two callbacks</strong> and no more: {@link #onChunk} for
 * recovered data and {@link #onError} for failures. This is the entire contract
 * a caller implements to consume a read; there are no other hooks, no lifecycle
 * methods, and no return values.
 *
 * <h2>How a read is delivered</h2>
 * <p>Reads stream. Instead of returning one large buffer, the SDK invokes
 * {@link #onChunk(RecoveredChunk)} as each chunk arrives and
 * {@link #onError(ReadError)} as each range fails.
 * The read call returns only after every region has completed,
 * so once it returns no further callbacks will be fired for that request.
 *
 * <h2>Threading</h2>
 * <p>Regions are fetched concurrently on independent streams, 
 * serialize callbacks: {@link #onChunk} and {@link #onError}
 * may be invoked <strong>concurrently on multiple threads</strong>
 * Keep each callback short and non-blocking as much as possible,
 * since a callback runs on an SDK transport thread.
 *
 * <h2>What each callback owns</h2>
 * <p>Every {@link RecoveredChunk} and {@link ReadError} is self-locating via its
 * absolute vdisk offset, so a handler can persist data or schedule retries
 * directly without buffering or reordering. See {@link ReadError#errorCode()}
 * for driving a retry workflow.
 *
 * <h2>If a callback throws</h2>
 * <p>Exceptions thrown from {@link #onChunk} or {@link #onError} are the SDK's to
 * contain, not the caller's to catch: the SDK logs and suppresses them per
 * callback. They do <strong>not</strong> propagate out of the {@code read} call,
 * do not stop other chunks or regions, and never leak the internal stream/permit
 * bookkeeping. A suppressed {@link #onChunk} exception simply means that one
 * chunk may go unpersisted; the read otherwise proceeds. Do not use exceptions
 * for control flow; signal problems through your own state instead.
 */
public interface ReadHandler {
    /**
     * Invoked once per recovered chunk. The chunk is self-locating via its
     * absolute {@link RecoveredChunk#offset()}; for a {@code zeroData} chunk the
     * data is empty and {@link RecoveredChunk#length()} tells how many zeros to
     * materialize.
     *
     * @param chunk the recovered data; never {@code null}
     */
    void onChunk(RecoveredChunk chunk);

    /**
     * Invoked once per failed range. The failing {@code [offset, length)} is
     * reported (see {@link ReadError}) so the caller can retry exactly that
     * range, optionally keyed off {@link ReadError#errorCode()}.
     *
     * @param error the failed range and its reason; never {@code null}
     */
    void onError(ReadError error);
}
