/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Caller-supplied source that yields a data region's bytes on demand.
 */
package com.nutanix.securedata.sdk.api;

import com.google.protobuf.ByteString;

/**
 * Random-access source of the bytes to write. The SDK asks the provider for one
 * region's range at a time, on the fly, at the moment it is about to put that
 * region on the wire — much like reading a range from a
 * {@link java.nio.channels.SeekableByteChannel}. A single provider serves every
 * region in a write, so it holds no per-region state.
 *
 * <p>This is what lets a write avoid holding the whole payload in memory: the
 * SDK only pulls a region's bytes when a send slot frees up, so at most one
 * request's worth of data per in-flight slot is materialized at once. Zero-fill
 * regions carry no bytes and are never requested from the provider.
 *
 * <p>{@link #getData(long, int)} must return a {@link ByteString} of exactly
 * {@code length} bytes for the range {@code [offset, offset + length)}; a
 * {@code null} or wrong-sized value, or one larger than the configured
 * per-request cap, is reported to the write handler as a failed region. The
 * {@code ByteString} is placed on the wire directly, without an extra copy, so a
 * provider backed by a file or channel can return a zero-copy view (for example
 * {@link ByteString#copyFrom(java.nio.ByteBuffer)} over a mapped region, or
 * {@code com.google.protobuf.UnsafeByteOperations.unsafeWrap} over a buffer it
 * does not mutate afterwards). The provider is consulted only for data regions;
 * declare empty regions as holes with {@link WriteRegion#ofZeros} so they are not
 * pulled or sent at all.
 *
 * <p>May be invoked from an SDK thread; within a single write it is called once
 * per data region.
 *
 * <p>If {@link #getData(long, int)} throws, the SDK contains the exception: it
 * logs it, reports that one region as a failed write via
 * {@link WriteHandler#onError} (with {@link ErrorCode#INTERNAL_ERROR}), and
 * continues sending the remaining regions. The exception does not propagate out
 * of the {@code write} call.
 */
@FunctionalInterface
public interface WriteDataProvider {
    /**
     * Returns the bytes for the range {@code [offset, offset + length)}.
     *
     * @param offset absolute vdisk offset of the range being written
     * @param length number of bytes to return; the result must be exactly this long
     * @return exactly {@code length} bytes for the range
     */
    ByteString getData(long offset, int length);
}
