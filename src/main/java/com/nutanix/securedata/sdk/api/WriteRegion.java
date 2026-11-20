/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * A single region to write: a data extent or a declared zero-fill hole.
 */
package com.nutanix.securedata.sdk.api;

/**
 * A single region to write during a sparse/incremental write.
 *
 * <p>A region is a pure destination descriptor: it says only <em>where</em>
 * ({@code offset}) and <em>how much</em> ({@code length}) to write, and whether
 * it is a zero-fill hole. It carries no data bytes. The bytes for data regions
 * are pulled on the fly from the {@link WriteDataProvider} passed alongside the
 * regions, so a caller never has to hold the whole payload in memory. Use
 * {@link #ofData(long, int)} for real data (the provider supplies the bytes) or
 * {@link #ofZeros(long, int)} for a zero-filled hole (no bytes on the wire, the
 * provider is not consulted). The SDK does not scan data regions for zeros: a
 * caller that knows a region is empty — for example from changed-region tracking,
 * whose API already classifies each region as zero or data — must declare it a
 * hole with {@link #ofZeros(long, int)}. A data region's bytes are always sent as
 * given, even if they happen to be all zeros.
 *
 * <p>A region maps one-to-one to a single write request, so its data must fit in
 * one request: callers should keep {@code length} at or below the configured
 * per-request cap ({@code VDiskClientConfig.maxWriteBytesPerRequest}). A region
 * whose data exceeds the cap is reported as a failed write rather than split.
 *
 * <p>{@code length} is an {@code int} because that single wire range's proto
 * {@code DiskDataRange.length} is int32. Callers writing more than
 * {@link Integer#MAX_VALUE} bytes must split the work into multiple regions.
 *
 * @param offset   absolute vdisk offset to write at
 * @param length   number of bytes to write; for data regions this is exactly how
 *                 many bytes the {@link WriteDataProvider} must return
 * @param zeroData true for a zero-fill hole (no bytes sent), false for real data
 */
public record WriteRegion(long offset, int length, boolean zeroData) {
    public WriteRegion {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        // The server maps each region to one wire range and rejects any range
        // with length <= 0, so an empty region is never valid.
        if (length <= 0) {
            throw new IllegalArgumentException("length must be > 0");
        }
    }

    /**
     * Region of {@code length} real bytes at {@code offset}; the bytes are pulled
     * on the fly from the write's {@link WriteDataProvider} at send time.
     */
    public static WriteRegion ofData(long offset, int length) {
        return new WriteRegion(offset, length, false);
    }

    /** Region that zero-fills {@code length} bytes at {@code offset} without sending zeros. */
    public static WriteRegion ofZeros(long offset, int length) {
        return new WriteRegion(offset, length, true);
    }
}
