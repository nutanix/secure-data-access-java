/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * One piece of recovered read data (or a zero-fill hole) at an absolute offset.
 */
package com.nutanix.securedata.sdk.api;

import com.google.protobuf.ByteString;

/**
 * One piece of data recovered from a read, delivered to
 * {@link ReadHandler#onChunk(RecoveredChunk)} as soon as it arrives.
 *
 * <p>The chunk is self-locating: {@link #offset()} is the absolute vdisk offset,
 * so a caller can persist it (for example {@code pwrite(fd, data, length, offset)})
 * without buffering or reordering, even though chunks may arrive out of order.
 * All regions in a single read belong to the same vdisk, so the absolute offset
 * is sufficient to place every chunk; no region index is needed.
 *
 * <p>The payload is a {@link ByteString}, which is an immutable, read-only view
 * over the bytes as they arrived off the wire: the SDK hands it through without
 * copying it into a fresh {@code byte[]}. A caller should consume it in place —
 * for example {@link ByteString#asReadOnlyByteBuffer()} to write it straight to a
 * channel, or {@link ByteString#writeTo(java.io.OutputStream)} — rather than
 * calling {@code toByteArray()}, which would reintroduce a full copy.
 *
 * @param offset   absolute vdisk offset of this chunk
 * @param length   chunk length in bytes
 * @param data     recovered bytes; empty when {@code zeroData} is true
 * @param zeroData true when this chunk is a hole; materialize {@code length} zeros
 */
public record RecoveredChunk(
        long offset,
        long length,
        ByteString data,
        boolean zeroData) {
}
