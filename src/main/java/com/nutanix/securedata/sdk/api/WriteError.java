/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * A write failure for a specific region, with a typed error code.
 */
package com.nutanix.securedata.sdk.api;

/**
 * A write failure for a specific region, delivered to
 * {@link WriteHandler#onError(WriteError)} when the server rejects it or the SDK
 * cannot send it.
 *
 * <p>The failure is self-locating: {@link #offset()} and {@link #length()}
 * describe the exact region that failed — the coordinates the caller submitted —
 * so a caller can retry precisely that region. {@link #errorCode()} is a typed
 * reason (see {@link ErrorCode}) for driving a retry workflow.
 *
 * @param offset    absolute vdisk offset of the region that failed
 * @param length    length in bytes of the region that failed
 * @param errorCode typed reason the region failed
 * @param message   human-readable failure detail
 */
public record WriteError(long offset, long length, ErrorCode errorCode, String message) {
}
