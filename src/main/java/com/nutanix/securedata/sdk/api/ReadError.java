/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * A read failure for a specific vdisk range, with a typed error code.
 */
package com.nutanix.securedata.sdk.api;

/**
 * A read failure for a specific vdisk range, delivered to
 * {@link ReadHandler#onError(ReadError)} as soon as the server reports it.
 *
 * <p>The failure is self-locating like a recovered chunk: {@link #offset()} and
 * {@link #length()} describe the exact byte range the server failed to read, so
 * a caller can retry precisely that range. When the server reports the failing
 * range it is used verbatim; otherwise (for setup-time failures that carry no
 * range) the bounds of the originating region are reported instead.
 *
 * <p>{@link #errorCode()} is a typed reason (see {@link ErrorCode}) so a
 * caller can drive its own retry workflow off it.
 *
 * @param offset    absolute vdisk offset of the range that failed
 * @param length    length in bytes of the range that failed
 * @param errorCode typed reason the range failed
 * @param message   human-readable failure detail
 */
public record ReadError(long offset, long length, ErrorCode errorCode, String message) {
}
