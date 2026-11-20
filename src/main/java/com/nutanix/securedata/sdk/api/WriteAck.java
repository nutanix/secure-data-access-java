/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Acknowledgement that a single written region was accepted by the server.
 */
package com.nutanix.securedata.sdk.api;

/**
 * Acknowledgement that a single region was written, delivered to
 * {@link WriteHandler#onSuccess(WriteAck)} when the server confirms it.
 *
 * <p>The region is identified by its absolute {@link #offset()} and
 * {@link #length()} — the same coordinates the caller submitted — not by any
 * SDK-internal correlation id.
 *
 * @param offset absolute vdisk offset that was written
 * @param length number of bytes written at {@code offset}
 */
public record WriteAck(long offset, long length) {
}
