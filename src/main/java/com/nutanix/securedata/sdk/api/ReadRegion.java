/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * A single vdisk region to read, given by absolute offset and length.
 */
package com.nutanix.securedata.sdk.api;

/**
 * A single vdisk region the caller wants to read.
 *
 * <p>Each region maps one-to-one to its own read stream on the wire (the server
 * accepts only one read per stream), so a multi-region read fans out to one
 * stream per region.
 *
 * @param offset absolute vdisk offset to start reading from
 * @param length number of bytes to read from {@code offset}
 */
public record ReadRegion(long offset, long length) {
    public ReadRegion {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        if (length <= 0) {
            throw new IllegalArgumentException("length must be > 0");
        }
    }
}
