/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * A write request of one or more regions (one active writer per vdisk).
 */
package com.nutanix.securedata.sdk.api;

import java.util.List;
import java.util.Objects;

/**
 * A write message carrying one or more regions.
 *
 * <p>Each {@link WriteRegion} maps one-to-one to a proto {@code DiskDataRange}.
 * The disk to write to is determined by the bearer token on the connection, so
 * the request carries only the regions.
 *
 * <p><strong>Single writer per vDisk.</strong> The server permits at most one
 * active write stream per vDisk and rejects any second concurrent stream with
 * {@link ErrorCode#CONCURRENT_REQUEST_NOT_SUPPORTED} followed by a transport
 * cancel. Because the bearer token fixes the target disk, two overlapping writes
 * to the same disk always collide — whether they come from two concurrent
 * {@code writeRegions} calls on one client or from two clients sharing a token.
 * Serialize writes to a given disk; batch what would otherwise be concurrent
 * writes into a single request's region list instead.
 */
public record WriteRegionsRequest(List<WriteRegion> regions) {
    public WriteRegionsRequest {
        Objects.requireNonNull(regions, "regions is required");
        if (regions.isEmpty()) {
            throw new IllegalArgumentException("regions must not be empty");
        }
        regions = List.copyOf(regions);
    }
}
