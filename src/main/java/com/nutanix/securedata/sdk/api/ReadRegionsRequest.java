/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * A request to read one or more, possibly non-contiguous, vdisk regions.
 */
package com.nutanix.securedata.sdk.api;

import java.util.List;
import java.util.Objects;

/**
 * A request to read one or more, possibly non-contiguous, vdisk regions.
 *
 * <p>Every {@link ReadRegion} is fetched on its own read stream (the server
 * supports only one read per stream). The disk to read from is determined by
 * the bearer token on the connection, so the request carries only the regions.
 * Chunks are delivered to the caller as they arrive.
 */
public record ReadRegionsRequest(List<ReadRegion> regions) {
    public ReadRegionsRequest {
        Objects.requireNonNull(regions, "regions is required");
        if (regions.isEmpty()) {
            throw new IllegalArgumentException("regions must not be empty");
        }
        regions = List.copyOf(regions);
    }
}
