/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Factory that creates a VDiskClient from configuration (the transport seam).
 */
package com.nutanix.securedata.sdk.api;

import com.nutanix.securedata.sdk.internal.GrpcVDiskClient;

/**
 * Factory for creating {@link VDiskClient} instances from configuration.
 */
public final class VDiskClients {
    private VDiskClients() {
    }

    /**
     * Creates a client using the supplied configuration.
     *
     * @param config connection and runtime settings; must not be {@code null}
     * @return a ready-to-use {@link VDiskClient}
     */
    public static VDiskClient create(VDiskClientConfig config) {
        return new GrpcVDiskClient(config);
    }
}
