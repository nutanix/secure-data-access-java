/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Transport-agnostic contract for reading, writing, and closing a vdisk client.
 */
package com.nutanix.securedata.sdk.api;

/**
 * Client contract for reading from and writing to a vdisk.
 *
 * <p>Implementations manage the underlying connection. Obtain an instance via
 * {@link VDiskClients} or {@link com.nutanix.securedata.sdk.SecureDataAccess}.
 */
public interface VDiskClient extends AutoCloseable {
    /**
     * Reads each region on its own stream (the server allows only one read per
     * stream), keeping at most a configured number of streams in flight and
     * queuing the rest. Recovered data and per-range failures are delivered to
     * {@code handler} as they arrive; callbacks are not serialized and may run
     * concurrently, so the handler must be thread-safe. The call returns once
     * every region has completed.
     */
    void read(ReadRegionsRequest request, ReadHandler handler);

    /**
     * Writes each region as its own message, pipelined on a single stream,
     * keeping at most a configured number of requests in flight and queuing the
     * rest. Bytes for each data region are pulled from {@code dataProvider} at
     * send time, one range at a time. Per-region acknowledgements and failures
     * are delivered to {@code handler} as they arrive; callbacks may run
     * concurrently, so the handler must be thread-safe. The call returns once
     * every region has reported.
     */
    void write(WriteRegionsRequest request, WriteDataProvider dataProvider, WriteHandler handler);

    @Override
    void close();
}
