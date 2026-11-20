/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Entry point for the Secure Data Access SDK.
 */
package com.nutanix.securedata.sdk;

import com.nutanix.securedata.sdk.api.ReadHandler;
import com.nutanix.securedata.sdk.api.ReadRegion;
import com.nutanix.securedata.sdk.api.ReadRegionsRequest;
import com.nutanix.securedata.sdk.api.VDiskClient;
import com.nutanix.securedata.sdk.api.VDiskClientConfig;
import com.nutanix.securedata.sdk.api.VDiskClients;
import com.nutanix.securedata.sdk.api.WriteDataProvider;
import com.nutanix.securedata.sdk.api.WriteHandler;
import com.nutanix.securedata.sdk.api.WriteRegion;
import com.nutanix.securedata.sdk.api.WriteRegionsRequest;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Entry point for reading from and writing to secured vdisks.
 *
 * <p>Prefer this class for typical integrations. For advanced configuration,
 * use {@link VDiskClients} with a custom {@link VDiskClientConfig}.
 */
public final class SecureDataAccess {

    private SecureDataAccess() {
    }

    public static final class Client implements AutoCloseable {
        private final VDiskClient delegate;

        /**
         * Connects to a vdisk using a bearer token. The transport always uses TLS
         * with server-certificate verification skipped; authorization comes from
         * {@code token}, which also identifies the disk to read from and write to,
         * so the SDK holds no disk details of its own.
         *
         * <p>{@code timeoutSeconds} sets the keepalive ping interval used to detect
         * a stalled or dead connection, not a whole-operation deadline: a bulk read
         * or restore may run for as long as the connection stays alive, and the
         * overall transfer is left unbounded. For finer control (keepalive timeout
         * and an optional overall deadline) build a {@link VDiskClientConfig} and
         * use {@link VDiskClients} directly.
         */
        public Client(
                String host,
                int port,
                String token,
                long timeoutSeconds
        ) {
            this(host, port, token, timeoutSeconds,
                    VDiskClientConfig.DEFAULT_MAX_CONCURRENT_READS);
        }

        /**
         * Same as {@link #Client(String, int, String, long)} but also caps how
         * many read streams the SDK keeps in flight at once. Extra regions are
         * queued and started as earlier reads finish, so the cap bounds
         * concurrency without limiting the number of regions per call.
         */
        public Client(
                String host,
                int port,
                String token,
                long timeoutSeconds,
                int maxConcurrentReads
        ) {
            VDiskClientConfig config = VDiskClientConfig.builder()
                    .host(host)
                    .port(port)
                    .authToken(token)
                    .keepAliveTime(Duration.ofSeconds(timeoutSeconds))
                    .maxConcurrentReads(maxConcurrentReads)
                    .build();
            this.delegate = VDiskClients.create(config);
        }

        /**
         * Reads one or more, possibly non-contiguous, regions in one call.
         *
         * <p>This is the single read entry point: a plain contiguous read is just a
         * one-element list ({@code List.of(new ReadRegion(offset, length))}), while
         * an incremental-backup read passes the changed extents as multiple regions.
         * Each region is fetched on its own stream (the server allows only one read
         * per stream). The SDK keeps a bounded number of streams in flight and
         * queues the rest, so the region list can be arbitrarily long without
         * exceeding the server's concurrency cap.
         *
         * <p>Results stream through {@code handler}: {@link ReadHandler#onChunk}
         * receives each recovered chunk as it arrives and {@link ReadHandler#onError}
         * receives each failed range. Callbacks are not serialized and may run
         * concurrently on independent threads, so the handler must be thread-safe.
         * Because chunks and errors carry their absolute vdisk offset, callers can
         * persist data directly and retry exactly the ranges that failed, without
         * buffering, reordering, or an aggregate return value.
         */
        public void readRegions(List<ReadRegion> regions, ReadHandler handler) {
            if (regions == null || regions.isEmpty()) {
                throw new IllegalArgumentException("regions must be provided");
            }
            Objects.requireNonNull(handler, "handler is required");

            delegate.read(new ReadRegionsRequest(regions), handler);
        }

        /**
         * Writes one or more, possibly non-contiguous, regions in one call.
         *
         * <p>This is the single write entry point: a plain contiguous write is just a
         * one-element list ({@code List.of(WriteRegion.ofData(offset, length))}),
         * while incremental-backup restore passes the changed extents (real data or
         * zero-fill holes) as multiple regions. Each region maps to one request,
         * pipelined on a single stream and acknowledged independently. The SDK keeps
         * a bounded number of requests in flight and queues the rest, so the region
         * list can be arbitrarily long without exceeding the server's per-stream cap.
         *
         * <p>A region carries no bytes: it only says where and how much to write. The
         * bytes for data regions are pulled from {@code dataProvider} on the fly at
         * send time (one range at a time), so the caller never holds the whole
         * payload in memory. Because a region maps to a single request, callers
         * should size each region at or below the configured per-request cap
         * ({@code maxWriteBytesPerRequest}); sizing regions that way keeps peak memory
         * bounded to about {@code maxInFlightWriteRequests} times that cap. A region
         * whose data exceeds the cap is not sent and is reported via
         * {@link WriteHandler#onError}. Zero-fill regions send no bytes and never
         * consult the provider; the SDK does not scan data regions for zeros, so a
         * caller that knows a region is empty (for example from changed-region
         * tracking) should declare it a hole with {@code WriteRegion.ofZeros}.
         *
         * <p>Results stream through {@code handler}: {@link WriteHandler#onSuccess}
         * receives each acknowledged region and {@link WriteHandler#onError} receives
         * each failed region, both keyed by absolute offset and length so a caller can
         * retry exactly what failed. Callbacks are not serialized and may run
         * concurrently, so the handler must be thread-safe. The call returns once
         * every region has reported.
         *
         * <p><strong>Single writer per vDisk.</strong> The server allows only one
         * active write stream per vDisk and rejects a second with
         * {@link com.nutanix.securedata.sdk.api.ErrorCode#CONCURRENT_REQUEST_NOT_SUPPORTED}
         * followed by a transport cancel. The bearer token fixes the target disk, so
         * two overlapping {@code writeRegions} calls — on this client or on another
         * client sharing the token — always collide. Serialize writes to a disk (do
         * not run them concurrently) and put what would be concurrent writes into a
         * single call's region list instead.
         */
        public void writeRegions(
                List<WriteRegion> regions,
                WriteDataProvider dataProvider,
                WriteHandler handler) {
            if (regions == null || regions.isEmpty()) {
                throw new IllegalArgumentException("regions must be provided");
            }
            Objects.requireNonNull(dataProvider, "dataProvider is required");
            Objects.requireNonNull(handler, "handler is required");

            delegate.write(new WriteRegionsRequest(regions), dataProvider, handler);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
