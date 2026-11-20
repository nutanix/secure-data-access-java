/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Immutable connection and tuning configuration for a vdisk client.
 */
package com.nutanix.securedata.sdk.api;

import java.time.Duration;
import java.util.Objects;

/**
 * Connection settings for a vdisk client.
 *
 * <p>The transport always uses TLS with server-certificate verification
 * skipped: authorization is established by the bearer {@code authToken}, not by
 * the server certificate, so no CA material or hostname override is needed.
 */
public final class VDiskClientConfig {
    /**
     * Default cap on read streams the SDK keeps in flight at once.
     *
     * <p>The server's {@code stargate_vdisk_grpc_server_max_active_read_streams}
     * limit (default 64) is <em>process-wide and shared</em> across every client,
     * token, and vDisk — it is not a per-client, per-token, or per-vDisk budget. A
     * stream opened past that limit is refused with {@code kServerBusy} and
     * cancelled. This default is therefore set well below the server limit so a
     * single client does not consume the whole instance's read-stream capacity and
     * starve other concurrent backup or restore jobs. Raise it only when the
     * client is known to have the server largely to itself.
     */
    public static final int DEFAULT_MAX_CONCURRENT_READS = 8;

    /**
     * Default cap on the payload bytes of a single write request: 30 MiB.
     * The server rejects a larger request with {@code kInvalidArgument}; the SDK
     * rejects such a region locally with {@link ErrorCode#INVALID_ARGUMENT}
     * before sending. This is a client tunable and is not read from the server;
     * raise it only when the server flag is raised to match.
     */
    public static final long DEFAULT_MAX_WRITE_BYTES_PER_REQUEST = 30L * 1024 * 1024;

    /**
     * Default cap on write requests kept in flight (sent but not yet acked) on a
     * write stream: 20.
     * Additional regions are queued and sent as earlier ones are acked. The
     * server rejects requests beyond its flag with {@code kServerBusy}, which
     * the SDK does not retry. This is a client tunable and is not read from the
     * server; raise it only when the server flag is raised to match.
     */
    public static final int DEFAULT_MAX_IN_FLIGHT_WRITE_REQUESTS = 20;

    /**
     * Default cap on the aggregate payload bytes kept in flight (sent but not yet
     * acked) on a write stream: 30 MiB.
     * The server pauses reads on the stream at 75% of that flag. This bounds
     * peak client memory independently of
     * {@link #DEFAULT_MAX_IN_FLIGHT_WRITE_REQUESTS}: without it, the nominal
     * window would be request-count times per-request-cap (with these defaults,
     * {@code 20 * 30 MiB = 600 MiB}). The sender blocks until the budget has
     * room, so large regions throttle by bytes rather than only by count. This
     * is a client tunable and is not read from the server.
     */
    public static final long DEFAULT_MAX_IN_FLIGHT_WRITE_BYTES = 30L * 1024 * 1024;

    /**
     * Default keepalive ping interval: how often the transport sends an HTTP/2
     * keepalive ping to the server while a read or write call is active. Combined
     * with {@link #DEFAULT_KEEPALIVE_TIMEOUT} this is how a stalled or dead
     * connection is detected without a whole-call deadline that would kill a large
     * transfer. The server streams reads up to 1 TiB with no deadline of its own,
     * so connection liveness — not a wall-clock budget — is what bounds a hung
     * transfer.
     */
    public static final Duration DEFAULT_KEEPALIVE_TIME = Duration.ofSeconds(30);

    /**
     * Default keepalive ping timeout: how long the transport waits for a ping ack
     * before treating the connection as dead and failing the call with
     * {@code UNAVAILABLE}. Kept below {@link #DEFAULT_KEEPALIVE_TIME} so a stall is
     * detected within roughly one ping interval plus this timeout.
     */
    public static final Duration DEFAULT_KEEPALIVE_TIMEOUT = Duration.ofSeconds(20);

    /**
     * Default overall transfer deadline: {@link Duration#ZERO}, meaning
     * <em>unbounded</em>. The server accepts reads up to 1 TiB, so no single
     * wall-clock budget fits every transfer; liveness is enforced by keepalive
     * ({@link #DEFAULT_KEEPALIVE_TIME} / {@link #DEFAULT_KEEPALIVE_TIMEOUT})
     * instead. Set a positive value only to impose a hard cap on the whole
     * operation.
     */
    public static final Duration DEFAULT_OVERALL_DEADLINE = Duration.ZERO;

    private final String host;
    private final int port;
    private final Duration keepAliveTime;
    private final Duration keepAliveTimeout;
    private final Duration overallDeadline;
    private final String authToken;
    private final int maxConcurrentReads;
    private final long maxWriteBytesPerRequest;
    private final int maxInFlightWriteRequests;
    private final long maxInFlightWriteBytes;

    private VDiskClientConfig(Builder builder) {
        this.host = Objects.requireNonNull(builder.host, "host is required");
        this.port = builder.port;
        this.keepAliveTime = requireNonNegative(builder.keepAliveTime, "keepAliveTime");
        this.keepAliveTimeout = requireNonNegative(builder.keepAliveTimeout, "keepAliveTimeout");
        this.overallDeadline = requireNonNegative(builder.overallDeadline, "overallDeadline");
        this.authToken = builder.authToken;
        if (builder.maxConcurrentReads <= 0) {
            throw new IllegalArgumentException("maxConcurrentReads must be > 0");
        }
        this.maxConcurrentReads = builder.maxConcurrentReads;
        if (builder.maxWriteBytesPerRequest <= 0) {
            throw new IllegalArgumentException("maxWriteBytesPerRequest must be > 0");
        }
        this.maxWriteBytesPerRequest = builder.maxWriteBytesPerRequest;
        if (builder.maxInFlightWriteRequests <= 0) {
            throw new IllegalArgumentException("maxInFlightWriteRequests must be > 0");
        }
        this.maxInFlightWriteRequests = builder.maxInFlightWriteRequests;
        if (builder.maxInFlightWriteBytes <= 0) {
            throw new IllegalArgumentException("maxInFlightWriteBytes must be > 0");
        }
        // A single region reserves up to maxWriteBytesPerRequest from the budget,
        // so the budget must be able to admit at least one max-size region or that
        // region could never acquire and the write would stall forever.
        if (builder.maxInFlightWriteBytes < builder.maxWriteBytesPerRequest) {
            throw new IllegalArgumentException(
                    "maxInFlightWriteBytes (" + builder.maxInFlightWriteBytes
                            + ") must be >= maxWriteBytesPerRequest ("
                            + builder.maxWriteBytesPerRequest + ")");
        }
        this.maxInFlightWriteBytes = builder.maxInFlightWriteBytes;
    }

    private static Duration requireNonNegative(Duration value, String name) {
        Objects.requireNonNull(value, name + " is required");
        if (value.isNegative()) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return value;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    /**
     * Keepalive ping interval: how often the transport pings the server while a
     * call is active to detect a stalled or dead connection.
     * {@link Duration#ZERO} disables keepalive.
     */
    public Duration keepAliveTime() {
        return keepAliveTime;
    }

    /**
     * Keepalive ping timeout: how long to wait for a ping ack before treating the
     * connection as dead and failing the call. Only meaningful when
     * {@link #keepAliveTime()} is non-zero.
     */
    public Duration keepAliveTimeout() {
        return keepAliveTimeout;
    }

    /**
     * Optional overall transfer deadline applied as the gRPC call deadline.
     * {@link Duration#ZERO} (the default) means unbounded, so the transfer is
     * governed only by keepalive liveness ({@link #keepAliveTime()} /
     * {@link #keepAliveTimeout()}).
     */
    public Duration overallDeadline() {
        return overallDeadline;
    }

    /** Bearer token sent as the "authorization" metadata header. */
    public String authToken() {
        return authToken;
    }

    /**
     * Maximum number of read streams the SDK keeps in flight concurrently. Any
     * additional regions are queued and started as in-flight reads complete, so a
     * read call accepts an unbounded region list.
     *
     * <p>The server's active-read-stream limit is process-wide and shared across
     * all clients, so this defaults to a conservative
     * {@value #DEFAULT_MAX_CONCURRENT_READS} to leave capacity for other jobs
     * rather than sizing against that shared server cap. A stream the server
     * refuses past its limit surfaces as {@link ErrorCode#SERVER_BUSY}, a back-off
     * signal to retry the whole read later.
     */
    public int maxConcurrentReads() {
        return maxConcurrentReads;
    }

    /**
     * Maximum payload bytes allowed in a single write request. A region whose
     * data exceeds this cannot be split, so the SDK rejects such a write before
     * sending. Defaults to what the server accepts per request.
     */
    public long maxWriteBytesPerRequest() {
        return maxWriteBytesPerRequest;
    }

    /**
     * Maximum write requests the SDK keeps in flight (sent but not yet
     * acknowledged) on a write stream. Additional regions are queued and sent as
     * earlier ones are acknowledged, so a write accepts an unbounded region list
     * without exceeding the server's per-stream cap.
     */
    public int maxInFlightWriteRequests() {
        return maxInFlightWriteRequests;
    }

    /**
     * Maximum aggregate payload bytes the SDK keeps in flight (sent but not yet
     * acknowledged) on a write stream. The sender blocks until the budget has
     * room before pulling and sending the next region's bytes, so peak client
     * memory is bounded by this budget regardless of how large individual regions
     * are or how many requests the count cap would otherwise allow. Must be
     * &gt;= {@link #maxWriteBytesPerRequest()}.
     */
    public long maxInFlightWriteBytes() {
        return maxInFlightWriteBytes;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String host;
        private int port = 14231;
        private Duration keepAliveTime = DEFAULT_KEEPALIVE_TIME;
        private Duration keepAliveTimeout = DEFAULT_KEEPALIVE_TIMEOUT;
        private Duration overallDeadline = DEFAULT_OVERALL_DEADLINE;
        private String authToken;
        private int maxConcurrentReads = DEFAULT_MAX_CONCURRENT_READS;
        private long maxWriteBytesPerRequest = DEFAULT_MAX_WRITE_BYTES_PER_REQUEST;
        private int maxInFlightWriteRequests = DEFAULT_MAX_IN_FLIGHT_WRITE_REQUESTS;
        private long maxInFlightWriteBytes = DEFAULT_MAX_IN_FLIGHT_WRITE_BYTES;

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Sets the keepalive ping interval (how often to ping the server while a
         * call is active). {@link Duration#ZERO} disables keepalive. Must not be
         * negative.
         */
        public Builder keepAliveTime(Duration keepAliveTime) {
            this.keepAliveTime = keepAliveTime;
            return this;
        }

        /**
         * Sets the keepalive ping timeout (how long to wait for a ping ack before
         * failing the connection). Must not be negative.
         */
        public Builder keepAliveTimeout(Duration keepAliveTimeout) {
            this.keepAliveTimeout = keepAliveTimeout;
            return this;
        }

        /**
         * Sets the optional overall transfer deadline (gRPC call deadline).
         * {@link Duration#ZERO} (the default) means unbounded. Must not be
         * negative.
         */
        public Builder overallDeadline(Duration overallDeadline) {
            this.overallDeadline = overallDeadline;
            return this;
        }

        public Builder authToken(String authToken) {
            this.authToken = authToken;
            return this;
        }

        /** Sets the cap on concurrently in-flight read streams (must be &gt; 0). */
        public Builder maxConcurrentReads(int maxConcurrentReads) {
            this.maxConcurrentReads = maxConcurrentReads;
            return this;
        }

        /** Sets the cap on payload bytes in a single write request (must be &gt; 0). */
        public Builder maxWriteBytesPerRequest(long maxWriteBytesPerRequest) {
            this.maxWriteBytesPerRequest = maxWriteBytesPerRequest;
            return this;
        }

        /** Sets the cap on in-flight (unacked) write requests per stream (must be &gt; 0). */
        public Builder maxInFlightWriteRequests(int maxInFlightWriteRequests) {
            this.maxInFlightWriteRequests = maxInFlightWriteRequests;
            return this;
        }

        /**
         * Sets the cap on aggregate in-flight (unacked) write payload bytes (must
         * be &gt; 0 and &gt;= {@code maxWriteBytesPerRequest}).
         */
        public Builder maxInFlightWriteBytes(long maxInFlightWriteBytes) {
            this.maxInFlightWriteBytes = maxInFlightWriteBytes;
            return this;
        }

        public VDiskClientConfig build() {
            return new VDiskClientConfig(this);
        }
    }
}
