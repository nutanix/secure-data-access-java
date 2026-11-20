/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Unit tests for request and client-configuration validation.
 */
package com.nutanix.securedata.sdk.api;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RequestAndConfigValidationTest {

    @Test
    void rejectsNegativeReadOffset() {
        assertThrows(IllegalArgumentException.class, () -> new ReadRegion(-1, 128));
    }

    @Test
    void rejectsNonPositiveReadLength() {
        assertThrows(IllegalArgumentException.class, () -> new ReadRegion(0, 0));
    }

    @Test
    void rejectsNegativeWriteOffset() {
        assertThrows(IllegalArgumentException.class, () -> WriteRegion.ofData(-1, 128));
    }

    @Test
    void rejectsNonPositiveWriteLength() {
        assertThrows(IllegalArgumentException.class, () -> WriteRegion.ofData(0, 0));
    }

    @Test
    void requiresHost() {
        assertThrows(
                NullPointerException.class,
                () -> VDiskClientConfig.builder()
                        .port(14231)
                        .keepAliveTime(Duration.ofSeconds(30))
                        .build()
        );
    }

    @Test
    void buildsConfigWithExpectedFields() {
        VDiskClientConfig config = VDiskClientConfig.builder()
                .host("127.0.0.1")
                .port(14231)
                .keepAliveTime(Duration.ofSeconds(15))
                .keepAliveTimeout(Duration.ofSeconds(10))
                .overallDeadline(Duration.ofMinutes(30))
                .authToken("token")
                .build();

        assertEquals("127.0.0.1", config.host());
        assertEquals(14231, config.port());
        assertEquals(Duration.ofSeconds(15), config.keepAliveTime());
        assertEquals(Duration.ofSeconds(10), config.keepAliveTimeout());
        assertEquals(Duration.ofMinutes(30), config.overallDeadline());
        assertEquals("token", config.authToken());
    }

    @Test
    void defaultsAreBulkFriendly() {
        VDiskClientConfig config = VDiskClientConfig.builder().host("127.0.0.1").build();

        assertEquals(VDiskClientConfig.DEFAULT_KEEPALIVE_TIME, config.keepAliveTime());
        assertEquals(VDiskClientConfig.DEFAULT_KEEPALIVE_TIMEOUT, config.keepAliveTimeout());
        // Overall transfer is unbounded by default so a large read/restore is
        // never killed by a single wall-clock budget; liveness comes from keepalive.
        assertEquals(Duration.ZERO, config.overallDeadline());
    }

    @Test
    void defaultReadConcurrencySitsWellBelowSharedServerCap() {
        VDiskClientConfig config = VDiskClientConfig.builder().host("127.0.0.1").build();

        // The server's active-read-stream limit (64) is process-wide and shared
        // across all clients, so the default must stay well under it to leave
        // capacity for other jobs rather than let one client saturate the server.
        assertEquals(8, config.maxConcurrentReads());
        assertEquals(8, VDiskClientConfig.DEFAULT_MAX_CONCURRENT_READS);
        assertTrue(config.maxConcurrentReads() < 64,
                "default read concurrency must sit below the server's shared stream cap");
    }

    @Test
    void rejectsNegativeTimeouts() {
        assertThrows(IllegalArgumentException.class, () -> VDiskClientConfig.builder()
                .host("127.0.0.1").keepAliveTime(Duration.ofSeconds(-1)).build());
        assertThrows(IllegalArgumentException.class, () -> VDiskClientConfig.builder()
                .host("127.0.0.1").keepAliveTimeout(Duration.ofSeconds(-1)).build());
        assertThrows(IllegalArgumentException.class, () -> VDiskClientConfig.builder()
                .host("127.0.0.1").overallDeadline(Duration.ofSeconds(-1)).build());
    }
}
