/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Example: incremental-backup read driven by changed-region tracking.
 */
package com.nutanix.securedata.sdk.examples;

import com.google.protobuf.ByteString;
import com.nutanix.securedata.sdk.SecureDataAccess;
import com.nutanix.securedata.sdk.api.ReadError;
import com.nutanix.securedata.sdk.api.ReadHandler;
import com.nutanix.securedata.sdk.api.ReadRegion;
import com.nutanix.securedata.sdk.api.RecoveredChunk;

import java.util.List;

/**
 * Incremental-backup read example for the Secure Data Java SDK.
 *
 * <p>An incremental backup only copies what changed since the last recovery
 * point. The application first asks the Nutanix Changed Region Tracking (CBT)
 * API which regions changed — that call classifies each region as {@code ZEROED}
 * or {@code REGULAR} — and then reads back only the {@code REGULAR} (data)
 * regions with this SDK. {@code ZEROED} regions are recorded as holes in the
 * backup and never read, so no bytes are fetched for them.
 *
 * <p>The read path is streaming: a single {@link ReadHandler} exposes one
 * callback for recovered chunks and one for failed ranges, each invoked as soon
 * as it arrives. Every chunk and error carries its absolute vdisk offset, so a
 * real backup persists each chunk directly at that offset (for example
 * {@code pwrite(fd, data, length, offset)}) and retries failed ranges rather
 * than accumulating the whole read in memory.
 */
public final class BackupReadExample {
    private BackupReadExample() {
    }

    /**
     * Prints recovered data and failures. A real backup would instead persist
     * each chunk at its absolute {@code offset()} (materializing zeros when
     * {@code zeroData()} is true) and retry the {@code [offset, length)} range of
     * each error, optionally keyed off {@code errorCode()}.
     */
    private static final class PrintingReadHandler implements ReadHandler {
        @Override
        public void onChunk(RecoveredChunk chunk) {
            System.out.println(
                    "READ offset=" + chunk.offset()
                            + " length=" + chunk.length()
                            + " zeroData=" + chunk.zeroData()
                            + " preview=" + preview(chunk.data(), 16));
        }

        @Override
        public void onError(ReadError error) {
            System.err.println(
                    "READ FAILED offset=" + error.offset()
                            + " length=" + error.length()
                            + " errorCode=" + error.errorCode()
                            + " message=" + error.message());
        }
    }

    /**
     * Runs an incremental-backup read using system properties for connection and
     * range values.
     *
     * <p>Common properties:
     * {@code -Dvdisk.host}, {@code -Dvdisk.port}, {@code -Dvdisk.token},
     * {@code -Dvdisk.offset}, and {@code -Dvdisk.length}.
     */
    public static void main(String[] args) {
        // Base range size used for the stand-in changed regions below.
        long offset = Long.getLong("vdisk.offset", 0L);
        long length = Long.getLong("vdisk.length", 1L << 20);

        // Connection settings default to the local SSH tunnel used during development.
        String host = System.getProperty("vdisk.host", "127.0.0.1");
        int port = Integer.getInteger("vdisk.port", 14231);
        String token = System.getProperty("vdisk.token", "TOKEN");
        long timeoutSeconds = Long.getLong("vdisk.timeoutSeconds", 120L);

        // The changed data regions to back up. A real backup builds this list
        // from the CBT "changed regions" response, keeping only the REGULAR
        // (non-zero) entries — the ZEROED ones are recorded as holes without a
        // read. Each region is read on its own stream; the SDK keeps a bounded
        // number of streams in flight and queues the rest, so this list can be
        // arbitrarily long. A full backup is just the same call over every
        // allocated region.
        List<ReadRegion> changedRegions = List.of(
                new ReadRegion(offset, length),
                new ReadRegion(offset + (1L << 30), length),
                new ReadRegion(offset + (2L << 30), length)
        );

        try (SecureDataAccess.Client client =
                     new SecureDataAccess.Client(host, port, token, timeoutSeconds)) {
            // App code owns each chunk: write it to the backup target at
            // chunk.offset(), checksum it, or process it inline. For zeroData=true
            // chunks, data is empty and length() tells how many zeros to record.
            // Failed ranges arrive on the same handler's error callback so the
            // backup can retry exactly those bytes.
            client.readRegions(changedRegions, new PrintingReadHandler());
        } catch (RuntimeException e) {
            System.err.println("Backup read failed: " + e.getMessage());
            System.err.println("Hint: verify -Dvdisk.host, -Dvdisk.port, and -Dvdisk.token values.");
            throw e;
        }
    }

    /**
     * Returns a short hexadecimal preview so the example proves data is available
     * without printing an entire vdisk chunk to stdout.
     */
    private static String preview(ByteString data, int maxBytes) {
        if (data.isEmpty()) {
            return "<empty>";
        }
        StringBuilder builder = new StringBuilder();
        int bytesToShow = Math.min(data.size(), maxBytes);
        for (int i = 0; i < bytesToShow; i++) {
            if (i > 0) {
                builder.append(' ');
            }
            builder.append(String.format("%02x", data.byteAt(i) & 0xff));
        }
        if (data.size() > bytesToShow) {
            builder.append(" ...");
        }
        return builder.toString();
    }
}
