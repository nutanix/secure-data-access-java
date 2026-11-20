/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Example: restore write replaying data and zero-fill regions in one call.
 */
package com.nutanix.securedata.sdk.examples;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.nutanix.securedata.sdk.SecureDataAccess;
import com.nutanix.securedata.sdk.api.WriteAck;
import com.nutanix.securedata.sdk.api.WriteDataProvider;
import com.nutanix.securedata.sdk.api.WriteError;
import com.nutanix.securedata.sdk.api.WriteHandler;
import com.nutanix.securedata.sdk.api.WriteRegion;

import java.util.List;

/**
 * Restore write example for the Secure Data Java SDK.
 *
 * <p>Restore is a one-way replay: the application takes the regions captured by a
 * backup and writes them to the target vdisk in a single
 * {@link SecureDataAccess.Client#writeRegions} call. Each region is a pure
 * destination descriptor — it says only where and how much to write — and its
 * type is declared by the caller from the backup's own metadata:
 * <ul>
 *   <li>a data region with {@link WriteRegion#ofData}: the bytes are pulled on
 *       the fly from a {@link WriteDataProvider} (the backup source) one range at
 *       a time, so peak memory stays bounded by the in-flight window rather than
 *       the total restore size;</li>
 *   <li>a hole with {@link WriteRegion#ofZeros}: no bytes travel the wire and the
 *       provider is never consulted. A backup marks a region a hole when CBT
 *       classified it {@code ZEROED}, so the restore never has to scan for zeros.</li>
 * </ul>
 *
 * <p>A plain contiguous restore is just a one-element region list.
 */
public final class RestoreWriteExample {

    // Size of each restored data extent in this example.
    private static final int EXTENT_BYTES = 64 * 1024;

    private RestoreWriteExample() {
    }

    public static void main(String[] args) {
        // The regions to restore, described only by offset, length, and type. A
        // real restore builds this list from the backup's captured regions:
        // data extents become ofData, and regions the backup recorded as holes
        // (CBT ZEROED) become ofZeros.
        List<WriteRegion> regions = List.of(
                WriteRegion.ofData(1L << 30, EXTENT_BYTES),
                WriteRegion.ofData(2L << 30, EXTENT_BYTES),
                // A region the backup recorded as a hole: declared zero-fill, so
                // no bytes are sent and the source is never consulted for it.
                WriteRegion.ofZeros(3L << 30, 4 << 20)
        );

        String host = System.getProperty("vdisk.host", "127.0.0.1");
        int port = Integer.getInteger("vdisk.port", 14231);
        String token = System.getProperty("vdisk.token", "TOKEN");
        long timeoutSeconds = Long.getLong("vdisk.timeoutSeconds", 120L);

        try (SecureDataAccess.Client client =
                     new SecureDataAccess.Client(host, port, token, timeoutSeconds)) {
            client.writeRegions(
                    regions,
                    // One data source for the whole restore; the SDK pulls each
                    // data region's range from it on the fly.
                    new BackupDataProvider(),
                    new WriteHandler() {
                        @Override
                        public void onSuccess(WriteAck ack) {
                            System.out.println(
                                    "  wrote offset=" + ack.offset() + " length=" + ack.length());
                        }

                        @Override
                        public void onError(WriteError error) {
                            System.out.println(
                                    "  FAILED offset=" + error.offset()
                                            + " length=" + error.length()
                                            + " code=" + error.errorCode()
                                            + " message=" + error.message());
                        }
                    });
        } catch (RuntimeException e) {
            System.err.println("Restore write failed: " + e.getMessage());
            System.err.println("Hint: verify -Dvdisk.host, -Dvdisk.port, and -Dvdisk.token values.");
            throw e;
        }
    }

    /**
     * How a caller supplies data: implement {@link WriteDataProvider} and override
     * {@link #getData(long, int)}. The SDK calls it once per data region with the
     * range it is about to send, so the bytes are produced on the fly and never
     * held ahead of time. A real implementation would seek to {@code offset} in
     * the backup source and read {@code length} bytes.
     */
    private static final class BackupDataProvider implements WriteDataProvider {
        @Override
        public ByteString getData(long offset, int length) {
            // Deterministic non-zero pattern standing in for real restored bytes.
            byte[] data = new byte[length];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) ((offset + i) & 0xFF | 0x01);
            }
            // We own this array and never touch it again, so wrap it without a
            // copy rather than ByteString.copyFrom.
            return UnsafeByteOperations.unsafeWrap(data);
        }
    }
}
