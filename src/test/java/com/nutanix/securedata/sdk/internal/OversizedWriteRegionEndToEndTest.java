/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * End-to-end test that an oversized write region is rejected without deadlock.
 */
package com.nutanix.securedata.sdk.internal;

import com.google.protobuf.ByteString;
import com.nutanix.api.dataprotection.streaming.v1.VDiskDataSvcGrpc;
import com.nutanix.api.dataprotection.streaming.v1.VDiskErrorCode;
import com.nutanix.api.dataprotection.streaming.v1.VDiskWriteArg;
import com.nutanix.api.dataprotection.streaming.v1.VDiskWriteRet;
import com.nutanix.securedata.sdk.api.ErrorCode;
import com.nutanix.securedata.sdk.api.VDiskClientConfig;
import com.nutanix.securedata.sdk.api.WriteAck;
import com.nutanix.securedata.sdk.api.WriteDataProvider;
import com.nutanix.securedata.sdk.api.WriteError;
import com.nutanix.securedata.sdk.api.WriteHandler;
import com.nutanix.securedata.sdk.api.WriteRegion;
import com.nutanix.securedata.sdk.api.WriteRegionsRequest;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.SelfSignedCertificate;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * End-to-end guard that an oversized write region fails fast instead of
 * deadlocking the stream.
 *
 * <p>A region whose declared length exceeds {@code maxWriteBytesPerRequest} can
 * never fit the in-flight byte budget (its capacity is
 * {@code >= maxWriteBytesPerRequest}), so reserving its bytes would block until
 * the RPC deadline aborts the whole stream. The client must reject it with
 * {@link ErrorCode#INVALID_ARGUMENT} before touching either gate or the data
 * provider, and keep the stream healthy so later regions still succeed.
 */
class OversizedWriteRegionEndToEndTest {

    // Small caps so an oversized region is a few KiB, not hundreds of MiB. The
    // budget equals the per-request cap (its minimum), reproducing the reviewer's
    // "512 MiB cap and budget, 600 MiB region" deadlock at a testable scale.
    private static final long MAX_WRITE_BYTES_PER_REQUEST = 1024;

    @Test
    void oversizedFirstRegionFailsFastAndStreamStaysHealthy() throws Exception {
        // A fake server that acks every write arg with kNoError, echoing its
        // sequence number, and cleanly finishes on the client's half-close.
        VDiskDataSvcGrpc.VDiskDataSvcImplBase ackingService =
                new VDiskDataSvcGrpc.VDiskDataSvcImplBase() {
                    @Override
                    public StreamObserver<VDiskWriteArg> vDiskStreamWrite(
                            StreamObserver<VDiskWriteRet> responseObserver) {
                        return new StreamObserver<>() {
                            @Override
                            public void onNext(VDiskWriteArg value) {
                                responseObserver.onNext(VDiskWriteRet.newBuilder()
                                        .setSequenceNumber(value.getSequenceNumber())
                                        .setErrorCode(VDiskErrorCode.kNoError)
                                        .build());
                            }

                            @Override
                            public void onError(Throwable t) {
                            }

                            @Override
                            public void onCompleted() {
                                responseObserver.onCompleted();
                            }
                        };
                    }
                };

        SelfSignedCertificate cert = new SelfSignedCertificate("localhost");
        SslContext serverSsl = GrpcSslContexts
                .forServer(cert.certificate(), cert.privateKey())
                .build();
        Server server = NettyServerBuilder.forPort(0)
                .sslContext(serverSsl)
                .addService(ackingService)
                .build()
                .start();

        VDiskClientConfig config = VDiskClientConfig.builder()
                .host("localhost")
                .port(server.getPort())
                .keepAliveTime(Duration.ofSeconds(10))
                .maxWriteBytesPerRequest(MAX_WRITE_BYTES_PER_REQUEST)
                .maxInFlightWriteBytes(MAX_WRITE_BYTES_PER_REQUEST)
                .build();

        // First region is oversized (2x the cap); the second is valid. If the
        // oversized region were not rejected up front, reserving its bytes would
        // park the sender until the deadline and the valid region would never send.
        WriteRegion oversized = WriteRegion.ofData(0, (int) (MAX_WRITE_BYTES_PER_REQUEST * 2));
        WriteRegion valid = WriteRegion.ofData(4096, (int) (MAX_WRITE_BYTES_PER_REQUEST / 2));
        List<WriteRegion> regions = List.of(oversized, valid);

        // Non-zero bytes so the valid region is a real data write; the oversized
        // region is rejected before the provider is ever consulted.
        WriteDataProvider provider = (offset, length) -> {
            byte[] bytes = new byte[length];
            Arrays.fill(bytes, (byte) 1);
            return ByteString.copyFrom(bytes);
        };

        Map<Long, WriteError> errorsByOffset = new ConcurrentHashMap<>();
        List<WriteAck> acks = Collections.synchronizedList(new ArrayList<>());
        WriteHandler handler = new WriteHandler() {
            @Override
            public void onSuccess(WriteAck ack) {
                acks.add(ack);
            }

            @Override
            public void onError(WriteError error) {
                errorsByOffset.put(error.offset(), error);
            }
        };

        try (GrpcVDiskClient client = new GrpcVDiskClient(config)) {
            // Must return promptly: the fix rejects the oversized region without
            // waiting on the byte budget, so this never approaches the deadline.
            assertTimeoutPreemptively(Duration.ofSeconds(15), () ->
                    client.write(new WriteRegionsRequest(regions), provider, handler));
        } finally {
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
            cert.delete();
        }

        WriteError oversizedError = errorsByOffset.get(oversized.offset());
        assertEquals(ErrorCode.INVALID_ARGUMENT, oversizedError.errorCode(),
                "an oversized region must fail fast with INVALID_ARGUMENT");

        assertNull(errorsByOffset.get(valid.offset()),
                "the valid region must not be failed by the oversized one");
        assertEquals(1, acks.size(), "exactly the valid region should be acknowledged");
        assertEquals(valid.offset(), acks.get(0).offset(),
                "the acknowledged region must be the valid one; the stream stayed healthy");
    }
}
