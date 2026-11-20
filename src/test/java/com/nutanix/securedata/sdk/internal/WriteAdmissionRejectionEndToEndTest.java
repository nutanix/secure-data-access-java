/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * End-to-end test for write-stream admission rejection handling.
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
import io.grpc.Status;
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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end guard for Stargate's connection-admission rejection over a real
 * gRPC/TLS transport.
 *
 * <p>Stargate reports admission failures (server busy, permission denied,
 * concurrent writer, ...) as an <em>unsequenced</em> {@code VDiskWriteRet}
 * (sequence number 0) and then cancels the stream. That response matches no
 * pending region, so its typed reason must be captured as the stream's failure
 * and preserved when the transport cancellation follows — otherwise every
 * outstanding and unsent region collapses to a generic {@code INTERNAL_ERROR}.
 *
 * <p>This drives the actual {@link GrpcVDiskClient#write} path (its own Netty TLS
 * channel, response {@link StreamObserver}, and terminal drains) against a fake
 * server that reproduces that sequence, and asserts every region is reported with
 * the original {@link ErrorCode#SERVER_BUSY} and server message.
 */
class WriteAdmissionRejectionEndToEndTest {

    private static final String REJECTION_MESSAGE = "connection admission failed";

    @Test
    void unsequencedRejectionThenCancelFailsEveryRegionWithOriginalCode() throws Exception {
        // A fake server that accepts the pipelined write args, then — on the
        // client's half-close — emits one unsequenced kServerBusy response and
        // cancels the stream, exactly as Stargate does on admission failure.
        VDiskDataSvcGrpc.VDiskDataSvcImplBase admissionRejectingService =
                new VDiskDataSvcGrpc.VDiskDataSvcImplBase() {
                    @Override
                    public StreamObserver<VDiskWriteArg> vDiskStreamWrite(
                            StreamObserver<VDiskWriteRet> responseObserver) {
                        return new StreamObserver<>() {
                            @Override
                            public void onNext(VDiskWriteArg value) {
                                // Accept and drop; admission is rejected wholesale below.
                            }

                            @Override
                            public void onError(Throwable t) {
                            }

                            @Override
                            public void onCompleted() {
                                // Unsequenced (sequence_number defaults to 0) typed
                                // rejection, immediately followed by cancellation.
                                responseObserver.onNext(VDiskWriteRet.newBuilder()
                                        .setErrorCode(VDiskErrorCode.kServerBusy)
                                        .setErrorMessage(REJECTION_MESSAGE)
                                        .build());
                                responseObserver.onError(Status.UNAVAILABLE
                                        .withDescription("stream cancelled after admission rejection")
                                        .asRuntimeException());
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
                .addService(admissionRejectingService)
                .build()
                .start();

        VDiskClientConfig config = VDiskClientConfig.builder()
                .host("localhost")
                .port(server.getPort())
                .keepAliveTime(Duration.ofSeconds(10))
                .build();

        List<WriteRegion> regions = List.of(
                WriteRegion.ofData(0, 8),
                WriteRegion.ofData(8, 8),
                WriteRegion.ofData(16, 8));

        // Non-zero bytes so each region is a real data region (all-zero data would
        // be sent as a zero-fill hole); it is still owned by `pending` either way.
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
            assertTimeoutPreemptively(Duration.ofSeconds(15), () ->
                    client.write(new WriteRegionsRequest(regions), provider, handler));
        } finally {
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
            cert.delete();
        }

        assertTrue(acks.isEmpty(), "no region should be acknowledged when admission is rejected");
        assertEquals(regions.size(), errorsByOffset.size(),
                "every submitted region must receive exactly one terminal error");
        for (WriteRegion region : regions) {
            WriteError error = errorsByOffset.get(region.offset());
            assertEquals(ErrorCode.SERVER_BUSY, error.errorCode(),
                    "region at offset " + region.offset()
                            + " must carry the original admission-rejection code, not INTERNAL_ERROR");
            assertEquals(REJECTION_MESSAGE, error.message(),
                    "the server's admission-rejection message must be preserved");
        }
    }
}
