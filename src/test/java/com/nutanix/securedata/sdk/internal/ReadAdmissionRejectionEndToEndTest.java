/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * End-to-end test that a read-stream admission refusal surfaces once as SERVER_BUSY.
 */
package com.nutanix.securedata.sdk.internal;

import com.nutanix.api.dataprotection.streaming.v1.VDiskDataSvcGrpc;
import com.nutanix.api.dataprotection.streaming.v1.VDiskErrorCode;
import com.nutanix.api.dataprotection.streaming.v1.VDiskReadArg;
import com.nutanix.api.dataprotection.streaming.v1.VDiskReadRet;
import com.nutanix.securedata.sdk.api.ErrorCode;
import com.nutanix.securedata.sdk.api.ReadError;
import com.nutanix.securedata.sdk.api.ReadHandler;
import com.nutanix.securedata.sdk.api.ReadRegion;
import com.nutanix.securedata.sdk.api.ReadRegionsRequest;
import com.nutanix.securedata.sdk.api.RecoveredChunk;
import com.nutanix.securedata.sdk.api.VDiskClientConfig;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.SelfSignedCertificate;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end guard for Stargate's read-stream admission rejection over a real
 * gRPC/TLS transport.
 *
 * <p>The server's active-read-stream limit is process-wide and shared; a stream
 * opened past it is refused with an unsequenced {@code VDiskReadRet} that carries
 * {@code kServerBusy} and <em>no</em> range vector, immediately followed by a
 * transport cancel. That refusal is a back-off signal for the whole region, not a
 * per-range data failure, so the region must be reported exactly once with
 * {@link ErrorCode#SERVER_BUSY} — the SDK must not also report the ensuing
 * {@code CANCELLED} as a second, masking failure.
 *
 * <p>This drives the actual {@link GrpcVDiskClient#read} path against a fake
 * server that reproduces that sequence.
 */
class ReadAdmissionRejectionEndToEndTest {

    private static final String REJECTION_MESSAGE =
            "Server is at maximum concurrent read-stream capacity";

    @Test
    void serverBusyRefusalIsReportedOnceAsBackoffSignal() throws Exception {
        // A fake server that, as soon as a read stream opens, emits one
        // unsequenced kServerBusy response with no range vector and then cancels
        // the stream, exactly as Stargate does when the shared read-stream cap is
        // hit at admission time.
        VDiskDataSvcGrpc.VDiskDataSvcImplBase admissionRejectingService =
                new VDiskDataSvcGrpc.VDiskDataSvcImplBase() {
                    @Override
                    public StreamObserver<VDiskReadArg> vDiskStreamRead(
                            StreamObserver<VDiskReadRet> responseObserver) {
                        responseObserver.onNext(VDiskReadRet.newBuilder()
                                .setErrorCode(VDiskErrorCode.kServerBusy)
                                .setErrorMessage(REJECTION_MESSAGE)
                                .build());
                        responseObserver.onError(Status.UNAVAILABLE
                                .withDescription("stream cancelled after admission rejection")
                                .asRuntimeException());
                        return new StreamObserver<>() {
                            @Override
                            public void onNext(VDiskReadArg value) {
                            }

                            @Override
                            public void onError(Throwable t) {
                            }

                            @Override
                            public void onCompleted() {
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

        List<ReadRegion> regions = List.of(
                new ReadRegion(0, 8),
                new ReadRegion(8, 8));

        List<RecoveredChunk> chunks = new CopyOnWriteArrayList<>();
        List<ReadError> errors = new CopyOnWriteArrayList<>();
        ReadHandler handler = new ReadHandler() {
            @Override
            public void onChunk(RecoveredChunk chunk) {
                chunks.add(chunk);
            }

            @Override
            public void onError(ReadError error) {
                errors.add(error);
            }
        };

        try (GrpcVDiskClient client = new GrpcVDiskClient(config)) {
            assertTimeoutPreemptively(Duration.ofSeconds(15), () ->
                    client.read(new ReadRegionsRequest(regions), handler));
        } finally {
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
            cert.delete();
        }

        assertTrue(chunks.isEmpty(), "a refused read must not yield any recovered chunks");
        // Exactly one error per region: the SERVER_BUSY back-off signal, and not a
        // second CANCELLED masking it.
        assertEquals(regions.size(), errors.size(),
                "each refused region must be reported exactly once, not doubled by the follow-on cancel");
        for (ReadError error : errors) {
            assertEquals(ErrorCode.SERVER_BUSY, error.errorCode(),
                    "a stream-level refusal must surface as the SERVER_BUSY back-off code");
            assertEquals(REJECTION_MESSAGE, error.message(),
                    "the server's admission-rejection message must be preserved");
        }
    }
}
