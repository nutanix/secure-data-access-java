package com.nutanix.api.dataprotection.streaming.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.64.0)",
    comments = "Source: vdisk.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class VDiskDataSvcGrpc {

  private VDiskDataSvcGrpc() {}

  public static final java.lang.String SERVICE_NAME = "VDiskDataSvc";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.nutanix.api.dataprotection.streaming.v1.VDiskWriteArg,
      com.nutanix.api.dataprotection.streaming.v1.VDiskWriteRet> getVDiskStreamWriteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "VDiskStreamWrite",
      requestType = com.nutanix.api.dataprotection.streaming.v1.VDiskWriteArg.class,
      responseType = com.nutanix.api.dataprotection.streaming.v1.VDiskWriteRet.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<com.nutanix.api.dataprotection.streaming.v1.VDiskWriteArg,
      com.nutanix.api.dataprotection.streaming.v1.VDiskWriteRet> getVDiskStreamWriteMethod() {
    io.grpc.MethodDescriptor<com.nutanix.api.dataprotection.streaming.v1.VDiskWriteArg, com.nutanix.api.dataprotection.streaming.v1.VDiskWriteRet> getVDiskStreamWriteMethod;
    if ((getVDiskStreamWriteMethod = VDiskDataSvcGrpc.getVDiskStreamWriteMethod) == null) {
      synchronized (VDiskDataSvcGrpc.class) {
        if ((getVDiskStreamWriteMethod = VDiskDataSvcGrpc.getVDiskStreamWriteMethod) == null) {
          VDiskDataSvcGrpc.getVDiskStreamWriteMethod = getVDiskStreamWriteMethod =
              io.grpc.MethodDescriptor.<com.nutanix.api.dataprotection.streaming.v1.VDiskWriteArg, com.nutanix.api.dataprotection.streaming.v1.VDiskWriteRet>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "VDiskStreamWrite"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.nutanix.api.dataprotection.streaming.v1.VDiskWriteArg.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.nutanix.api.dataprotection.streaming.v1.VDiskWriteRet.getDefaultInstance()))
              .setSchemaDescriptor(new VDiskDataSvcMethodDescriptorSupplier("VDiskStreamWrite"))
              .build();
        }
      }
    }
    return getVDiskStreamWriteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.nutanix.api.dataprotection.streaming.v1.VDiskReadArg,
      com.nutanix.api.dataprotection.streaming.v1.VDiskReadRet> getVDiskStreamReadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "VDiskStreamRead",
      requestType = com.nutanix.api.dataprotection.streaming.v1.VDiskReadArg.class,
      responseType = com.nutanix.api.dataprotection.streaming.v1.VDiskReadRet.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<com.nutanix.api.dataprotection.streaming.v1.VDiskReadArg,
      com.nutanix.api.dataprotection.streaming.v1.VDiskReadRet> getVDiskStreamReadMethod() {
    io.grpc.MethodDescriptor<com.nutanix.api.dataprotection.streaming.v1.VDiskReadArg, com.nutanix.api.dataprotection.streaming.v1.VDiskReadRet> getVDiskStreamReadMethod;
    if ((getVDiskStreamReadMethod = VDiskDataSvcGrpc.getVDiskStreamReadMethod) == null) {
      synchronized (VDiskDataSvcGrpc.class) {
        if ((getVDiskStreamReadMethod = VDiskDataSvcGrpc.getVDiskStreamReadMethod) == null) {
          VDiskDataSvcGrpc.getVDiskStreamReadMethod = getVDiskStreamReadMethod =
              io.grpc.MethodDescriptor.<com.nutanix.api.dataprotection.streaming.v1.VDiskReadArg, com.nutanix.api.dataprotection.streaming.v1.VDiskReadRet>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "VDiskStreamRead"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.nutanix.api.dataprotection.streaming.v1.VDiskReadArg.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.nutanix.api.dataprotection.streaming.v1.VDiskReadRet.getDefaultInstance()))
              .setSchemaDescriptor(new VDiskDataSvcMethodDescriptorSupplier("VDiskStreamRead"))
              .build();
        }
      }
    }
    return getVDiskStreamReadMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static VDiskDataSvcStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<VDiskDataSvcStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<VDiskDataSvcStub>() {
        @java.lang.Override
        public VDiskDataSvcStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new VDiskDataSvcStub(channel, callOptions);
        }
      };
    return VDiskDataSvcStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static VDiskDataSvcBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<VDiskDataSvcBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<VDiskDataSvcBlockingStub>() {
        @java.lang.Override
        public VDiskDataSvcBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new VDiskDataSvcBlockingStub(channel, callOptions);
        }
      };
    return VDiskDataSvcBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static VDiskDataSvcFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<VDiskDataSvcFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<VDiskDataSvcFutureStub>() {
        @java.lang.Override
        public VDiskDataSvcFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new VDiskDataSvcFutureStub(channel, callOptions);
        }
      };
    return VDiskDataSvcFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default io.grpc.stub.StreamObserver<com.nutanix.api.dataprotection.streaming.v1.VDiskWriteArg> vDiskStreamWrite(
        io.grpc.stub.StreamObserver<com.nutanix.api.dataprotection.streaming.v1.VDiskWriteRet> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getVDiskStreamWriteMethod(), responseObserver);
    }

    /**
     */
    default io.grpc.stub.StreamObserver<com.nutanix.api.dataprotection.streaming.v1.VDiskReadArg> vDiskStreamRead(
        io.grpc.stub.StreamObserver<com.nutanix.api.dataprotection.streaming.v1.VDiskReadRet> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getVDiskStreamReadMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service VDiskDataSvc.
   */
  public static abstract class VDiskDataSvcImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return VDiskDataSvcGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service VDiskDataSvc.
   */
  public static final class VDiskDataSvcStub
      extends io.grpc.stub.AbstractAsyncStub<VDiskDataSvcStub> {
    private VDiskDataSvcStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VDiskDataSvcStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new VDiskDataSvcStub(channel, callOptions);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<com.nutanix.api.dataprotection.streaming.v1.VDiskWriteArg> vDiskStreamWrite(
        io.grpc.stub.StreamObserver<com.nutanix.api.dataprotection.streaming.v1.VDiskWriteRet> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getVDiskStreamWriteMethod(), getCallOptions()), responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<com.nutanix.api.dataprotection.streaming.v1.VDiskReadArg> vDiskStreamRead(
        io.grpc.stub.StreamObserver<com.nutanix.api.dataprotection.streaming.v1.VDiskReadRet> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getVDiskStreamReadMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service VDiskDataSvc.
   */
  public static final class VDiskDataSvcBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<VDiskDataSvcBlockingStub> {
    private VDiskDataSvcBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VDiskDataSvcBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new VDiskDataSvcBlockingStub(channel, callOptions);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service VDiskDataSvc.
   */
  public static final class VDiskDataSvcFutureStub
      extends io.grpc.stub.AbstractFutureStub<VDiskDataSvcFutureStub> {
    private VDiskDataSvcFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VDiskDataSvcFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new VDiskDataSvcFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_VDISK_STREAM_WRITE = 0;
  private static final int METHODID_VDISK_STREAM_READ = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_VDISK_STREAM_WRITE:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.vDiskStreamWrite(
              (io.grpc.stub.StreamObserver<com.nutanix.api.dataprotection.streaming.v1.VDiskWriteRet>) responseObserver);
        case METHODID_VDISK_STREAM_READ:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.vDiskStreamRead(
              (io.grpc.stub.StreamObserver<com.nutanix.api.dataprotection.streaming.v1.VDiskReadRet>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getVDiskStreamWriteMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              com.nutanix.api.dataprotection.streaming.v1.VDiskWriteArg,
              com.nutanix.api.dataprotection.streaming.v1.VDiskWriteRet>(
                service, METHODID_VDISK_STREAM_WRITE)))
        .addMethod(
          getVDiskStreamReadMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              com.nutanix.api.dataprotection.streaming.v1.VDiskReadArg,
              com.nutanix.api.dataprotection.streaming.v1.VDiskReadRet>(
                service, METHODID_VDISK_STREAM_READ)))
        .build();
  }

  private static abstract class VDiskDataSvcBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    VDiskDataSvcBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.nutanix.api.dataprotection.streaming.v1.VDiskDataServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("VDiskDataSvc");
    }
  }

  private static final class VDiskDataSvcFileDescriptorSupplier
      extends VDiskDataSvcBaseDescriptorSupplier {
    VDiskDataSvcFileDescriptorSupplier() {}
  }

  private static final class VDiskDataSvcMethodDescriptorSupplier
      extends VDiskDataSvcBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    VDiskDataSvcMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (VDiskDataSvcGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new VDiskDataSvcFileDescriptorSupplier())
              .addMethod(getVDiskStreamWriteMethod())
              .addMethod(getVDiskStreamReadMethod())
              .build();
        }
      }
    }
    return result;
  }
}
