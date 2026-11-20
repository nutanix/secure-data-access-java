# Secure Data Java SDK - Client Usage Guide

This guide is for application developers who want to use the SDK without
needing to know internal transport details.

## What you use from the SDK

The single entrypoint is one facade class:

```java
import com.nutanix.securedata.sdk.SecureDataAccess;
```

Core type:

- `SecureDataAccess.Client` — an `AutoCloseable` connection to one vdisk.

The two operations are region-based and stream their results through a handler:

- `readRegions(List<ReadRegion> regions, ReadHandler handler)`
- `writeRegions(List<WriteRegion> regions, WriteDataProvider dataProvider, WriteHandler handler)`

Everything else you touch lives in `com.nutanix.securedata.sdk.api`:

- `ReadRegion`, `ReadHandler`, `RecoveredChunk`, `ReadError`
- `WriteRegion`, `WriteHandler`, `WriteDataProvider`, `WriteAck`, `WriteError`
- `ErrorCode`

## 1) Create the client

```java
String host = "10.96.84.198"; // CVM IP or DNS reachable from your client
int port = 14231;
String token = "TOKEN";        // bearer token; also identifies the disk
long timeoutSeconds = 120L;    // keepalive ping interval (see note below)

try (SecureDataAccess.Client client =
             new SecureDataAccess.Client(host, port, token, timeoutSeconds)) {
    // use client here
}
```

Timeout notes:

- `timeoutSeconds` is the **keepalive ping interval**, not a whole-operation
  deadline. While a call is active the transport pings the server this often and
  fails the call if the connection is dead (reported as `ErrorCode.SERVER_BUSY`
  from the transport's `UNAVAILABLE`). This is deliberate: the server streams
  reads up to 1 TiB with no deadline of its own, so connection liveness — not a
  wall-clock budget — is what bounds a hung transfer, and a large transfer is
  never killed while the connection stays alive.
- For finer control, build a `VDiskClientConfig` (via `VDiskClients`) with
  `keepAliveTime` (ping interval, default 30s), `keepAliveTimeout` (ack wait,
  default 20s), and `overallDeadline` (an optional hard cap on the whole
  operation, default **unbounded**). When set, an `overallDeadline` breach is
  reported as `DEADLINE_EXCEEDED`, distinguishable from a genuine server error and
  typically safe to retry.

Transport notes:

- The transport is always TLS, with server-certificate verification skipped.
  Authorization comes from the bearer `token`, not from the server certificate,
  so there is no CA certificate, `serverName`, or "skip verify" flag to pass.
- The `token` authenticates the caller and also carries the disk access scope.
  The server derives which disk can be read/written from this token context, so
  the SDK holds no disk details of its own. To access a different disk, use a
  token scoped for that disk.

Optional read-concurrency cap:

```java
int maxConcurrentReads = 16; // defaults to 8
try (SecureDataAccess.Client client =
             new SecureDataAccess.Client(host, port, token, timeoutSeconds, maxConcurrentReads)) {
    // ...
}
```

`maxConcurrentReads` is a client-wide cap on how many read streams the SDK keeps
in flight at once. Concurrent `readRegions` calls on the same client share that
pool; the cap is not applied separately to each call. Extra regions are queued
and started as earlier reads finish, so the cap bounds concurrency without
limiting how many regions a single call may pass.

The server's active-read-stream limit is **process-wide and shared** across every
client (default 64 on the server), not a per-client budget, so the SDK default is
a conservative `8` to avoid one client saturating the server and starving other
jobs. Raise it only when the client has the server largely to itself. A stream the
server refuses past its limit is reported as `ErrorCode.SERVER_BUSY` — a back-off
signal to retry the whole read later, not a per-range data failure.

## 2) Read

`readRegions` fetches one or more, possibly non-contiguous, regions in a single
call. A plain contiguous read is just a one-element list; an incremental-backup
read passes the changed extents as multiple regions. Each region is fetched on
its own stream (the server allows only one read per stream); the SDK keeps a
bounded number of streams in flight and queues the rest, so the region list can
be arbitrarily long.

Results stream through a `ReadHandler`: `onChunk` receives each recovered chunk
as it arrives, and `onError` receives each failed range. Callbacks are **not**
serialized and may run concurrently on independent threads, so the handler must
be thread-safe. Because every chunk and error carries its absolute vdisk offset,
callers can persist data directly and retry exactly the ranges that failed —
there is no aggregate return value. The call returns only after every region has
completed.

```java
List<ReadRegion> regions = List.of(
        new ReadRegion(0L, 1L << 20),          // offset, length
        new ReadRegion(1L << 30, 1L << 20)
);

client.readRegions(regions, new ReadHandler() {
    @Override
    public void onChunk(RecoveredChunk chunk) {
        // chunk.offset() is the absolute vdisk offset — write it there directly.
        // When chunk.zeroData() is true, chunk.data() is empty and chunk.length()
        // tells how many zeros to materialize.
        // chunk.data() is an immutable ByteString; consume it in place
        // (e.g. asReadOnlyByteBuffer() or writeTo(OutputStream)) to avoid a copy.
    }

    @Override
    public void onError(ReadError error) {
        // Retry exactly [error.offset(), error.length()); error.errorCode() is typed.
    }
});
```

## 3) Write

`writeRegions` writes one or more, possibly non-contiguous, regions in a single
call. A plain contiguous write is a one-element list; an incremental-backup
restore passes the changed extents (real data or zero-fill holes) as multiple
regions. Each region maps to one request, pipelined on a single stream and
acknowledged independently. The SDK keeps a bounded number of requests in flight
and queues the rest, so the region list can be arbitrarily long.

A region carries no bytes — it only says where and how much to write. Build one
with `WriteRegion.ofData(offset, length)` for real data or
`WriteRegion.ofZeros(offset, length)` for a zero-fill hole. The bytes for data
regions are pulled from the `WriteDataProvider` on the fly at send time (one
range at a time), so the caller never holds the whole payload in memory.

Results stream through a `WriteHandler`: `onSuccess` receives each acknowledged
region and `onError` receives each failed region, both keyed by absolute offset
and length so a caller can retry exactly what failed. Callbacks may run
concurrently, so the handler must be thread-safe. The call returns once every
region has reported.

```java
byte[] payload = "hello secure data".getBytes(java.nio.charset.StandardCharsets.UTF_8);

List<WriteRegion> regions = List.of(WriteRegion.ofData(1L << 30, payload.length));

client.writeRegions(
        regions,
        // Data provider: the SDK asks for a range and you return exactly that
        // many bytes as an immutable ByteString, produced on the fly.
        (offset, length) -> ByteString.copyFrom(payload),
        new WriteHandler() {
            @Override
            public void onSuccess(WriteAck ack) {
                System.out.println("wrote offset=" + ack.offset() + " length=" + ack.length());
            }

            @Override
            public void onError(WriteError error) {
                System.err.println("FAILED offset=" + error.offset()
                        + " length=" + error.length()
                        + " code=" + error.errorCode()
                        + " message=" + error.message());
            }
        });
```

Write behavior notes:

- `WriteRegion.length` is an `int`; a wire range's length is int32. To write more
  than `Integer.MAX_VALUE` bytes, split the work across multiple regions.
- Each region maps to a single request, so its data must fit in one request. Keep
  each region at or below the per-request cap (`maxWriteBytesPerRequest`, default
  30 MiB). A region whose data exceeds the cap is not sent — it is reported via
  `WriteHandler.onError` with `ErrorCode.INVALID_ARGUMENT`.
- Zero-fill regions send no bytes and never consult the provider. The SDK does
  not scan data regions for zeros, so a caller that knows a region is empty — for
  example from changed-region tracking, whose API already classifies each region
  as zeroed or regular — should declare it a hole with `WriteRegion.ofZeros`.
- A provider that returns `null`, the wrong number of bytes, or throws fails only
  that one region; the remaining regions still send.

## 4) Handling errors and retries

Both `ReadError` and `WriteError` expose a typed `ErrorCode` so you can drive a
retry policy without parsing strings:

- `NO_ERROR`
- `INTERNAL_ERROR`
- `INVALID_ARGUMENT`
- `DISK_NOT_FOUND`
- `DISK_READ_ERROR`
- `DISK_WRITE_ERROR`
- `PERMISSION_DENIED`
- `CONCURRENT_REQUEST_NOT_SUPPORTED`
- `SERVER_BUSY`
- `RETRY`
- `UNKNOWN` (a code newer than this SDK build; upgrade the SDK to interpret it)

Whether a given code is worth retrying is left to your policy. Because each error
carries the exact `[offset, length)` that failed, retrying means resubmitting
just that range or region.

Handler threading and exceptions:

- Handlers may be invoked concurrently on SDK transport threads. Guard shared
  mutable state with your own synchronization, atomics, or concurrent
  collections, and keep callbacks short and non-blocking.
- Exceptions thrown from a handler callback are logged and suppressed by the SDK;
  they do not propagate out of the call, cancel the stream, or stop other regions.
  Signal problems through your own state rather than by throwing.

## 5) Tuning

Connection-level tuning is set when constructing the client. The facade exposes
`maxConcurrentReads` directly (see section 1). Write and keepalive knobs are
client tunables on `VDiskClientConfig`. Their defaults match the stock Stargate
vdisk gRPC server flags named below. The SDK does not read those flags. If an
operator changes a flag, set the matching client knob to the same value or a
default client can be rejected.

| Setting | Default | Purpose |
| --- | --- | --- |
| `maxConcurrentReads` | 8 | Client-wide cap on read streams in flight. Concurrent `readRegions` calls share this pool. The server's `stargate_vdisk_grpc_server_max_active_read_streams` (default 64) is process-wide and shared, so 8 leaves room for other jobs. |
| `maxWriteBytesPerRequest` | 30 MiB | Largest payload allowed in one write request. Matches `stargate_vdisk_grpc_server_max_in_flight_write_bytes_per_stream`. A larger region is rejected locally with `INVALID_ARGUMENT`; the server rejects a larger request with `kInvalidArgument`. |
| `maxInFlightWriteRequests` | 20 | Write requests kept in flight (sent, unacked) on one stream. Matches `stargate_vdisk_grpc_server_max_active_write_requests_per_stream`. The server rejects extras with `kServerBusy`. The SDK does not retry those rejections, so the default stays at the server cap and queues the rest. |
| `maxInFlightWriteBytes` | 30 MiB | Aggregate unacked write payload on one stream. Matches the same server byte flag. The server pauses reads on the stream at 75% of that flag. |
| `keepAliveTime` | 30s | HTTP/2 keepalive ping interval while a call is active. `Duration.ZERO` disables keepalive. |
| `keepAliveTimeout` | 20s | How long to wait for a ping ack before treating the connection as dead. |

The write byte budget (`maxInFlightWriteBytes`) bounds peak memory independently
of the request count: the sender blocks until the budget has room before pulling
the next region's bytes, so large regions throttle by bytes rather than only by
count. It must be `>= maxWriteBytesPerRequest`. With the defaults, one 30 MiB
region fills the byte budget, and further regions wait until that write is
acknowledged.

## 6) Direct-connect preflight (before running examples)

Direct connection is supported; SSH tunneling is optional and only needed when
your workstation cannot route to the CVM/service network.

Before running read/write examples, verify:

- Service is listening on server: `ss -lntp | grep 14231`
- TCP reachability from client: `nc -zv -w 3 <cvm-ip> 14231`
- Firewall allows your actual source IP (often VPN IP, not local Wi-Fi IP)

Find your route/source context on macOS:

```bash
route -n get <cvm-ip>
ifconfig | grep -A3 utun
```

Example temporary allow rule on CVM (replace with your routed source IP):

```bash
sudo iptables -I INPUT 1 -s <client-or-vpn-ip>/32 -p tcp --dport 14231 -j ACCEPT
```

Use your platform-approved persistent firewall flow (for example,
`modify_firewall`) after validation.

## 7) Bundled examples

Runnable examples live under `examples/` (package
`com.nutanix.securedata.sdk.examples`):

- `BackupReadExample` — incremental-backup read: streams back only the changed
  (CBT `REGULAR`) regions; `ZEROED` regions are recorded as holes and never read.
- `RestoreWriteExample` — restore write: replays several non-contiguous regions,
  including zero-fill holes, from a single `WriteDataProvider` in one call.

They accept these system properties (with defaults): `vdisk.host` (`127.0.0.1`),
`vdisk.port` (`14231`), `vdisk.token` (`TOKEN`), `vdisk.timeoutSeconds` (`120`),
and, for `BackupReadExample`, `vdisk.offset` (`0`) and `vdisk.length` (`1048576`).

## 8) Running examples from CLI

Backup read (incremental):

```bash
mvn exec:java -Dexec.mainClass=com.nutanix.securedata.sdk.examples.BackupReadExample \
  -Dvdisk.host=10.96.84.198 -Dvdisk.port=14231 \
  -Dvdisk.token=TOKEN \
  -Dvdisk.offset=0 -Dvdisk.length=1048576
```

Restore write:

```bash
mvn exec:java -Dexec.mainClass=com.nutanix.securedata.sdk.examples.RestoreWriteExample \
  -Dvdisk.host=10.96.84.198 -Dvdisk.port=14231 \
  -Dvdisk.token=TOKEN
```
