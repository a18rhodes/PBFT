# PBFT

A naive implementation of [Practical Byzantine Fault Tolerance](https://en.wikipedia.org/wiki/Byzantine_fault) built as a learning exercise to understand how the protocol actually works end-to-end, not to produce anything usable.

Do not use this for anything real.

## What it implements

1. Three-phase commit: PrePrepare, Prepare, Commit
2. View change on timer expiry, with prepared proofs forwarded in the ViewChange message
3. New primary re-issues prepared-but-uncommitted requests under the new view
4. Per-request failover timer on each replica, cancelled on commit
5. Replicas self-register at startup; membership list is sorted for consistent primary election across nodes

## What it skips

All of this is intentional, the point was the protocol logic, not the surrounding machinery.

1. Cryptography
   - No signatures, no MACs, no client or replica authentication. Any node can forge any message.
2. Checkpointing and garbage collection
   - The log grows without bound.
3. Watermarks
   - The low/high watermark window that bounds in-flight sequence numbers is absent.
4. ViewChange proof validation
   - The new primary accepts ViewChange messages without verifying the enclosed prepared proofs.
5. Client reply protocol
   - In the paper replicas reply directly to the client and the client waits for f+1 matching replies; here the HTTP response from the primary carries the sequence identity back to the caller.

## Where it deviates from the paper

Beyond outright omissions, a few things work differently by design.

1. HTTP transport
   - The paper assumes reliable point-to-point channels; this uses FastAPI and `requests` over localhost, which handles delivery and ordering for free, which is cheating a bit but keeps the transport layer out of the way.
2. Content digest
   - Uses `xxhash` rather than a collision-resistant hash, intentionally non-cryptographic since it just needs to be deterministic across processes.
3. Primary deduplication by content hash
   - The paper tracks requests by client ID and timestamp; here the primary deduplicates by content digest, which means two clients sending identical payloads would collide.
4. Backup forwarding
   - Backups forward client requests to the primary synchronously and start the failover timer after the primary acks, whereas the paper has backups buffer the request and only escalate if no PrePrepare arrives within the timeout.
5. Timer granularity
   - One timer per content digest, not one per (client, timestamp) pair.

## Running it

```bash
# 4-node cluster on ports 8000–8003
uv run pbft launch-cluster -m 8000 -n 4

# send a request (broadcasts to all replicas)
uv run pbft send "hello" --port 8000 --port 8001 --port 8002 --port 8003
```

## License

MIT. See [LICENSE](LICENSE).
