# bk-rs

A toy blockchain-based voting system in Rust. Votes are recorded as proof-of-work-mined blocks
gossiped across a peer-to-peer network of nodes, with one node exposing an HTTP API for casting
votes and reading results.

## How it works

- **Nodes** communicate over raw TCP using length-delimited, `wincode`-serialized messages
  (`src/types/network_message.rs`). Each node listens on an OS-assigned port and can act as a
  root peer (the first node started) or join an existing network by connecting to a root peer's
  IP/port.
- **Peer discovery**: a joining node sends a `PeerDiscoveryReq` to the root peer and receives back
  the current peer list, then gossips its own presence (`PushPeersReq`) to the network.
- **Voting**: the root peer runs a [Rocket](https://rocket.rs/) HTTP server
  (`src/http.rs`) with endpoints to submit a vote, look up a vote, and get totals/tallies. Casting
  a vote creates a `MiningTask` that is fanned out to a subset of known peers to mine.
- **Mining**: a block is valid once its SHA-256 hash has a configured number of leading zero bits
  (`DIFFICULTY` in `src/types/block.rs`, mirrored in `config.toml`). Blocks link to the previous
  block's hash and record the voter's UUID and chosen candidate, so each voter can only be counted
  once. Mining runs one block at a time per node (`src/types/app_state.rs`), racing against blocks
  mined elsewhere; a node that loses the race requeues its task against the new chain tip.
- **Chain sync**: nodes validate and adopt the longest/newest valid chain seen from peers
  (`AppState::sync_chain`), rebuilding an in-memory vote cache (`src/types/vote_cache.rs`) for O(1)
  vote lookups and tallying instead of rescanning the chain.

## Project layout

```
src/
  main.rs                 node startup, arg parsing, TCP listener loop
  http.rs                  Rocket HTTP API (root peer only)
  randomizer.rs            block mining
  utils.rs                 TCP send/receive helpers, chain file dump
  types/
    block.rs                Block + Candidate definitions, PoW hashing/validation
    peer.rs                 Peer struct (ip/port)
    network_message.rs      wire message enums (req/res)
    mining_task.rs           a pending vote awaiting mining
    mining_pool.rs           per-node queue of pending mining tasks
    vote_cache.rs            voter_id -> vote map and tallies
    app_state.rs             shared node state and core protocol logic
config.toml                mining difficulty
launch_nodes.py             spins up N local nodes (1 root + N-1 peers)
vote.py                     casts random votes against a running node's HTTP API
tally.py                    verifies cast votes and compares tallies against the chain
```

## Building

Requires a Rust toolchain (edition 2024).

```bash
cargo build --release
```

## Running a network locally

Start a root node (no arguments beyond a node id runs it as root, launching the HTTP server on
port 8000):

```bash
cargo run -- node0
```

The node prints the TCP address it bound for peer traffic, e.g. `IP: 127.0.0.1, Port: 54321`.
Start additional peers pointing at that address, passing the root's node id as the fourth
argument:

```bash
cargo run -- node1 127.0.0.1 54321 node0
```

Or launch a whole local cluster at once (spins up 1 root + N-1 peers, wiring the root id/IP/port
through automatically):

```bash
python3 launch_nodes.py 5
```

`launch_nodes.py` options:

| Flag | Description |
|------|-------------|
| `n` (positional) | Total number of nodes to launch, including the root |
| `--node-prefix` | Node name prefix, e.g. `node` for `node0`, `node1`, ... (default: `node`) |
| `--delay-min` / `--delay-max` | Random delay range (seconds) between launching successive peers |
| `--connect-ip` | IP peers use to reach the root node (default: `0.0.0.0`) |
| `-b`, `--binary` | Run the compiled `target/release/bk-rs` binary instead of `cargo run` (requires `cargo build --release` first) |

## Casting and tallying votes

With a root node running its HTTP server (default `localhost:8000`):

```bash
python3 vote.py 20                 # cast 20 random votes, recording them to votes_record.jsonl
python3 tally.py --timeout 30      # wait for propagation, verify each vote, compare tallies
```

### HTTP API (root peer)

| Method | Path                    | Description                                  |
|--------|-------------------------|-----------------------------------------------|
| POST   | `/add_vote/<voter_id>/<vote>` | Cast a vote (`vote` is one of `A`-`F`)  |
| GET    | `/vote/<voter_id>`      | Look up a voter's recorded vote (`NOT_FOUND` if absent) |
| GET    | `/votes/total`          | Total number of recorded votes                |
| GET    | `/votes/tally`          | Comma-separated `candidate:count` tally       |
