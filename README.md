# bk-rs

A blockchain-based voting system in Rust. Votes are recorded as proof-of-work-mined blocks
gossiped across a peer-to-peer network of nodes, with one node exposing an HTTP API for casting
votes and reading results.

## How it works

- **Nodes** communicate over raw TCP using length-delimited, `wincode`-serialized messages
  (`src/net/network_message.rs`). Each node has a string **peer id** (`--node-id`, e.g. `node0`)
  used to identify it in every other node's peer table, separate from its `Peer` (IP + port). A
  node either starts as the **root** (no `--root-ip`/`--root-port` given) or joins an existing
  network by connecting to a root peer's IP/port/id.
- **Peer discovery**: a joining node sends a `PeerDiscoveryReq` (its own `Peer` + peer id) to the
  root peer and gets back the root's current peer table (`Vec<(Peer, id)>`), then gossips its own
  presence to the network with `PushPeersReq` (`App::push_peer_sync`).
- **Voting**: the root peer runs a [Rocket](https://rocket.rs/) HTTP server
  (`src/http_server/http.rs`) with endpoints to submit a vote, look up a vote, and get
  totals/tallies. Casting a vote creates a `MiningTask` that is fanned out (`DistributeMiningTask`)
  to a subset of known peers to mine (`fan_out = ceil(known_peers * 0.4)`, at least one peer).
- **Mining**: a block is valid once its SHA-256 hash has a configured number of leading zero bits
  (`DIFFICULTY` in `src/block/block.rs`). Blocks link to the previous
  block's hash and record the voter's UUID and chosen candidate, so each voter can only be counted
  once — duplicate mining tasks for a voter already seen or already queued are dropped
  (`App::add_mining_task`, `MiningPool::add_task`). Mining runs one block at a time per node
  (`App::mining_lock`), racing against blocks mined elsewhere; a node that loses the race requeues
  its task against the new chain tip (`ValidateChainRes::AttemptedLateAdd`).
- **Chain sync and gossip flood prevention**: nodes validate and adopt the longest/newer valid
  chain seen from peers (`App::sync_chain`), rebuilding an in-memory vote cache
  (`src/http_server/vote_cache.rs`) for O(1) vote lookups and tallying instead of rescanning the
  chain. Each node's `Chain::hash()` (an MD5 digest of the serialized chain, distinct from each
  block's own SHA-256 PoW hash) is tracked per known peer in `KnownPeers::id_map`. When a chain
  update is accepted, it's only re-pushed to peers whose tracked hash doesn't already match — plus
  whichever peer ids the sender already forwarded to (`PushChainReq`'s `already_sent` list),
  passed along and merged at each hop. This "already sent" set stops the same chain from being
  re-gossiped endlessly around the peer graph.

## Project layout

```
src/
  main.rs                       CLI arg parsing (clap), node startup, TCP listener loop
  app.rs                        shared node state (App): peers, chain, mining, gossip/sync logic
  block/
    block.rs                    Block + Candidate definitions, PoW hashing/validation
    chain.rs                    Chain (Vec<Block>) validation, extension, and hashing
    miner.rs                    MiningTask, block mining entry point
    mining_pool.rs               per-node queue of pending mining tasks, deduped by voter_id
  peer/
    peer.rs                     Peer struct (ip/port)
    known_peers.rs               peer id -> (Peer, last known chain hash) table
  net/
    network_message.rs           wire message enums (req/res)
    utils.rs                     TCP send/receive helpers, chain-to-file dump
  http_server/
    http.rs                      Rocket HTTP API (root peer only)
    vote_cache.rs                 voter_id -> vote map and tallies, rebuilt on chain sync
config.toml                     mining difficulty
launch_nodes.py                 spins up N local nodes (1 root + N-1 peers, or all peers with --no-root)
vote.py                         casts random votes against a running node's HTTP API
tally.py                        verifies cast votes and compares tallies against the chain
```

## Building

Requires a Rust toolchain (edition 2024).

```bash
cargo build --release
```

## Running a network directly with `cargo run` / the built binary

`src/main.rs` takes these flags (defaults shown are what the Rust binary itself falls back to):

| Flag | Default | Description |
|------|---------|-------------|
| `--node-id` | `node_0` | Identifier for this node |
| `--root-ip` | *(none)* | IP of the root node to join; omit to run as root |
| `--root-port` | *(none)* | Port of the root node to join; omit to run as root |
| `--root-id` | `node_0` | Identifier of the root node to join |
| `--public-ip` | `0.0.0.0` | Public IP address for this node |
| `--public-port` | `4567` | TCP port for peer traffic |

A node only attempts to join an existing network when `--root-ip` and `--root-port` are both
given; otherwise it starts as root and launches the Rocket HTTP server on port 8000.

Start a root node:

```bash
cargo run -- --node-id node0
```

The node prints the TCP address it bound for peer traffic, e.g. `IP: 127.0.0.1, Port: 54321`.
Start additional peers pointing at that address:

```bash
cargo run -- --node-id node1 --root-ip 127.0.0.1 --root-port 54321 --root-id node0
```

Or with the compiled release binary:

```bash
./target/release/bk-rs --node-id node1 --root-ip 127.0.0.1 --root-port 54321 --root-id node0
```

## Running a network with the Python helper

`launch_nodes.py` wraps the above, wiring the root id/IP/port through automatically:

```bash
python3 launch_nodes.py 5
```

`launch_nodes.py` options:

| Flag | Description |
|------|-------------|
| `n` (positional) | Total number of nodes to launch (including root, unless `--no-root` is set) |
| `--node-prefix` | Node name prefix, e.g. `node` for `node0`, `node1`, ... (default: `node`) |
| `--connect-ip` | IP peers use to reach the root node (default: `0.0.0.0`); with `--no-root`, this is the external root's IP |
| `-p`, `--public-ip` | Public IP passed to every launched node (default: `0.0.0.0`, matching the Rust default) |
| `-b`, `--binary` | Run the compiled `target/release/bk-rs` binary instead of `cargo run` (requires `cargo build --release` first) |
| `-nr`, `--no-root` | Don't launch a local root node; instead connect all `n` launched nodes to an already-running external root (requires `--root-port` and `--root-id`, with `--connect-ip` set to the external root's IP) |
| `--root-port` | Port of the external root node to connect to (required with `--no-root`) |
| `--root-id` | Identifier of the external root node to connect to (default: `node_0`, used with `--no-root`) |

## Casting and tallying votes

With a root node running its HTTP server (default `0.0.0.0:8000`, matching the Rust node's
`--public-ip` default and Rocket's fixed port):

```bash
python3 vote.py 20                 # cast 20 random votes, recording them to votes_record.jsonl (cleared first)
python3 tally.py --timeout 30      # wait for propagation, verify each vote, compare tallies
```

Both scripts take `--root-ip` (default `0.0.0.0`) and `--root-port` (default `8000`) to point at a
root node running elsewhere — note `--root-port` here is the HTTP port, not the P2P
`--public-port` used between nodes.

### HTTP API (root peer)

| Method | Path                    | Description                                  |
|--------|-------------------------|-----------------------------------------------|
| POST   | `/add_vote/<voter_id>/<vote>` | Cast a vote (`vote` is one of `A`-`F`)  |
| GET    | `/vote/<voter_id>`      | Look up a voter's recorded vote (`NOT_FOUND` if absent) |
| GET    | `/votes/total`          | Total number of recorded votes                |
| GET    | `/votes/tally`          | Comma-separated `candidate:count` tally       |
