use std::{
    collections::{HashMap, HashSet},
    println,
    sync::{Arc, Mutex},
};
use tokio::{io::AsyncReadExt, net::TcpStream, task::JoinSet};

use crate::{
    randomizer::mine_block,
    types::{
        block::{Block, Candidate},
        mining_pool::MiningPool,
        mining_task::MiningTask,
        network_message::{NetworkMessageReq, NetworkMessageRes},
        peer::{Peer, peer_exists},
        vote_cache::VoteCache,
    },
    utils::{
        open_stream, save_chain_to_file, send_packet_and_wait, send_packet_req, send_packet_res,
    },
};

pub struct AppState {
    self_as_peer: Peer,
    known_peers: Arc<Mutex<Vec<Peer>>>,
    chain: Arc<Mutex<Vec<Block>>>,
    seen_voter_ids: Arc<Mutex<HashSet<String>>>,
    node_id: String,
    mining_pool: Arc<Mutex<MiningPool>>,
    // Held while a mining attempt is in progress so a node only ever mines one block at a time.
    // Must stay a tokio Mutex: it's held across the mining loop's .await points by design.
    mining_lock: Arc<tokio::sync::Mutex<()>>,
    votes_cache: Arc<Mutex<VoteCache>>,
}

impl AppState {
    pub fn init(
        node_id: String,
        self_as_peer: Peer,
        known_peers: Arc<Mutex<Vec<Peer>>>,
        chain: Arc<Mutex<Vec<Block>>>,
        seen_voter_ids: Arc<Mutex<HashSet<String>>>,
    ) -> Self {
        return Self {
            self_as_peer,
            known_peers,
            chain,
            node_id,
            seen_voter_ids,
            mining_pool: Arc::new(Mutex::new(MiningPool::new())),
            mining_lock: Arc::new(tokio::sync::Mutex::new(())),
            votes_cache: Arc::new(Mutex::new(VoteCache::new())),
        };
    }

    pub async fn get_vote(&self, voter_id: &str) -> Option<Candidate> {
        self.votes_cache.lock().unwrap().get_vote(voter_id)
    }

    pub async fn get_tally(&self) -> HashMap<Candidate, usize> {
        self.votes_cache.lock().unwrap().tally()
    }

    pub async fn get_total_votes(&self) -> usize {
        self.votes_cache.lock().unwrap().total_votes()
    }

    pub async fn get_known_peers(&self) -> Vec<Peer> {
        let peers = self.known_peers.lock().unwrap();
        return peers.clone();
    }

    pub async fn add_mining_task(&self, mining_task: MiningTask) {
        let already_seen = self
            .seen_voter_ids
            .lock()
            .unwrap()
            .contains(&mining_task.voter_id);
        if already_seen {
            return;
        }

        // a task for this voter is already queued
        let queued = self.mining_pool.lock().unwrap().add_task(mining_task);
        if !queued {
            return;
        }

        self.drive_mining_pool().await;
    }

    // Mines tasks from the mining pool one at a time. If another call is
    // already draining the pool, this returns immediately and lets that
    // call pick up the newly queued task instead.
    async fn drive_mining_pool(&self) {
        let mining_guard = match self.mining_lock.try_lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };

        loop {
            let task = {
                let mut mining_pool_lock = self.mining_pool.lock().unwrap();
                mining_pool_lock.take_last()
            };
            let task = match task {
                Some(task) => task,
                None => break,
            };

            let already_seen = self.seen_voter_ids.lock().unwrap().contains(&task.voter_id);
            if already_seen {
                continue;
            }

            let (last_block, len) = {
                let chain_lock = self.chain.lock().unwrap();
                (chain_lock.clone().into_iter().last(), chain_lock.len())
            };

            let block = tokio::task::spawn_blocking(move || {
                mine_block(&task, last_block.as_ref(), if len > 0 { len - 1 } else { 0 })
            })
            .await;

            if let Ok(block) = block {
                let requeue_task = MiningTask {
                    voter_id: block.voter_id.clone(),
                    candidate: block.data,
                };
                if self.add_block_to_chain(block).await == ValidateChainRes::AttemptedLateAdd {
                    self.mining_pool.lock().unwrap().requeue(requeue_task);
                }
            }
        }

        drop(mining_guard);
    }

    pub async fn root_peer_discovery(&self, root_peer: Peer) -> Result<(), String> {
        let peers = self.get_known_peers().await;
        if !peer_exists(&peers, &root_peer) {
            self.add_peer_serializable(root_peer).await;
        }
        let stream = open_stream(&root_peer).await;
        if let Ok(mut stream) = stream {
            let res = send_packet_and_wait(
                &mut stream,
                NetworkMessageReq::PeerDiscoveryReq(self.self_as_peer),
            )
            .await;
            return match res {
                Ok(res) => match res {
                    NetworkMessageRes::PeerDiscoveryRes(res) => {
                        for peer in res {
                            self.add_peer_serializable(peer).await;
                        }
                        return Ok(());
                    }
                    _ => Err("Peer discovery response invalid".to_string()),
                },
                Err(e) => Err(format!(
                    "Error while waiting for peer discovery response: {}",
                    e.to_string()
                )),
            };
        };
        return Err("Could not open stream to root peer".to_string());
    }

    pub async fn server_process(&self, stream: &mut TcpStream) {
        let mut buff = [0; 1024];
        let mut final_buff = Vec::new();
        stream.readable().await.unwrap();
        loop {
            let bytes_read = stream.read(&mut buff).await.unwrap();
            final_buff.extend_from_slice(&buff[..bytes_read]);
            if bytes_read < 1024 {
                break;
            }
            // set a max limit?
        }
        let buff = &final_buff;
        let req = wincode::deserialize::<NetworkMessageReq>(&buff);
        if let Ok(req) = req {
            match req {
                NetworkMessageReq::PeerDiscoveryReq(peer_serializable) => {
                    println!("Received peer discovery request");
                    let peer = peer_serializable;
                    let peer_added = {
                        let mut known_peers_lock = self.known_peers.lock().unwrap();
                        if peer != self.self_as_peer && !peer_exists(&known_peers_lock, &peer) {
                            known_peers_lock.push(peer);
                            true
                        } else {
                            false
                        }
                    };
                    let known_peers = self.get_known_peers().await;
                    let _r = send_packet_res(
                        stream,
                        NetworkMessageRes::PeerDiscoveryRes(known_peers),
                    )
                    .await;

                    self.push_peer_sync().await;
                    if peer_added {
                        self.save_chain().await;
                    }
                }
                NetworkMessageReq::PushChainReq(chain) => {
                    println!("Received chain sync");
                    self.sync_chain(chain).await;
                }
                NetworkMessageReq::PushPeersReq(peers) => {
                    self.add_multiple_peer(&peers).await;
                }
                NetworkMessageReq::DistributeMiningTask(mining_task) => {
                    self.add_mining_task(mining_task).await;
                }
            }
        } else {
            println!("Failed to deserialize network message");
            return;
        }
    }

    pub async fn add_peer_serializable(&self, peer: Peer) {
        if peer == self.self_as_peer {
            return;
        }
        let added = {
            let mut known_peers_lock = self.known_peers.lock().unwrap();
            if !peer_exists(&known_peers_lock, &peer) {
                known_peers_lock.push(peer);
                true
            } else {
                false
            }
        };
        if added {
            self.push_peer_sync().await;
            self.save_chain().await;
        }
    }

    pub async fn sync_chain(&self, new_chain: Vec<Block>) {
        let new_len = new_chain.len();
        let outcome = {
            let mut chain_lock = self.chain.lock().unwrap();
            let mut seen_voter_ids_lock = self.seen_voter_ids.lock().unwrap();
            let validation_result = Self::validate_chain(&new_chain, &chain_lock);
            let current_len = chain_lock.len();
            match validation_result {
                Ok(()) => {
                    chain_lock.clear();
                    seen_voter_ids_lock.clear();
                    let mut votes_cache = VoteCache::new();
                    for node in &new_chain {
                        seen_voter_ids_lock.insert(node.voter_id.clone());
                        votes_cache.record_vote(node.voter_id.clone(), node.data);
                    }
                    chain_lock.extend(new_chain);
                    Ok((current_len, votes_cache))
                }
                Err(reason) => Err((current_len, reason)),
            }
        };
        match outcome {
            Ok((current_len, votes_cache)) => {
                *self.votes_cache.lock().unwrap() = votes_cache;
                println!(
                    "ACCEPTED chain from peer: {} blocks (previous: {} blocks)",
                    new_len, current_len
                );
                self.push_chain_sync().await;
                self.save_chain().await;
            }
            Err((current_len, reason)) => {
                println!(
                    "IGNORED chain from peer: {} blocks (current: {} blocks) - reason: {}",
                    new_len, current_len, reason
                );
            }
        }
    }

    async fn add_block_to_chain(&self, block: Block) -> ValidateChainRes {
        let (result, added_vote) = {
            let mut chain_lock = self.chain.lock().unwrap();
            let mut seen_voter_ids = self.seen_voter_ids.lock().unwrap();
            let result = Self::validate_chain_addition(&seen_voter_ids, &chain_lock, &block);
            let mut added_vote = None;
            match result {
                ValidateChainRes::AddBlock => {
                    let voter_id = block.voter_id.clone();
                    let candidate = block.data;
                    chain_lock.push(block);
                    seen_voter_ids.insert(voter_id.clone());
                    added_vote = Some((voter_id, candidate));
                }
                ValidateChainRes::AttemptedLateAdd => {}
                ValidateChainRes::IgnoreBlock => {
                    println!("IGNORING INVALID BLOCK")
                }
            }
            (result, added_vote)
        };
        if let Some((voter_id, candidate)) = added_vote {
            self.votes_cache.lock().unwrap().record_vote(voter_id, candidate);
            self.push_chain_sync().await;
            self.save_chain().await;
        }
        result
    }

    async fn add_multiple_peer(&self, peers: &[Peer]) {
        let grew = {
            let mut known_peers_lock = self.known_peers.lock().unwrap();
            let old_len = known_peers_lock.len();
            for peer in peers {
                if *peer != self.self_as_peer && !peer_exists(&known_peers_lock, peer) {
                    known_peers_lock.push(*peer);
                }
            }
            known_peers_lock.len() > old_len
        };
        if grew {
            self.push_peer_sync().await;
            self.save_chain().await;
        }
    }

    fn validate_chain(new_blocks: &[Block], current_chain: &[Block]) -> Result<(), String> {
        if new_blocks.len() < current_chain.len() {
            return Err(format!(
                "new chain ({} blocks) is shorter than current chain ({} blocks)",
                new_blocks.len(),
                current_chain.len()
            ));
        }
        if new_blocks.len() == current_chain.len() {
            let new_last_timestamp = new_blocks.last().map(|b| b.timestamp);
            let current_last_timestamp = current_chain.last().map(|b| b.timestamp);
            if new_last_timestamp <= current_last_timestamp {
                return Err(format!(
                    "new chain has equal height ({} blocks) but is not newer (new timestamp: {:?}, current timestamp: {:?})",
                    new_blocks.len(),
                    new_last_timestamp,
                    current_last_timestamp
                ));
            }
        }
        let mut prev_block_hash = new_blocks[0].hash.clone();
        let mut new_blocks_voter_ids = HashSet::new();
        for i in 1..new_blocks.len() {
            if new_blocks[i].prev_hash != prev_block_hash {
                return Err(format!(
                    "block {} prev_hash does not match hash of block {}",
                    i,
                    i - 1
                ));
            }
            if !Block::validate(&new_blocks[i]) {
                return Err(format!("block {} failed validation", i));
            }
            if !new_blocks_voter_ids.contains(&new_blocks[i].voter_id) {
                new_blocks_voter_ids.insert(new_blocks[i].voter_id.clone());
                prev_block_hash = new_blocks[i].hash.clone();
            } else {
                return Err(format!(
                    "block {} has a duplicate voter_id within the new chain",
                    i
                ));
            }
        }
        Ok(())
    }

    // Assumes that the provided chain itself is valid
    fn validate_chain_addition(
        seen_voter_ids: &HashSet<String>,
        current_chain: &[Block],
        new_block: &Block,
    ) -> ValidateChainRes {
        // If already voted ignore
        if seen_voter_ids.contains(&new_block.voter_id) {
            return ValidateChainRes::IgnoreBlock;
        }

        if current_chain.is_empty() {
            // Only try to add new block if it says it is the first block on the chain
            if new_block.idx == 0 {
                if Block::validate(new_block) {
                    return ValidateChainRes::AddBlock;
                } else {
                    return ValidateChainRes::IgnoreBlock;
                }
            } else {
                return ValidateChainRes::IgnoreBlock;
            }
        }
        let last_block = &current_chain[current_chain.len() - 1];

        if new_block.idx <= last_block.idx {
            // If the block is earlier in the chain ignore the block;
            return ValidateChainRes::AttemptedLateAdd;
        }

        // if current chain has blocks only try to add new block if it says it is the next block on the chain
        if new_block.idx == last_block.idx + 1 {
            if new_block.prev_hash != last_block.hash {
                // if hash mismatch blocked
                return ValidateChainRes::IgnoreBlock;
            } else {
                if Block::validate(new_block) {
                    return ValidateChainRes::AddBlock;
                } else {
                    return ValidateChainRes::IgnoreBlock;
                }
            }
        } else {
            return ValidateChainRes::IgnoreBlock;
        }
    }

    async fn push_peer_sync(&self) {
        let mut join_set = JoinSet::new();
        {
            let known_peers_lock = self.known_peers.lock().unwrap();
            let peers_list = known_peers_lock.clone();
            let len = peers_list.len();
            drop(known_peers_lock);

            for i in 0..len {
                let peer = peers_list[i].clone();
                let list = peers_list.clone();
                join_set.spawn(async move {
                    let stream = open_stream(&peer).await;
                    if let Ok(mut stream) = stream {
                        let r = send_packet_req(&mut stream, NetworkMessageReq::PushPeersReq(list))
                            .await;
                        if let Ok(_) = r {
                            return Ok(peer);
                        } else {
                            return Err(peer);
                        }
                    }
                    return Err(peer);
                });
            }
        }
        println!("TASKS LEN, {}", join_set.len());

        let failed_peers = join_set
            .join_all()
            .await
            .iter()
            .filter(|r| r.is_err())
            .map(|r| r.unwrap_err())
            .collect::<Vec<Peer>>();

        let peer_removed = {
            let mut known_peers_lock = self.known_peers.lock().unwrap();
            let mut peer_removed = false;
            for peer in failed_peers {
                if peer_exists(&known_peers_lock, &peer) {
                    let idx = known_peers_lock.iter().position(|x| *x == peer).unwrap();
                    known_peers_lock.remove(idx);
                    peer_removed = true;
                }
            }
            peer_removed
        };
        if peer_removed {
            self.save_chain().await;
        }
    }

    async fn push_chain_sync(&self) {
        println!("PUSHING CHAIN SYNC");
        let mut join_set = JoinSet::new();
        {
            let known_peers_lock = self.known_peers.lock().unwrap();
            let chain_lock = self.chain.lock().unwrap();
            let peers_list = known_peers_lock.clone();
            let chain = chain_lock.clone();
            let len = peers_list.len();
            drop(known_peers_lock);
            drop(chain_lock);

            for i in 0..len {
                let peer = peers_list[i].clone();
                let chain = chain.clone();
                join_set.spawn(async move {
                    let stream = open_stream(&peer).await;
                    if let Ok(mut stream) = stream {
                        let r =
                            send_packet_req(&mut stream, NetworkMessageReq::PushChainReq(chain))
                                .await;
                        if let Ok(_) = r {
                            return Ok(peer);
                        } else {
                            return Err(peer);
                        }
                    }
                    return Err(peer);
                });
            }
        }
        println!("TASKS LEN, {}", join_set.len());

        let failed_peers = join_set
            .join_all()
            .await
            .iter()
            .filter(|r| r.is_err())
            .map(|r| r.unwrap_err())
            .collect::<Vec<Peer>>();

        let peer_removed = {
            let mut known_peers_lock = self.known_peers.lock().unwrap();
            let mut peer_removed = false;
            for peer in failed_peers {
                if peer_exists(&known_peers_lock, &peer) {
                    let idx = known_peers_lock.iter().position(|x| *x == peer).unwrap();
                    known_peers_lock.remove(idx);
                    peer_removed = true;
                }
            }
            peer_removed
        };
        if peer_removed {
            self.save_chain().await;
        }
    }

    pub async fn save_chain(&self) {
        let filename = format!("chain_{}.bin", self.node_id);
        let known_peers_lock = self.known_peers.lock().unwrap();
        let chain_lock = self.chain.lock().unwrap();
        let _ = save_chain_to_file(
            &chain_lock,
            &filename,
            self.self_as_peer.clone(),
            &known_peers_lock,
        );
        let known_peer_addresses = known_peers_lock
            .iter()
            .map(|peer| format!("{}:{}", peer.ip, peer.port))
            .collect::<Vec<String>>();
        drop(chain_lock);
        drop(known_peers_lock);

        let self_address = format!("{}:{}", self.self_as_peer.ip, self.self_as_peer.port);
        println!("Chain saved to {}", filename);
        println!("Self address: {}", self_address);
        if known_peer_addresses.is_empty() {
            println!("Known peers: none");
        } else {
            println!("Known peers: {}", known_peer_addresses.join(", "));
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum ValidateChainRes {
    IgnoreBlock,
    AddBlock,
    AttemptedLateAdd,
}
