use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    println,
    sync::{Arc, Mutex},
};
use tokio::{io::AsyncReadExt, net::TcpStream, task::JoinSet};

use crate::{
    block::{
        block::{Block, Candidate},
        chain::{Chain, ValidateChainRes},
        miner::{MiningTask, mine_block},
        mining_pool::MiningPool,
    },
    http_server::vote_cache::VoteCache,
    net::{
        network_message::{NetworkMessageReq, NetworkMessageRes},
        utils::{
            open_stream, save_chain_to_file, send_packet_and_wait, send_packet_req, send_packet_res,
        },
    },
    peer::{
        known_peers::KnownPeers,
        peer::{Peer, peer_exists},
    },
};

pub struct App {
    self_as_peer: Peer,
    public_ip: IpAddr,
    known_peers: Arc<Mutex<KnownPeers>>,
    chain: Arc<Mutex<Chain>>,
    seen_voter_ids: Arc<Mutex<HashSet<String>>>,
    node_id: String,
    mining_pool: Arc<Mutex<MiningPool>>,
    // Held while a mining attempt is in progress so a node only ever mines one block at a time.
    // Must stay a tokio Mutex: it's held across the mining loop's .await points by design.
    mining_lock: Arc<tokio::sync::Mutex<()>>,
    votes_cache: Arc<Mutex<VoteCache>>,
}

impl App {
    pub fn init(
        public_ip: IpAddr,
        node_id: String,
        self_as_peer: Peer,
        known_peers: Arc<Mutex<KnownPeers>>,
        chain: Arc<Mutex<Chain>>,
        seen_voter_ids: Arc<Mutex<HashSet<String>>>,
    ) -> Self {
        return Self {
            public_ip,
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
        return peers.peers.clone();
    }

    pub async fn add_mining_task(&self, mining_task: MiningTask) {
        println!(
            "MINING TASK received: voter_id={} candidate={:?}",
            mining_task.voter_id, mining_task.candidate
        );
        let already_seen = self
            .seen_voter_ids
            .lock()
            .unwrap()
            .contains(&mining_task.voter_id);
        if already_seen {
            println!(
                "MINING TASK ignored: voter_id={} has already voted",
                mining_task.voter_id
            );
            return;
        }

        // a task for this voter is already queued
        let voter_id = mining_task.voter_id.clone();
        let queued = self.mining_pool.lock().unwrap().add_task(mining_task);
        if !queued {
            println!(
                "MINING TASK ignored: voter_id={} is already queued",
                voter_id
            );
            return;
        }
        println!("MINING TASK queued: voter_id={}", voter_id);

        self.drive_mining_pool().await;
    }

    // Mines tasks from the mining pool one at a time. If another call is
    // already draining the pool, this returns immediately and lets that
    // call pick up the newly queued task instead.
    async fn drive_mining_pool(&self) {
        let mining_guard = match self.mining_lock.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                println!("MINING already in progress, task left in pool for that run to pick up");
                return;
            }
        };

        loop {
            let task = {
                let mut mining_pool_lock = self.mining_pool.lock().unwrap();
                mining_pool_lock.take_last()
            };
            let task = match task {
                Some(task) => task,
                None => {
                    println!("MINING POOL drained, nothing left to mine");
                    break;
                }
            };

            let already_seen = self.seen_voter_ids.lock().unwrap().contains(&task.voter_id);
            if already_seen {
                println!(
                    "MINING TASK skipped: voter_id={} has already voted",
                    task.voter_id
                );
                continue;
            }

            println!(
                "MINING started: voter_id={} candidate={:?}",
                task.voter_id, task.candidate
            );

            let (last_block, len) = {
                let chain_lock = self.chain.lock().unwrap();
                (chain_lock.last().cloned(), chain_lock.len())
            };

            let block = tokio::task::spawn_blocking(move || {
                mine_block(
                    &task,
                    last_block.as_ref(),
                    if len > 0 { len - 1 } else { 0 },
                )
            })
            .await;

            if let Ok(block) = block {
                println!(
                    "MINING finished: voter_id={} idx={} hash={}",
                    block.voter_id,
                    block.idx,
                    hex::encode(&block.hash)
                );
                let requeue_task = MiningTask {
                    voter_id: block.voter_id.clone(),
                    candidate: block.data,
                };
                if self.add_block_to_chain(block).await == ValidateChainRes::AttemptedLateAdd {
                    println!(
                        "MINING TASK requeued: voter_id={} lost the race for this chain slot",
                        requeue_task.voter_id
                    );
                    self.mining_pool.lock().unwrap().requeue(requeue_task);
                }
            }
        }

        drop(mining_guard);
    }

    pub async fn root_peer_discovery(&self, root_peer: Peer, peer_id: &str) -> Result<(), String> {
        let peers = self.get_known_peers().await;
        if !peer_exists(&peers, &root_peer) {
            self.add_peer_serializable(root_peer, peer_id.to_string())
                .await;
        }
        let stream = open_stream(&root_peer).await;
        if let Ok(mut stream) = stream {
            let res = send_packet_and_wait(
                &mut stream,
                NetworkMessageReq::PeerDiscoveryReq((
                    Peer {
                        ip: self.public_ip,
                        port: self.self_as_peer.port,
                    },
                    self.node_id.clone(),
                )),
            )
            .await;
            return match res {
                Ok(res) => match res {
                    NetworkMessageRes::PeerDiscoveryRes(res) => {
                        for (peer, id) in res {
                            self.add_peer_serializable(peer, id).await;
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
                NetworkMessageReq::PeerDiscoveryReq((peer_serializable, id)) => {
                    println!("Received peer discovery request");
                    let peer = peer_serializable;
                    let peer_added = {
                        let mut known_peers_lock = self.known_peers.lock().unwrap();
                        if peer != self.self_as_peer && !peer_exists(&known_peers_lock.peers, &peer)
                        {
                            known_peers_lock.add_peer(peer, id);
                            true
                        } else {
                            false
                        }
                    };
                    let mut entries = vec![];
                    {
                        let known_peers_lock = self.known_peers.lock().unwrap();
                        entries = known_peers_lock.as_entries();
                    }

                    let _r =
                        send_packet_res(stream, NetworkMessageRes::PeerDiscoveryRes(entries)).await;

                    self.push_peer_sync().await;
                    if peer_added {
                        self.save_chain().await;
                    }
                }
                NetworkMessageReq::PushChainReq((chain, sender_node_id, already_sent)) => {
                    println!("Received chain sync");
                    self.sync_chain(chain, sender_node_id, already_sent).await;
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

    pub async fn add_peer_serializable(&self, peer: Peer, id: String) {
        if peer == self.self_as_peer {
            return;
        }
        let added = {
            let mut known_peers_lock = self.known_peers.lock().unwrap();
            if !peer_exists(&known_peers_lock.peers, &peer) {
                known_peers_lock.add_peer(peer, id);
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

    pub async fn sync_chain(
        &self,
        new_chain: Vec<Block>,
        sender_node_id: String,
        already_sent: Vec<String>,
    ) {
        let new_len = new_chain.len();
        let outcome = {
            let mut chain_lock = self.chain.lock().unwrap();
            let mut seen_voter_ids_lock = self.seen_voter_ids.lock().unwrap();
            let mut known_peers_lock = self.known_peers.lock().unwrap();
            let validation_result = Chain::validate(&new_chain, &chain_lock);
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
                    let chain_hash = chain_lock.hash();
                    known_peers_lock.update_chain_hash(&sender_node_id, chain_hash.clone());
                    Ok((current_len, votes_cache, chain_hash))
                }
                Err(reason) => Err((current_len, reason)),
            }
        };
        match outcome {
            Ok((current_len, votes_cache, chain_hash)) => {
                *self.votes_cache.lock().unwrap() = votes_cache;
                println!(
                    "ACCEPTED chain from peer: {} blocks (previous: {} blocks)",
                    new_len, current_len
                );
                let mut peers_to_ignore = vec![];
                {
                    let known_peers_lock = self.known_peers.lock().unwrap();
                    let known_peers = known_peers_lock.as_entries_with_hash();
                    peers_to_ignore = known_peers
                        .iter()
                        .filter(|(_, _, hash)| *hash == chain_hash)
                        .map(|(_, id, _)| id.clone())
                        .collect();
                }
                {
                    let mut known_peers_lock = self.known_peers.lock().unwrap();
                    known_peers_lock.update_chain_hash_all(chain_hash);
                }
                peers_to_ignore.extend(already_sent);
                self.push_chain_sync(&peers_to_ignore).await;
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
            let result = Chain::validate_addition(&seen_voter_ids, &chain_lock, &block);
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
            self.votes_cache
                .lock()
                .unwrap()
                .record_vote(voter_id, candidate);
            // When new block is mined no peer has it so we can ignore this parameter
            self.push_chain_sync(&vec![]).await;
            self.save_chain().await;
        }
        result
    }

    // Peer, Id
    async fn add_multiple_peer(&self, peers: &[(Peer, String)]) {
        let grew = {
            let mut known_peers_lock = self.known_peers.lock().unwrap();
            let old_len = known_peers_lock.peers.len();
            for (peer, id) in peers {
                if *peer != self.self_as_peer && !peer_exists(&known_peers_lock.peers, &peer) {
                    known_peers_lock.add_peer(*peer, id.clone());
                }
            }
            known_peers_lock.peers.len() > old_len
        };
        if grew {
            self.push_peer_sync().await;
            self.save_chain().await;
        }
    }

    async fn push_peer_sync(&self) {
        let mut join_set = JoinSet::new();
        {
            let known_peers_lock = self.known_peers.lock().unwrap();
            let peer_entries = known_peers_lock.as_entries();
            let len = peer_entries.len();
            drop(known_peers_lock);
            let peer_entries = peer_entries.clone();
            for i in 0..len {
                let (peer, _id) = peer_entries[i].clone();
                let peer_entries = peer_entries.clone();
                join_set.spawn(async move {
                    let stream = open_stream(&peer).await;
                    if let Ok(mut stream) = stream {
                        let r = send_packet_req(
                            &mut stream,
                            NetworkMessageReq::PushPeersReq(peer_entries),
                        )
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
                if known_peers_lock.remove_peer_with_peer(&peer) {
                    peer_removed = true;
                }
            }
            peer_removed
        };
        if peer_removed {
            self.save_chain().await;
        }
    }

    async fn push_chain_sync(&self, ignore_peers: &[String]) {
        println!("PUSHING CHAIN SYNC");
        let mut join_set = JoinSet::new();
        {
            let known_peers_lock = self.known_peers.lock().unwrap();
            let chain_lock = self.chain.lock().unwrap();
            let peers_list = known_peers_lock
                .as_entries()
                .iter()
                .filter(|(_, id)| !ignore_peers.contains(id))
                .map(|v| v.clone())
                .collect::<Vec<(Peer, String)>>();
            let chain: Vec<Block> = chain_lock.to_vec();
            let len = peers_list.len();
            let sent_ids = peers_list
                .clone()
                .into_iter()
                .map(|v| v.1)
                .collect::<Vec<String>>();
            let peers_list = peers_list
                .clone()
                .into_iter()
                .map(|v| v.0)
                .collect::<Vec<Peer>>();
            drop(known_peers_lock);
            drop(chain_lock);
            let node_id = self.node_id.clone();
            for i in 0..len {
                let peer = peers_list[i].clone();
                let chain = chain.clone();
                let node_id = node_id.clone();
                let sent_ids = sent_ids.clone();
                join_set.spawn(async move {
                    let stream = open_stream(&peer).await;
                    if let Ok(mut stream) = stream {
                        let r = send_packet_req(
                            &mut stream,
                            NetworkMessageReq::PushChainReq((chain, node_id, sent_ids.clone())),
                        )
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
                if known_peers_lock.remove_peer_with_peer(&peer) {
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
            &known_peers_lock.peers,
        );
        let known_peer_addresses = known_peers_lock
            .peers
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
