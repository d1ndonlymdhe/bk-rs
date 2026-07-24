use core::panic;
use std::{println, sync::Arc};
use tokio::{io::AsyncReadExt, net::TcpStream, sync::Mutex, task::JoinSet};

use crate::{
    randomizer::mine_random_block,
    types::{
        block::Block,
        network_message::{NetworkMessageReq, NetworkMessageRes},
        peer::{Peer, peer_exists},
    },
    utils::{
        open_stream, save_chain_to_file, send_packet_and_wait, send_packet_req, send_packet_res,
    },
};

pub struct AppState {
    self_as_peer: Peer,
    known_peers: Arc<Mutex<Vec<Peer>>>,
    chain: Arc<Mutex<Vec<Block>>>,
    node_id: String,
}

impl AppState {
    pub fn init(
        node_id: String,
        self_as_peer: Peer,
        known_peers: Arc<Mutex<Vec<Peer>>>,
        chain: Arc<Mutex<Vec<Block>>>,
    ) -> Self {
        return Self {
            self_as_peer,
            known_peers,
            chain,
            node_id,
        };
    }

    pub async fn mine_random_block(&self) {
        let chain_lock = self.chain.lock().await;
        let last_block = chain_lock.clone().into_iter().last();
        let len = chain_lock.len();
        drop(chain_lock);
        let rand_block = tokio::task::spawn_blocking(move || {
            mine_random_block(last_block.as_ref(), if len > 0 { len - 1 } else { 0 })
        })
        .await;
        match rand_block {
            Ok(block) => {
                self.add_block_to_chain(block).await;
            }
            Err(_) => {
                eprintln!("Failed to mine random block");
            }
        }
    }

    pub async fn root_peer_discovery(&self, root_peer: Peer) -> Result<(), String> {
        if !peer_exists(&self.known_peers.lock().await, &root_peer) {
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
                    {
                        let mut known_peers_lock = self.known_peers.lock().await;
                        if peer != self.self_as_peer && !peer_exists(&known_peers_lock, &peer) {
                            known_peers_lock.push(peer);
                        }

                        let _r = send_packet_res(
                            stream,
                            NetworkMessageRes::PeerDiscoveryRes(known_peers_lock.clone()),
                        )
                        .await;
                    }

                    self.push_peer_sync().await;
                }
                NetworkMessageReq::PushChainReq(chain) => {
                    self.sync_chain(chain).await;
                }
                NetworkMessageReq::PushPeersReq(peers) => {
                    self.add_multiple_peer(&peers).await;
                }
            }
        } else {
            println!("Failed to deserialize network message");
            return;
        }
    }

    pub async fn add_peer_serializable(&self, peer: Peer) {
        let mut known_peers_lock = self.known_peers.lock().await;
        if !peer_exists(&known_peers_lock, &peer) {
            known_peers_lock.push(peer);
            drop(known_peers_lock);
            self.push_peer_sync().await;
        }
    }

    pub async fn sync_chain(&self, new_chain: Vec<Block>) {
        let mut chain_lock = self.chain.lock().await;
        let is_chain_valid = Self::validate_chain(&new_chain, &chain_lock);
        let mut chain_changed = false;
        if is_chain_valid {
            chain_lock.clear();
            chain_lock.extend(new_chain);
            chain_changed = true;
        }
        drop(chain_lock);
        if chain_changed {
            self.push_chain_sync().await;
        }
    }

    pub async fn add_block_to_chain(&self, block: Block) {
        let mut chain_lock = self.chain.lock().await;
        match Self::validate_chain_addition(&chain_lock, &block) {
            ValidateChainRes::AddBlock => {
                chain_lock.push(block);
                drop(chain_lock);
                self.push_chain_sync().await;
            }
            _ => {
                println!("IGNORING INVALID BLOCK")
            }
        }
    }

    async fn add_multiple_peer(&self, peers: &[Peer]) {
        let mut known_peers_lock = self.known_peers.lock().await;
        let old_len = known_peers_lock.len();
        for peer in peers {
            if *peer != self.self_as_peer && !peer_exists(&known_peers_lock, peer) {
                known_peers_lock.push(*peer);
            }
        }
        let new_len = known_peers_lock.len();
        drop(known_peers_lock);
        if new_len > old_len {
            self.push_peer_sync().await;
        }
    }

    fn validate_chain(new_blocks: &[Block], current_chain: &[Block]) -> bool {
        if new_blocks.len() <= current_chain.len() {
            return false;
        }
        let mut prev_block_hash = new_blocks[0].hash.clone();
        for i in 1..new_blocks.len() {
            if new_blocks[i].prev_hash != prev_block_hash {
                return false;
            }
            if !Block::validate(&new_blocks[i]) {
                return false;
            }
            prev_block_hash = new_blocks[i].hash.clone();
        }
        true
    }

    // Assumes that the provided chain itself is valid
    fn validate_chain_addition(current_chain: &[Block], new_block: &Block) -> ValidateChainRes {
        if current_chain.is_empty() {
            // Only try to add new block if it says it is the first block on the chain
            if new_block.idx == 0 {
                if Block::validate(new_block) {
                    return ValidateChainRes::AddBlock;
                } else {
                    return ValidateChainRes::IgnoreBlock;
                }
            } else {
                return ValidateChainRes::RequestFullChain;
            }
        }
        let last_block = &current_chain[current_chain.len() - 1];

        if new_block.idx <= last_block.idx {
            // If the block is earlier in the chain ignore the block;
            return ValidateChainRes::IgnoreBlock;
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
            return ValidateChainRes::RequestFullChain;
        }
    }

    async fn push_peer_sync(&self) {
        let mut join_set = JoinSet::new();
        {
            let known_peers_lock = self.known_peers.lock().await;
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

        let mut known_peers_lock = self.known_peers.lock().await;

        for peer in failed_peers {
            if peer_exists(&known_peers_lock, &peer) {
                let idx = known_peers_lock.iter().position(|x| *x == peer).unwrap();
                known_peers_lock.remove(idx);
            }
        }
    }

    async fn push_chain_sync(&self) {
        let mut join_set = JoinSet::new();
        {
            let known_peers_lock = self.known_peers.lock().await;
            let chain_lock = self.chain.lock().await;
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

        let mut known_peers_lock = self.known_peers.lock().await;

        for peer in failed_peers {
            if peer_exists(&known_peers_lock, &peer) {
                let idx = known_peers_lock.iter().position(|x| *x == peer).unwrap();
                known_peers_lock.remove(idx);
            }
        }
    }

    pub async fn save_chain(&self) {
        let filename = format!("chain_{}.bin", self.node_id);
        let chain_lock = self.chain.lock().await;
        let _ = save_chain_to_file(
            &chain_lock,
            &filename,
            self.self_as_peer.clone(),
            &self.known_peers.lock().await,
        );
        drop(chain_lock);

        let self_address = format!("{}:{}", self.self_as_peer.ip, self.self_as_peer.port);
        let known_peers_lock = self.known_peers.lock().await;
        let known_peer_addresses = known_peers_lock
            .iter()
            .map(|peer| format!("{}:{}", peer.ip, peer.port))
            .collect::<Vec<String>>();
        drop(known_peers_lock);

        println!("Chain saved to {}", filename);
        println!("Self address: {}", self_address);
        if known_peer_addresses.is_empty() {
            println!("Known peers: none");
        } else {
            println!("Known peers: {}", known_peer_addresses.join(", "));
        }
    }
}

enum ValidateChainRes {
    RequestFullChain,
    IgnoreBlock,
    AddBlock,
}
