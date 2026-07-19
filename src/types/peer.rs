use std::{net::IpAddr, sync::Arc};

use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
};

use crate::{
    types::{
        block::{Block, ValidateChainRes, validate_chain, validate_chain_addition},
        network_message::{NetworkMessageReq, NetworkMessageRes, SyncResMessage},
        peer_serializable::PeerSerializable,
    },
    utils::{NetError, open_stream, send_packet_and_wait, timeout},
};

#[derive(Debug)]
pub struct Peer {
    pub ip: IpAddr,
    pub port: u16,
    // TODO use peer id instead?
    pub drop_signal_sender: mpsc::Sender<(PeerSerializable, String)>,
    pub sync_handle: Option<JoinHandle<()>>,
    pub chain: Arc<Mutex<Vec<Block>>>,
}

impl Peer {
    pub fn new(
        ip: IpAddr,
        port: u16,
        drop_signal_sender: mpsc::Sender<(PeerSerializable, String)>,
        chain: Arc<Mutex<Vec<Block>>>,
    ) -> Self {
        let obj = Self {
            ip,
            port,
            drop_signal_sender,
            sync_handle: None,
            chain,
        };
        return obj;
    }
    pub fn from_serializable(
        serializable: PeerSerializable,
        drop_signal_sender: mpsc::Sender<(PeerSerializable, String)>,
        chain: Arc<Mutex<Vec<Block>>>,
    ) -> Self {
        Self {
            ip: serializable.ip,
            port: serializable.port,
            drop_signal_sender,
            sync_handle: None,
            chain,
        }
    }

    pub fn init_sync(&mut self, known_peers: Arc<Mutex<Vec<Self>>>, chain: Arc<Mutex<Vec<Block>>>) {
        if self.sync_handle.is_some() {
            return;
        }
        let drop_signal_sender = self.drop_signal_sender.clone();
        let self_serialized = PeerSerializable {
            ip: self.ip,
            port: self.port,
        };
        let handle = tokio::spawn(async move {
            loop {
                timeout().await;
                let r = sync(
                    &self_serialized,
                    drop_signal_sender.clone(),
                    chain.clone(),
                    known_peers.clone(),
                )
                .await;
                match r {
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        });
        self.sync_handle = Some(handle);
    }
}

impl Drop for Peer {
    // When dropped cancel the heartbeat
    fn drop(&mut self) {
        println!("Executing drop");
        match &self.sync_handle {
            Some(h) => {
                println!("{} {}", self.ip, self.port);
                h.abort();
            }
            None => {}
        };
    }
}

impl Into<PeerSerializable> for &Peer {
    fn into(self) -> PeerSerializable {
        return PeerSerializable {
            ip: self.ip,
            port: self.port,
        };
    }
}

impl PartialEq<PeerSerializable> for Peer {
    fn eq(&self, other: &PeerSerializable) -> bool {
        return self.ip == other.ip && self.port == other.port;
    }
}

async fn sync(
    remote_peer: &PeerSerializable,
    drop_signal_sender: mpsc::Sender<(PeerSerializable, String)>,
    chain: Arc<Mutex<Vec<Block>>>,
    known_peers: Arc<Mutex<Vec<Peer>>>,
) -> Result<(), ()> {
    let stream = open_stream(remote_peer).await;
    if stream.is_err() {
        drop_signal_sender
            .clone()
            .send((remote_peer.clone(), "Failed to open stream".into()))
            .await
            .expect("Failed to send drop signal");
    }
    let mut stream = stream.unwrap();

    println!(
        "Sending sync message to {} {}",
        remote_peer.ip, remote_peer.port
    );

    let message = send_packet_and_wait(&mut stream, NetworkMessageReq::SyncReq).await;

    match message {
        Ok(res) => match res {
            NetworkMessageRes::SyncRes(sync_res_message) => {
                let SyncResMessage { last_block, peers } = sync_res_message;
                let mut chain_lock = chain.lock().await;

                let known_peers_for_init = known_peers.clone();
                let mut known_peers_lock = known_peers.lock().await;
                for peer in peers {
                    if peer != *remote_peer && !peer_exists(&known_peers_lock, &peer) {
                        let mut new_peer = Peer::from_serializable(
                            peer,
                            drop_signal_sender.clone(),
                            chain.clone(),
                        );
                        drop(known_peers_lock);
                        new_peer.init_sync(known_peers_for_init.clone(), chain.clone());
                        known_peers_lock = known_peers.lock().await;
                        known_peers_lock.push(new_peer);
                    }
                }

                if let Some(new_block) = last_block {
                    match validate_chain_addition(&chain_lock, &new_block) {
                        ValidateChainRes::RequestFullChain => {
                            return request_full_chain(remote_peer, &mut chain_lock).await;
                        }
                        ValidateChainRes::IgnoreBlock => {
                            println!("Ignoring invalid block");
                            return Ok(());
                        }
                        ValidateChainRes::AddBlock => {
                            chain_lock.push(new_block);
                            return Ok(());
                        }
                    }
                } else {
                    return Ok(());
                }
            }
            _ => {
                println!("WRONG RESPONSE TYPE FOR SYNC RES");
                return Err(());
            }
        },
        Err(e) => {
            println!(
                "Error occurred while waiting for Sync response: {}",
                match e {
                    NetError::IoError(error) => error.to_string(),
                    NetError::Timeout => "Timed out while waiting for Sync response".into(),
                }
            );
            return Err(());
        }
    }
}

async fn request_full_chain(
    peer: &PeerSerializable,
    current_chain: &mut Vec<Block>,
) -> Result<(), ()> {
    let mut stream = open_stream(peer).await.unwrap();
    let res = send_packet_and_wait(&mut stream, NetworkMessageReq::FullChainReq).await;
    match res {
        Ok(res) => match res {
            NetworkMessageRes::FullChainRes(new_blocks) => {
                let is_chain_valid = validate_chain(&new_blocks, current_chain);
                if is_chain_valid {
                    current_chain.clear();
                    current_chain.extend(new_blocks);
                    return Ok(());
                } else {
                    return Err(());
                }
            }
            _ => {
                println!("Invalid response type for full chain request");
                return Err(());
            }
        },
        Err(err) => {
            println!(
                "Error occurred while waiting for full chain response: {}",
                match err {
                    NetError::IoError(error) => error.to_string(),
                    NetError::Timeout => "Timed out while waiting for full chain response".into(),
                }
            );
            return Err(());
        }
    }
}

pub fn peer_exists(known_peers: &[Peer], peer: &PeerSerializable) -> bool {
    known_peers.iter().any(|p| p == peer)
}
