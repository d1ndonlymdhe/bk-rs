use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
};
use wincode::{SchemaRead, SchemaWrite};

use crate::{
    block::Block,
    utils::{NetError, open_stream, peer_exists, send_packet_and_wait, timeout},
};

#[derive(Debug)]
pub struct Peer {
    pub ip: IpAddr,
    pub port: u16,
    // TODO use peer id instead?
    pub drop_signal_sender: mpsc::Sender<(PeerSerializable, String)>,
    pub heartbeat_handle: Option<JoinHandle<()>>,
    pub chain: Arc<Mutex<Vec<Block>>>,
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite)]
pub struct SyncResMessage {
    pub peers: Vec<PeerSerializable>,
    pub last_block: Option<Block>,
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
            heartbeat_handle: None,
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
            heartbeat_handle: None,
            chain,
        }
    }
    // Will stop automatically when dropped
    pub fn init_heartbeat(
        &mut self,
        known_peers: Arc<Mutex<Vec<Peer>>>,
        chain: Arc<Mutex<Vec<Block>>>,
    ) {
        if self.heartbeat_handle.is_some() {
            return;
        }
        let drop_signal_sender = self.drop_signal_sender.clone();
        let self_serialized = PeerSerializable {
            ip: self.ip,
            port: self.port,
        };
        let handle = tokio::spawn(async move {
            let mut counter = 1;
            loop {
                timeout().await;
                let stream = open_stream(&self_serialized).await;
                if stream.is_err() {
                    drop_signal_sender
                        .clone()
                        .send((self_serialized, "Failed to open stream".into()))
                        .await
                        .unwrap();
                    break;
                }
                let mut stream = stream.unwrap();

                println!("Sending Sync message to {}", self_serialized.port);

                let m = send_packet_and_wait(&mut stream, NetworkMessage::SyncReq).await;

                match m {
                    Ok(res) => match res {
                        NetworkMessage::SyncRes(heartbeat_message) => {
                            let SyncResMessage { last_block, peers } = heartbeat_message;
                            let mut chain_lock = chain.lock().await;
                            // let mut known_peers = known_peers.lock().await;

                            if let Some(block) = last_block {
                                if block.idx == chain_lock.len() {
                                    let current_last = chain_lock.last();
                                    if current_last.is_some() {
                                        let current_last = current_last.unwrap();
                                        // TODO: add complete validation logic
                                        if block.prev_hash == current_last.hash {
                                            chain_lock.push(block);
                                        }
                                    }
                                }
                            }
                            let known_peers_for_init = known_peers.clone();
                            let mut known_peers_lock = known_peers.lock().await;
                            for peer in peers {
                                if peer != self_serialized && !peer_exists(&known_peers_lock, &peer)
                                {
                                    let mut new_peer = Peer::from_serializable(
                                        peer,
                                        drop_signal_sender.clone(),
                                        chain.clone(),
                                    );
                                    drop(known_peers_lock);
                                    new_peer.init_heartbeat(
                                        known_peers_for_init.clone(),
                                        chain.clone(),
                                    );
                                    known_peers_lock = known_peers.lock().await;
                                    known_peers_lock.push(new_peer);
                                }
                            }

                            counter = counter + 1;
                            continue;
                        }
                        _ => {
                            panic!("UNSUPPORTED MESSAGE FORMAT")
                        }
                    },
                    Err(e) => {
                        println!(
                            "Error occurred while waiting for Sync response {}",
                            match e {
                                NetError::IoError(_) => "IO ERROR",
                                NetError::Timeout => "Timeout",
                            }
                        );
                        drop_signal_sender
                            .clone()
                            .send((self_serialized, "Failed to receive Sync response".into()))
                            .await
                            .unwrap();
                        break;
                    }
                }
            }
        });
        self.heartbeat_handle = Some(handle);
    }
}

impl Drop for Peer {
    // When dropped cancel the heartbeat
    fn drop(&mut self) {
        println!("Executing drop");
        match &self.heartbeat_handle {
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

#[derive(SchemaRead, SchemaWrite, Clone, Copy, Debug, PartialEq)]
pub struct PeerSerializable {
    pub ip: IpAddr,
    pub port: u16,
}

impl Into<SocketAddr> for PeerSerializable {
    fn into(self) -> SocketAddr {
        SocketAddr::new(self.ip, self.port)
    }
}

impl From<SocketAddr> for PeerSerializable {
    fn from(value: SocketAddr) -> Self {
        return Self {
            ip: value.ip(),
            port: value.port(),
        };
    }
}

#[derive(SchemaWrite, SchemaRead)]
pub enum NetworkMessage {
    PeerDiscoveryReq(PeerSerializable),
    PeerDiscoveryRes(Vec<PeerSerializable>),
    SyncReq,
    SyncRes(SyncResMessage),
}
