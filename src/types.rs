use std::net::{IpAddr, SocketAddr};

use tokio::{sync::mpsc, task::JoinHandle};
use wincode::{SchemaRead, SchemaWrite};

use crate::utils::{NetError, open_stream, send_packet_and_wait, timeout};

#[derive(Debug)]
pub struct Peer {
    pub ip: IpAddr,
    pub port: u16,
    // TODO use peer id instead?
    pub drop_signal_sender: mpsc::Sender<(PeerSerializable, String)>,
    pub heartbeat_handle: Option<JoinHandle<()>>,
}

impl Peer {
    pub fn new(
        ip: IpAddr,
        port: u16,
        drop_signal_sender: mpsc::Sender<(PeerSerializable, String)>,
    ) -> Self {
        let obj = Self {
            ip,
            port,
            drop_signal_sender,
            heartbeat_handle: None,
        };
        return obj;
    }
    pub fn from_serializable(
        serializable: PeerSerializable,
        drop_signal_sender: mpsc::Sender<(PeerSerializable, String)>,
    ) -> Self {
        Self {
            ip: serializable.ip,
            port: serializable.port,
            drop_signal_sender,
            heartbeat_handle: None,
        }
    }
    // Will stop automatically when dropped
    pub fn init_heartbeat(&mut self) {
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

                let m = send_packet_and_wait(&mut stream, NetworkMessage::HeartbeatReq).await;

                match m {
                    Ok(res) => match res {
                        NetworkMessage::HeartbeatRes => {
                            counter = counter + 1;
                            continue;
                        }
                        _ => {
                            panic!("UNSUPPORTED MESSAGE FORMAT")
                        }
                    },
                    Err(e) => {
                        println!(
                            "Error occurred while waiting for heartbeat response {}",
                            match e {
                                NetError::IoError(_) => "IO ERROR",
                                NetError::Timeout => "Timeout",
                            }
                        );
                        drop_signal_sender
                            .clone()
                            .send((
                                self_serialized,
                                "Failed to receive heartbeat response".into(),
                            ))
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
            Some(h) => h.abort(),
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
    HeartbeatReq,
    HeartbeatRes,
}
