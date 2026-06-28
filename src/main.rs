use std::{
    env,
    io::Error,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    select,
    sync::{Mutex, mpsc},
    task::JoinHandle,
    time::Sleep,
};
use wincode::{SchemaRead, SchemaWrite};

mod block;

#[derive(Debug)]
struct Peer {
    ip: IpAddr,
    port: u16,
    // TODO use peer id instead?
    drop_signal_sender: mpsc::Sender<(PeerSerializable, String)>,
    heartbeat_handle: Option<JoinHandle<()>>,
}

impl Peer {
    fn new(
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
    fn from_serializable(
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
    fn init_heartbeat(&mut self) {
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
impl Into<PeerSerializable> for Peer {
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
struct PeerSerializable {
    ip: IpAddr,
    port: u16,
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
enum NetworkMessage {
    PeerDiscoveryReq(PeerSerializable),
    PeerDiscoveryRes(Vec<PeerSerializable>),
    HeartbeatReq,
    HeartbeatRes,
}

#[tokio::main]
async fn main() {
    let mut args = env::args();
    let mut root_peer = None;
    let (peer_drop_signal_sender, mut peer_drop_signal_receiver) =
        mpsc::channel::<(PeerSerializable, String)>(5);
    if args.len() == 3 {
        let root_ip = args.nth(1).unwrap();
        let root_port = args.nth(0).unwrap().parse::<u16>().unwrap();
        let p = Peer::new(
            root_ip.parse().unwrap(),
            root_port,
            peer_drop_signal_sender.clone(),
        );
        root_peer = Some(p);
    }

    let sock = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let local_addr = sock.local_addr().unwrap();
    let ip = local_addr.ip();
    let ip_string = ip.to_string();
    let port = local_addr.port();
    println!("IP: {}, Port: {}", ip_string, port);
    let me = Peer::new(ip, port, peer_drop_signal_sender.clone());
    let known_peers = Arc::new(Mutex::new(vec![me]));
    let self_serialized = PeerSerializable{ ip, port };
    if root_peer.is_some() {
        let root_peer = root_peer.unwrap();
        let root_peer_serialized = root_peer.into();
        let stream = open_stream(&root_peer_serialized).await;
        let known_peers = known_peers.clone();
        if stream.is_ok() {
            let mut stream = stream.unwrap();
            let res = send_packet_and_wait(&mut stream, NetworkMessage::PeerDiscoveryReq(self_serialized)).await;
            match res {
                Ok(msg) => match msg {
                    NetworkMessage::PeerDiscoveryRes(peers) => {
                        let mut known_peers = known_peers.lock().await;
                        for peer in peers {
                            if !peer_exists(&known_peers, &peer) {
                                let mut new_peer: Peer =
                                    Peer::from_serializable(peer, peer_drop_signal_sender.clone());
                                new_peer.init_heartbeat();
                                known_peers.push(new_peer);
                            }
                        }
                    }
                    _ => {
                        panic!("UNSUPPORTED RESPONSE FOR PEER DISCOVERY")
                    }
                },
                Err(_) => panic!("Could not connect with root peer"),
            }
        } else {
            panic!("Could not connect with root peer")
        }
    }
    let known_peers_c = known_peers.clone();
    tokio::spawn(async move {
        let known_peers = known_peers.clone();
        loop {
            let known_peers = known_peers.clone();
            let s = peer_drop_signal_receiver
                .recv()
                .await
                .expect("Error listening to drop signals");
            println!("Received drop signal for peer: {:#?}, reason: {}", s.0, s.1);
            {
                let mut known_peers = known_peers.lock().await;
                known_peers.retain(|p| *p != s.0);
            }
        }
    });

    let mut buff = [0; 1024];
    let known_peers_c = known_peers_c.clone();
    loop {
        let (mut stream, peer_addr) = sock.accept().await.unwrap();
        let known_peers_c = known_peers_c.clone();
        let peer_drop_signal_sender = peer_drop_signal_sender.clone();
        tokio::spawn(async move {
            stream.readable().await.unwrap();
            stream.read(&mut buff).await.unwrap();
            let req: NetworkMessage =
                wincode::deserialize(&buff).expect("Error while deserializing peer address");
            match req {
                NetworkMessage::PeerDiscoveryReq(peer_serializable) => {
                    let mut known_peers = known_peers_c.lock().await;
                    let peer = PeerSerializable::from(peer_serializable);
                    if !peer_exists(&known_peers, &(peer).into()) {
                        let mut new_peer: Peer =
                            Peer::from_serializable(peer, peer_drop_signal_sender.clone());
                        new_peer.init_heartbeat();
                        known_peers.push(new_peer);
                    }

                    let sv = known_peers
                        .iter()
                        .map(Into::<PeerSerializable>::into)
                        .collect();
                    let _m = send_packet(&mut stream, NetworkMessage::PeerDiscoveryRes(sv)).await;
                    println!("Discovery response sent");
                }
                NetworkMessage::PeerDiscoveryRes(_) => {}
                NetworkMessage::HeartbeatReq => {
                    let peer = PeerSerializable::from(peer_addr);
                    println!("Received Heartbeat req from {:#?}", peer,);
                    let _m = send_packet(&mut stream, NetworkMessage::HeartbeatRes).await;
                }
                NetworkMessage::HeartbeatRes => {}
            }
        });
    }
}

fn timeout() -> Sleep {
    return tokio::time::sleep(Duration::new(5, 0));
}

async fn open_stream(remote_peer: &PeerSerializable) -> Result<TcpStream, Error> {
    let stream = TcpStream::connect(SocketAddr::new(remote_peer.ip, remote_peer.port)).await?;
    return Ok(stream);
}

async fn send_packet(
    // remote_peer: &PeerSerializable,
    stream: &mut TcpStream,
    packet: NetworkMessage,
) -> Result<usize, Error> {
    // let mut stream = TcpStream::connect(SocketAddr::new(remote_peer.ip, remote_peer.port)).await?;
    let message_bytes = wincode::serialize(&packet).expect("Error serializing packet");
    let p = stream.write(&message_bytes).await;
    return p;
}

enum NetError {
    #[allow(dead_code)]
    IoError(std::io::Error),
    Timeout,
}

impl From<std::io::Error> for NetError {
    fn from(value: std::io::Error) -> Self {
        return NetError::IoError(value);
    }
}

async fn send_packet_and_wait(
    stream: &mut TcpStream,
    packet: NetworkMessage,
) -> Result<NetworkMessage, NetError> {
    let _sent = send_packet(stream, packet).await?;

    select! {
        _ = stream.readable() => {
            let mut buff = [0;1024];
            stream.read(&mut buff).await?;
            let message = wincode::deserialize::<NetworkMessage>(&buff).expect("Error deserializing message");
            return Ok(message);
        }
        _ = timeout() => {
            return Err(NetError::Timeout);
        }
    }
}

fn peer_exists(known_peers: &[Peer], peer: &PeerSerializable) -> bool {
    known_peers.iter().any(|p| p == peer)
}
