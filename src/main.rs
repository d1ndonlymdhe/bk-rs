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
};
use tokio_util::sync::CancellationToken;
use wincode::{SchemaRead, SchemaWrite};

mod block;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct HeartbeatState {
    count: u32,
    res: u32,
}

#[derive(Debug)]
struct Peer {
    ip: IpAddr,
    port: u16,
    heartbeat_cancel_token: Option<CancellationToken>,
    heartbeat_state: Arc<Mutex<HeartbeatState>>,
    // TODO use peer id instead?
    drop_signal_sender: mpsc::Sender<PeerSerializable>,
}

impl Peer {
    fn new(ip: IpAddr, port: u16, drop_signal_sender: mpsc::Sender<PeerSerializable>) -> Self {
        let obj = Self {
            ip,
            port,
            heartbeat_cancel_token: None,
            heartbeat_state: Arc::new(Mutex::new(HeartbeatState::default())),
            drop_signal_sender,
        };
        return obj;
    }
    fn from_serializable(
        serializable: PeerSerializable,
        drop_signal_sender: mpsc::Sender<PeerSerializable>,
    ) -> Self {
        Self {
            ip: serializable.ip,
            port: serializable.port,
            heartbeat_cancel_token: None,
            heartbeat_state: Arc::new(Mutex::new(HeartbeatState::default())),
            drop_signal_sender,
        }
    }
    // Will stop automatically when dropped
    fn init_heartbeat(&mut self, self_peer: PeerSerializable) {
        if self.heartbeat_cancel_token.is_some() {
            return;
        }
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        let serialized = PeerSerializable {
            ip: self.ip,
            port: self.port,
        };
        let token = CancellationToken::new();
        let cloned_token = token.clone();

        let heartbeat_state = self.heartbeat_state.clone();
        let drop_signal_sender = self.drop_signal_sender.clone();
        let _jh = tokio::spawn(async move {
            loop {
                select! {
                    _ = async
                    {
                        {interval.tick().await;}
                        } => {
                            let mut heartbeat_state = heartbeat_state.lock().await;
                            if heartbeat_state.count == heartbeat_state.res{
                                let m = send_packet(&serialized, NetworkMessage::HeartbeatReq((self_peer,heartbeat_state.count))).await;
                                if m.is_err(){
                                    drop_signal_sender.clone().send(serialized).await.unwrap();
                                }
                                heartbeat_state.count = heartbeat_state.count + 1;
                            }else{
                                drop_signal_sender.send(serialized).await.expect("Error sending drop signal");
                            }
                        }
                    _ = cloned_token.cancelled() => {break}
                }
            }
        });
        self.heartbeat_cancel_token = Some(token);
    }
}

impl Drop for Peer {
    // When dropped cancel the heartbeat
    fn drop(&mut self) {
        println!("Executing drop");
        match &self.heartbeat_cancel_token {
            Some(t) => t.cancel(),
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

#[derive(SchemaWrite, SchemaRead)]
enum NetworkMessage {
    PeerDiscoveryReq(PeerSerializable),
    PeerDiscoveryRes(Vec<PeerSerializable>),
    HeartbeatReq((PeerSerializable, u32)),
    HeartbeatRes((PeerSerializable, u32)),
}

#[tokio::main]
async fn main() {
    let mut args = env::args();
    let mut root_peer = None;
    let (peer_drop_signal_sender, mut peer_drop_signal_receiver) =
        mpsc::channel::<PeerSerializable>(5);
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
    let me_serialized = PeerSerializable { ip, port };
    let known_peers = Arc::new(Mutex::new(vec![me]));

    if root_peer.is_some() {
        let root_peer = root_peer.unwrap();
        let root_peer_serialized = root_peer.into();
        send_packet(
            &root_peer_serialized,
            NetworkMessage::PeerDiscoveryReq(me_serialized),
        )
        .await
        .expect("Error in root peer discovery");
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
            println!("Received drop signal for peer: {:#?}", s);
            {
                let mut known_peers = known_peers.lock().await;
                known_peers.retain(|p| *p != s);
            }
        }
    });

    let mut buff = [0; 1024];
    let known_peers_c = known_peers_c.clone();
    loop {
        let (mut stream, _addr) = sock.accept().await.unwrap();
        let known_peers_c = known_peers_c.clone();
        let peer_drop_signal_sender = peer_drop_signal_sender.clone();
        tokio::spawn(async move {
            stream.readable().await.unwrap();
            stream.read(&mut buff).await.unwrap();
            let req: NetworkMessage =
                wincode::deserialize(&buff).expect("Error while deserializing peer address");
            match req {
                NetworkMessage::PeerDiscoveryReq(peer) => {
                    let mut known_peers = known_peers_c.lock().await;
                    if !peer_exists(&known_peers, &(peer).into()) {
                        let mut new_peer: Peer =
                            Peer::from_serializable(peer, peer_drop_signal_sender.clone());
                        new_peer.init_heartbeat(me_serialized);
                        known_peers.push(new_peer);
                    }

                    let sv = known_peers
                        .iter()
                        .map(Into::<PeerSerializable>::into)
                        .collect();
                    let m = send_packet(&peer, NetworkMessage::PeerDiscoveryRes(sv)).await;
                    if m.is_err() {
                        peer_drop_signal_sender.clone().send(peer).await.unwrap();
                    }
                    println!("Discovery response sent");
                }
                NetworkMessage::PeerDiscoveryRes(peers) => {
                    let mut known_peers = known_peers_c.lock().await;
                    for peer in peers {
                        if !peer_exists(&known_peers, &peer) {
                            let mut new_peer: Peer =
                                Peer::from_serializable(peer, peer_drop_signal_sender.clone());
                            new_peer.init_heartbeat(me_serialized);
                            known_peers.push(new_peer);
                        }
                    }
                }
                NetworkMessage::HeartbeatReq((peer, req)) => {
                    println!("Received Heartbeat req from {:#?} , {req}", peer);
                    tokio::time::sleep(Duration::new(req as u64, 0)).await;
                    let m = send_packet(
                        &peer,
                        NetworkMessage::HeartbeatRes((me_serialized, req + 1)),
                    )
                    .await;
                    if m.is_err() {
                        peer_drop_signal_sender.clone().send(peer).await.unwrap();
                    }
                }
                NetworkMessage::HeartbeatRes((peer, res)) => {
                    println!("Sending Heartbeat res to {:#?}, {res}", peer);
                    let known_peers = known_peers_c.lock().await;
                    for kp in known_peers.iter() {
                        if *kp == peer {
                            let mut heartbeat_state = kp.heartbeat_state.lock().await;
                            heartbeat_state.res = res;
                        }
                    }
                }
            }
        });
    }
}

async fn send_packet(
    remote_peer: &PeerSerializable,
    packet: NetworkMessage,
) -> Result<usize, Error> {
    let mut stream = TcpStream::connect(SocketAddr::new(remote_peer.ip, remote_peer.port)).await?;
    let message_bytes = wincode::serialize(&packet).expect("Error serializing packet");
    let p = stream.write(&message_bytes).await;
    return p;
}

fn peer_exists(known_peers: &[Peer], peer: &PeerSerializable) -> bool {
    known_peers.iter().any(|p| p == peer)
}
