use std::{
    env,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};
use wincode::{SchemaRead, SchemaWrite};

mod block;

#[derive(SchemaWrite, SchemaRead, Clone, Copy, Debug)]
struct Peer {
    ip: IpAddr,
    port: u16,
}

#[derive(SchemaWrite, SchemaRead)]
enum NetworkMessage {
    PeerDiscoveryReq(Peer),
    PeerDiscoveryRes(Vec<Peer>),
}

#[tokio::main]
async fn main() {
    let mut args = env::args();
    let mut root_peer = None;
    if args.len() == 3 {
        let root_ip = args.nth(1).unwrap();
        let root_port = args.nth(0).unwrap().parse::<u16>().unwrap();
        root_peer = Some(Peer {
            ip: root_ip.parse().unwrap(),
            port: root_port,
        });
    }

    let sock = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let local_addr = sock.local_addr().unwrap();
    let ip = local_addr.ip();
    let ip_string = ip.to_string();
    let port = local_addr.port();
    println!("IP: {}, Port: {}", ip_string, port);

    let me = Peer { ip: ip, port: port };
    let known_peers = Arc::new(Mutex::new(vec![me]));

    if root_peer.is_some() {
        let root_peer = root_peer.unwrap();
        send_packet(&root_peer, NetworkMessage::PeerDiscoveryReq(me)).await;
    }

    let mut buff = [0; 1024];
    loop {
        println!("Waiting for connection...");
        let (mut stream, _addr) = sock.accept().await.unwrap();
        println!("Connection Received");
        let known_peers = known_peers.clone();
        tokio::spawn(async move {
            stream.readable().await.unwrap();
            println!("Stream Readable");
            stream.read(&mut buff).await.unwrap();
            let req: NetworkMessage =
                wincode::deserialize(&buff).expect("Error while deserializing peer address");
            match req {
                NetworkMessage::PeerDiscoveryReq(peer) => {
                    println!("OBTAINING KNOWN PEERS LOCK");
                    let mut known_peers = known_peers.lock().await;
                    println!("OBTAINED KNOWN PEERS LOCK");
                    if !peer_exists(&known_peers, &peer) {
                        known_peers.push(peer.clone());
                    }
                    send_packet(&peer, NetworkMessage::PeerDiscoveryRes(known_peers.clone())).await;
                    println!("Discovery response sent");
                }
                NetworkMessage::PeerDiscoveryRes(peers) => {
                    println!("Received discovery response");
                    println!("OBTAINING KNOWN PEERS LOCK");
                    let mut known_peers = known_peers.lock().await;
                    println!("OBTAINED KNOWN PEERS LOCK");
                    for peer in peers {
                        if !peer_exists(&known_peers, &peer) {
                            known_peers.push(peer);
                        }
                    }
                    println!("PEERS LIST: {:?}", known_peers);
                }
            }
        });
    }
}

async fn send_packet(remote_peer: &Peer, packet: NetworkMessage) -> usize {
    let mut stream = TcpStream::connect(SocketAddr::new(remote_peer.ip, remote_peer.port))
        .await
        .unwrap();
    let message_bytes = wincode::serialize(&packet).expect("Error serializing packet");
    stream.write(&message_bytes).await.unwrap()
}

fn peer_exists(known_peers: &[Peer], peer: &Peer) -> bool {
    known_peers
        .iter()
        .any(|p| p.ip == peer.ip && p.port == peer.port)
}
