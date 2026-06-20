use std::{
    env,
    net::{IpAddr, SocketAddr},
};

use tokio::net::UdpSocket;
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
    let sock = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let local_addr = sock.local_addr().unwrap();
    let ip = local_addr.ip();
    let ip_string = ip.to_string();
    let port = local_addr.port();
    println!("IP: {}, Port: {}", ip_string, port);

    let me = Peer { ip: ip, port: port };
    let mut known_peers = vec![me];

    let mut buff = [0; 1024];
    if root_peer.is_some() {
        let root_peer = root_peer.unwrap();
        let addr = SocketAddr::new(root_peer.ip, root_peer.port);
        let discovery_req = NetworkMessage::PeerDiscoveryReq(me);
        let discovery_req_bytes =
            wincode::serialize(&discovery_req).expect("Error while serializing discovery request");
        sock.send_to(&discovery_req_bytes, addr)
            .await
            .expect("Error sending init packet");
    }
    loop {
        let (_len, addr) = sock.recv_from(&mut buff).await.unwrap();
        let req: NetworkMessage =
            wincode::deserialize(&buff).expect("Error while deserializing peer address");

        match req {
            NetworkMessage::PeerDiscoveryReq(peer) => {
                known_peers.push(peer);
                let res = NetworkMessage::PeerDiscoveryRes(known_peers.clone());
                let res_bytes =
                    wincode::serialize(&res).expect("Error while serializing discovery response");
                sock.send_to(&res_bytes, addr)
                    .await
                    .expect("Error while responding to peer discovery");
            }
            NetworkMessage::PeerDiscoveryRes(peers) => {
                known_peers.append(&mut peers.clone());
                println!("PEERS LIST: {:?}", known_peers);
            }
        }
    }
}
