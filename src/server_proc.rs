use std::{net::SocketAddr, sync::Arc};

use tokio::{
    io::AsyncReadExt,
    net::TcpStream,
    sync::{Mutex, mpsc},
};

use crate::{
    types::{NetworkMessage, Peer, PeerSerializable},
    utils::{peer_exists, send_packet},
};

pub async fn server_process(
    stream: &mut TcpStream,
    peer_addr: SocketAddr,
    known_peers: Arc<Mutex<Vec<Peer>>>,
    peer_drop_signal_sender: mpsc::Sender<(PeerSerializable, String)>,
) {
    let mut buff = [0; 1024];
    stream.readable().await.unwrap();
    stream.read(&mut buff).await.unwrap();
    let req: NetworkMessage =
        wincode::deserialize(&buff).expect("Error while deserializing peer address");
    match req {
        NetworkMessage::PeerDiscoveryReq(peer_serializable) => {
            let mut known_peers = known_peers.lock().await;
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
            let _m = send_packet(stream, NetworkMessage::PeerDiscoveryRes(sv)).await;
            println!("Discovery response sent");
        }
        NetworkMessage::PeerDiscoveryRes(_) => {}
        NetworkMessage::HeartbeatReq => {
            let peer = PeerSerializable::from(peer_addr);
            println!("Received Heartbeat req from {:#?}", peer,);
            let _m = send_packet(stream, NetworkMessage::HeartbeatRes).await;
        }
        NetworkMessage::HeartbeatRes => {}
    }
}
