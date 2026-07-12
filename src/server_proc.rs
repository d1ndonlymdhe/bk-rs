use std::{net::SocketAddr, sync::Arc};

use tokio::{
    io::AsyncReadExt,
    net::TcpStream,
    sync::{Mutex, mpsc},
};

use crate::{
    block::Block,
    types::{SyncResMessage, NetworkMessage, Peer, PeerSerializable},
    utils::{peer_exists, send_packet},
};

pub async fn server_process(
    stream: &mut TcpStream,
    peer_addr: SocketAddr,
    known_peers: Arc<Mutex<Vec<Peer>>>,
    peer_drop_signal_sender: mpsc::Sender<(PeerSerializable, String)>,
    chain: Arc<Mutex<Vec<Block>>>,
) {
    let mut buff = [0; 1024];
    stream.readable().await.unwrap();
    stream.read(&mut buff).await.unwrap();
    let req: NetworkMessage =
        wincode::deserialize(&buff).expect("Error while deserializing peer address");
    match req {
        NetworkMessage::PeerDiscoveryReq(peer_serializable) => {
            let mut known_peers_lock = known_peers.lock().await;
            let peer = PeerSerializable::from(peer_serializable);
            let self_peer = PeerSerializable::from(peer_addr);
            if peer != self_peer && !peer_exists(&known_peers_lock, &(peer).into()) {
                let mut new_peer: Peer =
                    Peer::from_serializable(peer, peer_drop_signal_sender.clone(), chain.clone());

                new_peer.init_heartbeat(known_peers.clone(), chain);
                known_peers_lock.push(new_peer);
            }

            let sv = known_peers_lock
                .iter()
                .map(Into::<PeerSerializable>::into)
                .collect();
            let _m = send_packet(stream, NetworkMessage::PeerDiscoveryRes(sv)).await;
            println!("Discovery response sent");
        }
        NetworkMessage::PeerDiscoveryRes(_) => {}
        NetworkMessage::SyncReq => {
            let peer = PeerSerializable::from(peer_addr);
            println!("Received Sync Req req from {:#?}", peer,);
            let last_block = chain.lock().await.clone().into_iter().last();
            let known_peers = known_peers.lock().await;
            let known_peers_serialized = known_peers
                .iter()
                .map(|kp| {
                    return PeerSerializable {
                        ip: kp.ip,
                        port: kp.port,
                    };
                })
                .collect();
            let _m = send_packet(
                stream,
                NetworkMessage::SyncRes(SyncResMessage {
                    last_block: last_block,
                    peers: known_peers_serialized,
                }),
            )
            .await;
        }
        NetworkMessage::SyncRes(_) => {}
    }
}
