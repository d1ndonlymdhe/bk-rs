use std::{env, sync::Arc};

use tokio::{
    net::TcpListener,
    sync::{Mutex, mpsc},
};

use crate::server_proc::server_process;
use crate::{
    types::{NetworkMessage, Peer, PeerSerializable},
    utils::{open_stream, peer_exists, send_packet_and_wait},
};

mod block;
mod server_proc;
mod types;
mod utils;

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
    let self_serialized = PeerSerializable { ip, port };
    if root_peer.is_some() {
        let root_peer = root_peer.unwrap();
        let root_peer_serialized = (&root_peer).into();
        let stream = open_stream(&root_peer_serialized).await;
        let known_peers = known_peers.clone();
        if stream.is_ok() {
            let mut stream = stream.unwrap();
            let res = send_packet_and_wait(
                &mut stream,
                NetworkMessage::PeerDiscoveryReq(self_serialized),
            )
            .await;
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

    let known_peers_c = known_peers_c.clone();
    loop {
        let (mut stream, peer_addr) = sock.accept().await.unwrap();
        let peer_drop_signal_sender = peer_drop_signal_sender.clone();
        let v = known_peers_c.clone();
        tokio::spawn(async move {
            server_process(&mut stream, peer_addr, v.clone(), peer_drop_signal_sender).await;
        });
    }
}
