use std::{env, sync::Arc};

use tokio::{
    net::TcpListener,
    sync::{Mutex, mpsc},
};

use crate::{
    block::{Block, validate_chain_addition},
    network_message::{NetworkMessageReq, NetworkMessageRes},
    randomizer::mine_random_block,
    server_proc::server_process,
};
use crate::{peer::Peer, peer_serializable::PeerSerializable, utils::save_chain_to_file};
use crate::{
    peer::peer_exists,
    utils::{open_stream, send_packet_and_wait},
};

mod block;
mod network_message;
mod peer;
mod peer_serializable;
mod randomizer;
mod server_proc;
mod utils;

fn save_chain(node_id: &str, chain: &Vec<Block>) -> std::io::Result<()> {
    let filename = format!("chain_{}.bin", node_id);
    let _ = save_chain_to_file(chain, &filename)?;
    println!("Chain saved to {} ({} blocks)", filename, chain.len());
    Ok(())
}

#[tokio::main]
async fn main() {
    let mut args = env::args();
    args.next(); // skip program name

    let mut node_id = String::from("node_0");
    let mut root_peer_info = None;

    // Parse arguments: first is node_id, then optional root IP and port
    let args_vec: Vec<String> = args.collect();

    if args_vec.len() >= 1 {
        node_id = args_vec[0].clone();
    }

    if args_vec.len() >= 3 {
        let root_ip = args_vec[1].clone();
        let root_port = args_vec[2].parse::<u16>().unwrap();
        root_peer_info = Some((root_ip.parse().unwrap(), root_port));
    }

    let chain: Arc<Mutex<Vec<Block>>> = Arc::new(Mutex::new(Vec::new()));
    let (peer_drop_signal_sender, mut peer_drop_signal_receiver) =
        mpsc::channel::<(PeerSerializable, String)>(5);
    let sock = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let local_addr = sock.local_addr().unwrap();
    let ip = local_addr.ip();
    let ip_string = ip.to_string();
    let port = local_addr.port();
    println!("IP: {}, Port: {}", ip_string, port);
    let known_peers: Arc<Mutex<Vec<Peer>>> = Arc::new(Mutex::new(vec![]));

    // Create and initialize root peer if provided
    if root_peer_info.is_some() {
        let (root_ip, root_port) = root_peer_info.unwrap();
        let mut p = Peer::new(
            root_ip,
            root_port,
            peer_drop_signal_sender.clone(),
            chain.clone(),
        );
        p.init_sync(known_peers.clone(), chain.clone());
        let mut known_peers_lock = known_peers.lock().await;
        known_peers_lock.push(p);
    }

    let chain_rand_block = chain.clone();

    println!("Random block mining initialized");
    tokio::spawn(async move {
        println!("HELLO");
        loop {
            let binding = chain_rand_block.clone();
            let chain = binding.lock().await;
            let last_block = chain.iter().last().cloned();
            let len = chain.len();
            drop(chain);

            let rand_block = tokio::task::spawn_blocking(move || {
                mine_random_block(last_block.as_ref(), if len > 0 { len - 1 } else { 0 })
            })
            .await;
            if rand_block.is_err() {
                println!("{:?}", rand_block.err());
            } else {
                println!("Random block mined");
                let mut chain = binding.lock().await;
                let rand_block = rand_block.unwrap();
                match validate_chain_addition(&chain, &rand_block) {
                    block::ValidateChainRes::RequestFullChain => {
                        println!("GOT REQUEST FOR FULL CHAIN FROM SELF MINE: IGNORE");
                    }
                    block::ValidateChainRes::IgnoreBlock => {
                        println!("Invalid block mined: Ignore")
                    }
                    block::ValidateChainRes::AddBlock => {
                        chain.push(rand_block);
                    }
                }
            }
            // .unwrap();
        }
    });

    let self_serialized = PeerSerializable { ip, port };
    let chain_peer_discovery = chain.clone();

    // Perform peer discovery from root peer if we have one
    if !root_peer_info.is_none() {
        // Get root peer from known_peers
        let known_peers_lock = known_peers.lock().await;
        if let Some(root_peer) = known_peers_lock.first() {
            let root_peer_serialized = PeerSerializable {
                ip: root_peer.ip,
                port: root_peer.port,
            };
            drop(known_peers_lock);

            let stream = open_stream(&root_peer_serialized).await;
            if stream.is_ok() {
                let mut stream = stream.unwrap();
                let res = send_packet_and_wait(
                    &mut stream,
                    NetworkMessageReq::PeerDiscoveryReq(self_serialized),
                )
                .await;
                match res {
                    Ok(msg) => match msg {
                        NetworkMessageRes::PeerDiscoveryRes(peers) => {
                            let mut known_peers_lock = known_peers.lock().await;
                            for peer in peers {
                                if peer != self_serialized && !peer_exists(&known_peers_lock, &peer)
                                {
                                    let mut new_peer: Peer = Peer::from_serializable(
                                        peer,
                                        peer_drop_signal_sender.clone(),
                                        chain_peer_discovery.clone(),
                                    );
                                    let hb_chain = chain.clone();
                                    let hb_peers = known_peers.clone();
                                    new_peer.init_sync(hb_peers, hb_chain);
                                    known_peers_lock.push(new_peer);
                                }
                            }
                        }
                        _ => {
                            panic!("UNSUPPORTED RESPONSE FOR PEER DISCOVERY")
                        }
                    },
                    Err(_) => panic!("Could not get peer discovery response from root peer"),
                }
            } else {
                panic!("Could not connect with root peer")
            }
        }
    }
    let known_peers_c = known_peers.clone();

    // Listen for peer drop signals, when peer is dropped heartbeat is cancelled
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
    let chain_server_proc = chain.clone();
    println!("Starting server process");

    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
    let mut sigint =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();
    let chain_for_signal = chain.clone();
    let node_id_for_signal = node_id.clone();

    tokio::select! {
        _ = async {
            loop {
                let (mut stream, peer_addr) = sock.accept().await.unwrap();
                let peer_drop_signal_sender = peer_drop_signal_sender.clone();
                let v = known_peers_c.clone();
                let chain_server_proc = chain_server_proc.clone();
                tokio::spawn(async move {
                    // server listen process
                    server_process(
                        &mut stream,
                        peer_addr,
                        v.clone(),
                        peer_drop_signal_sender,
                        chain_server_proc.clone(),
                    )
                    .await;
                });
            }
        } => {}
        _ = sigterm.recv() => {
            println!("Received SIGTERM, saving chain and exiting...");
            let chain_lock = chain_for_signal.lock().await;
            let _ = save_chain(&node_id_for_signal, &chain_lock);
        }
        _ = sigint.recv() => {
            println!("Received SIGINT, saving chain and exiting...");
            let chain_lock = chain_for_signal.lock().await;
            let _ = save_chain(&node_id_for_signal, &chain_lock);
        }
    }
}
