use std::collections::{HashMap, HashSet};
use std::println;
use std::{
    env,
    net::IpAddr,
    str::FromStr,
    sync::{Arc, Mutex},
};

use crate::http::rocket_server;
use crate::types::app_state::AppState;
use crate::types::chain::Chain;
use crate::types::known_peers::KnownPeers;
use crate::types::peer::Peer;
use clap::Parser;
use tokio::net::TcpListener;

mod http;
mod randomizer;
mod types;
mod utils;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "node_0")]
    node_id: String,
    #[arg(long)]
    root_ip: Option<String>,
    #[arg(long)]
    root_port: Option<u16>,
    #[arg(long)]
    root_id: Option<String>,
    #[arg(long)]
    public_ip: String,
}

#[tokio::main]
async fn main() {
    let mut root_peer_info = None;
    let args = Args::parse();
    if args.root_id.is_some() && args.root_ip.is_some() && args.root_port.is_some() {
        let root_ip = args.root_ip.unwrap();
        let root_port = args.root_port.unwrap();
        let root_id = args.root_id.unwrap();
        root_peer_info = Some((
            Peer {
                ip: IpAddr::from_str(&root_ip).unwrap(),
                port: root_port,
            },
            root_id,
        ));
    }
    let node_id = args.node_id;
    let public_ip = IpAddr::from_str(&args.public_ip).expect("Invalid ip address provided");
    let sock = TcpListener::bind("0.0.0.0:0").await.unwrap();

    let local_addr = sock.local_addr().unwrap();
    let ip = local_addr.ip();
    let ip_string = ip.to_string();
    let port = local_addr.port();
    println!("IP: {}, Port: {}", ip_string, port);
    let app_state = Arc::new(AppState::init(
        public_ip,
        node_id,
        Peer { ip, port },
        Arc::new(Mutex::new(KnownPeers {
            id_map: HashMap::new(),
            peers: vec![],
        })),
        Arc::new(Mutex::new(Chain::new())),
        Arc::new(Mutex::new(HashSet::new())),
    ));

    let app_state_peer_discovery = app_state.clone();
    if !root_peer_info.is_none() {
        let (root_peer, root_id) = root_peer_info.unwrap();
        let r = app_state_peer_discovery
            .root_peer_discovery(root_peer, &root_id)
            .await;
        if r.is_err() {
            println!(
                "Error occurred while discovering root peer: {}",
                r.unwrap_err()
            );
            return;
        }
    } else {
        println!("This is the root peer");
        tokio::spawn(rocket_server(app_state.clone()));
    }

    println!("Starting server process");

    let app_state_server_process = app_state.clone();
    loop {
        let (mut stream, _peer_addr) = sock.accept().await.unwrap();
        let binding = app_state_server_process.clone();
        tokio::spawn(async move {
            binding.server_process(&mut stream).await;
        });
    }
}
