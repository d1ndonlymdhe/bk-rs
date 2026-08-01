use std::collections::{HashMap, HashSet};
use std::println;
use std::{
    net::IpAddr,
    str::FromStr,
    sync::{Arc, Mutex},
};

use crate::app::App;
use crate::block::chain::Chain;
use crate::http_server::http::rocket_server;
use crate::peer::known_peers::KnownPeers;
use crate::peer::peer::Peer;
use clap::Parser;
use tokio::net::TcpListener;
use tokio::select;

mod app;
mod block;
mod http_server;
mod net;
mod peer;

#[derive(Parser)]
struct Args {
    #[arg(
        long,
        default_value = "node_0",
        help = "Required, Identifier for this node"
    )]
    node_id: String,
    #[arg(
        long,
        help = "IP address of the root node, if left out the current node will run as root node"
    )]
    root_ip: Option<String>,
    #[arg(
        long,
        help = "Port of the root node, if left out the current node will run as root node"
    )]
    root_port: Option<u16>,
    #[arg(
        long,
        default_value = "node_0",
        help = "Identifier for the root node, if left out the current node will run as root node"
    )]
    root_id: Option<String>,
    #[arg(
        long,
        default_value = "0.0.0.0",
        help = "Required, Public IP address for the node"
    )]
    public_ip: String,
    #[arg(long, default_value = "4567", help = "Port for the node")]
    public_port: u16,
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
    let bind_string = format!("{}:{}", &args.public_ip, &args.public_port);
    let sock = TcpListener::bind(&bind_string).await.unwrap();

    let local_addr = sock.local_addr().unwrap();
    let ip = local_addr.ip();
    let ip_string = ip.to_string();
    let port = local_addr.port();
    println!("IP: {}, Port: {}", ip_string, port);
    let app_state = Arc::new(App::init(
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

    let ctrl_c = tokio::signal::ctrl_c();

    select! {
        _ = async {
            loop {
                let (mut stream, _peer_addr) = sock.accept().await.unwrap();
                let binding = app_state_server_process.clone();
                tokio::spawn(async move {
                    binding.server_process(&mut stream).await;
                });
            }
        } => {}
        _ = ctrl_c => {
            println!("Received Ctrl+C, shutting down...");
        }
    }
}
