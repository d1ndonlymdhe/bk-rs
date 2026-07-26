use std::collections::HashSet;
use std::println;
use std::{
    env,
    net::IpAddr,
    str::FromStr,
    sync::{Arc, Mutex},
};

use tokio::net::TcpListener;

use crate::http::rocket_server;
use crate::types::app_state::AppState;
use crate::types::peer::Peer;

mod http;
mod randomizer;
mod types;
mod utils;

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
        root_peer_info = Some(Peer {
            ip: IpAddr::from_str(&root_ip).unwrap(),
            port: root_port,
        });
    }

    let sock = TcpListener::bind("0.0.0.0:0").await.unwrap();

    let local_addr = sock.local_addr().unwrap();
    let ip = local_addr.ip();
    let ip_string = ip.to_string();
    let port = local_addr.port();
    println!("IP: {}, Port: {}", ip_string, port);
    let app_state = Arc::new(AppState::init(
        node_id,
        Peer { ip, port },
        Arc::new(Mutex::new(vec![])),
        Arc::new(Mutex::new(vec![])),
        Arc::new(Mutex::new(HashSet::new())),
    ));

    // let app_state_mining = app_state.clone();
    // tokio::spawn(async move {
    //     loop {
    //         app_state_mining.mine_random_block().await;
    //     }
    // });

    let app_state_peer_discovery = app_state.clone();
    if !root_peer_info.is_none() {
        let r = app_state_peer_discovery
            .root_peer_discovery(root_peer_info.unwrap())
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
