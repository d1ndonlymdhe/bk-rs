use std::{eprintln, sync::Arc};

use rocket::State;

use crate::{
    types::{
        app_state::AppState, block::Candidate, mining_task::MiningTask,
        network_message::NetworkMessageReq,
    },
    utils::{open_stream, send_packet_req},
};

// Starts a web server on port 8000
pub async fn rocket_server(app_state: Arc<AppState>) {
    let r = rocket::build()
        .mount(
            "/",
            rocket::routes![add_vote, get_vote, get_total_votes, get_tally],
        )
        .manage(app_state)
        .launch()
        .await;
    if r.is_err() {
        eprintln!("Error launching server, {}", r.unwrap_err().to_string())
    }
}

#[rocket::post("/add_vote/<voter_id>/<vote>")]
async fn add_vote(app_state: &State<Arc<AppState>>, voter_id: &str, vote: Candidate) -> String {
    let app_state = app_state.inner();
    let vote_task = MiningTask {
        voter_id: voter_id.to_string(),
        candidate: vote,
    };
    distribute_vote_task(app_state.clone(), &vote_task).await;
    distribute_vote_task(app_state.clone(), &vote_task).await;
    distribute_vote_task(app_state.clone(), &vote_task).await;

    return "OK".to_string();
}

#[rocket::get("/vote/<voter_id>")]
async fn get_vote(app_state: &State<Arc<AppState>>, voter_id: &str) -> String {
    let app_state = app_state.inner();
    match app_state.get_vote(voter_id).await {
        Some(candidate) => candidate.into(),
        None => "NOT_FOUND".to_string(),
    }
}

#[rocket::get("/votes/total")]
async fn get_total_votes(app_state: &State<Arc<AppState>>) -> String {
    let app_state = app_state.inner();
    app_state.get_total_votes().await.to_string()
}

#[rocket::get("/votes/tally")]
async fn get_tally(app_state: &State<Arc<AppState>>) -> String {
    let app_state = app_state.inner();
    let mut lines = app_state
        .get_tally()
        .await
        .into_iter()
        .map(|(candidate, count)| {
            let name: String = candidate.into();
            format!("{}:{}", name, count)
        })
        .collect::<Vec<String>>();
    lines.sort();
    lines.join(",")
}

async fn distribute_vote_task(app_state: Arc<AppState>, task: &MiningTask) {
    println!("GETTING PEERS");
    let known_peers = app_state.get_known_peers().await;
    println!("GOT PEERS");
    if known_peers.len() == 0 {
        return;
    }
    let rand_peer = known_peers[rand::random_range(0..known_peers.len())];
    let stream = open_stream(&rand_peer).await;

    match stream {
        Ok(mut stream) => {
            let r = send_packet_req(
                &mut stream,
                NetworkMessageReq::DistributeMiningTask(task.clone()),
            )
            .await;
            match r {
                Ok(_) => {
                    println!(
                        "Sending distribute task to {} {}",
                        rand_peer.ip, rand_peer.port,
                    );
                    return;
                }
                Err(err) => {
                    eprintln!("Error while sending mining task packet {}", err.to_string())
                }
            }
        }
        Err(err) => {
            eprintln!(
                "Error while opening stream to distribute task {}",
                err.to_string()
            )
        }
    }
}
