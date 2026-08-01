use wincode::{SchemaRead, SchemaWrite};

use crate::types::{block::Block, mining_task::MiningTask, peer::Peer};

#[derive(SchemaWrite, SchemaRead)]
pub enum NetworkMessageReq {
    // During request request using the provided public ip
    PeerDiscoveryReq((Peer, String)),
    PushPeersReq(Vec<(Peer, String)>),
    // Chain, Sender Node Id, Vec<NodeId> This message also sent to these no need to send to them
    PushChainReq((Vec<Block>, String, Vec<String>)),
    DistributeMiningTask(MiningTask),
}

#[derive(SchemaWrite, SchemaRead)]
pub enum NetworkMessageRes {
    PeerDiscoveryRes(Vec<(Peer, String)>),
    PushPeersRes,
    PushChainRes,
}
