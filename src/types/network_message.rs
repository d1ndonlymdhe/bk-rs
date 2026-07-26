use wincode::{SchemaRead, SchemaWrite};

use crate::types::{block::Block, mining_task::MiningTask, peer::Peer};


#[derive(SchemaWrite, SchemaRead)]
pub enum NetworkMessageReq {
    PeerDiscoveryReq(Peer),
    PushPeersReq(Vec<Peer>),
    PushChainReq(Vec<Block>),
    DistributeMiningTask(MiningTask)
}

#[derive(SchemaWrite, SchemaRead)]
pub enum NetworkMessageRes {
    PeerDiscoveryRes(Vec<Peer>),
    PushPeersRes,
    PushChainRes
}
