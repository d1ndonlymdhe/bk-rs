use wincode::{SchemaRead, SchemaWrite};

use crate::types::{block::Block, peer::Peer};


#[derive(SchemaWrite, SchemaRead)]
pub enum NetworkMessageReq {
    PeerDiscoveryReq(Peer),
    PushPeersReq(Vec<Peer>),
    PushChainReq(Vec<Block>)
}

#[derive(SchemaWrite, SchemaRead)]
pub enum NetworkMessageRes {
    PeerDiscoveryRes(Vec<Peer>),
    PushPeersRes,
    PushChainRes
}
