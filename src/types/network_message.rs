use wincode::{SchemaRead, SchemaWrite};

use crate::types::{block::Block, peer_serializable::PeerSerializable};

#[derive(Debug, Clone, SchemaRead, SchemaWrite)]
pub struct SyncResMessage {
    pub peers: Vec<PeerSerializable>,
    pub last_block: Option<Block>,
}

#[derive(SchemaWrite, SchemaRead)]
pub enum NetworkMessageReq {
    PeerDiscoveryReq(PeerSerializable),
    SyncReq,
    FullChainReq,
}

#[derive(SchemaWrite, SchemaRead)]
pub enum NetworkMessageRes {
    PeerDiscoveryRes(Vec<PeerSerializable>),
    SyncRes(SyncResMessage),
    FullChainRes(Vec<Block>),
}
