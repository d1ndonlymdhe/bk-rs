use crate::types::block::Candidate;
use wincode::{SchemaRead, SchemaWrite};

#[derive(SchemaWrite, SchemaRead,Clone)]
pub struct MiningTask {
    pub voter_id: String,
    pub candidate: Candidate   
}