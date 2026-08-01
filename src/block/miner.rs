use wincode::{SchemaRead, SchemaWrite};

use crate::block::block::{Block, Candidate};

#[derive(SchemaWrite, SchemaRead, Clone)]
pub struct MiningTask {
    pub voter_id: String,
    pub candidate: Candidate,
}

pub fn mine_block(mining_task: &MiningTask, last_block: Option<&Block>, last_idx: usize) -> Block {
    let block = Block::new(
        if last_block.is_none() {
            0
        } else {
            last_idx + 1
        },
        mining_task.candidate,
        last_block
            .as_ref()
            .map(|b| b.hash.clone())
            .unwrap_or_else(|| vec![]),
        &mining_task.voter_id,
    );
    block
}
