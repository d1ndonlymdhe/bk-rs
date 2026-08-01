use std::collections::HashSet;

use crate::block::block::Block;


pub struct Chain {
    blocks: Vec<Block>,
}

impl Chain {
    pub fn new() -> Self {
        Self { blocks: Vec::new() }
    }

    pub fn push(&mut self, block: Block) {
        self.blocks.push(block);
    }

    pub fn clear(&mut self) {
        self.blocks.clear();
    }

    pub fn extend(&mut self, new_blocks: Vec<Block>) {
        self.blocks.extend(new_blocks);
    }

    pub fn validate(new_blocks: &[Block], current_chain: &[Block]) -> Result<(), String> {
        if new_blocks.len() < current_chain.len() {
            return Err(format!(
                "new chain ({} blocks) is shorter than current chain ({} blocks)",
                new_blocks.len(),
                current_chain.len()
            ));
        }
        if new_blocks.len() == current_chain.len() {
            let new_last_timestamp = new_blocks.last().map(|b| b.timestamp);
            let current_last_timestamp = current_chain.last().map(|b| b.timestamp);
            if new_last_timestamp <= current_last_timestamp {
                return Err(format!(
                    "new chain has equal height ({} blocks) but is not newer (new timestamp: {:?}, current timestamp: {:?})",
                    new_blocks.len(),
                    new_last_timestamp,
                    current_last_timestamp
                ));
            }
        }
        let mut prev_block_hash = new_blocks[0].hash.clone();
        let mut new_blocks_voter_ids = HashSet::new();
        for i in 1..new_blocks.len() {
            if new_blocks[i].prev_hash != prev_block_hash {
                return Err(format!(
                    "block {} prev_hash does not match hash of block {}",
                    i,
                    i - 1
                ));
            }
            if !Block::validate(&new_blocks[i]) {
                return Err(format!("block {} failed validation", i));
            }
            if !new_blocks_voter_ids.contains(&new_blocks[i].voter_id) {
                new_blocks_voter_ids.insert(new_blocks[i].voter_id.clone());
                prev_block_hash = new_blocks[i].hash.clone();
            } else {
                return Err(format!(
                    "block {} has a duplicate voter_id within the new chain",
                    i
                ));
            }
        }
        Ok(())
    }

    // Assumes that the provided chain itself is valid
    pub fn validate_addition(
        seen_voter_ids: &HashSet<String>,
        current_chain: &[Block],
        new_block: &Block,
    ) -> ValidateChainRes {
        // If already voted ignore
        if seen_voter_ids.contains(&new_block.voter_id) {
            return ValidateChainRes::IgnoreBlock;
        }

        if current_chain.is_empty() {
            // Only try to add new block if it says it is the first block on the chain
            if new_block.idx == 0 {
                if Block::validate(new_block) {
                    return ValidateChainRes::AddBlock;
                } else {
                    return ValidateChainRes::IgnoreBlock;
                }
            } else {
                return ValidateChainRes::IgnoreBlock;
            }
        }
        let last_block = &current_chain[current_chain.len() - 1];

        if new_block.idx <= last_block.idx {
            // If the block is earlier in the chain ignore the block;
            return ValidateChainRes::AttemptedLateAdd;
        }

        // if current chain has blocks only try to add new block if it says it is the next block on the chain
        if new_block.idx == last_block.idx + 1 { 
            if new_block.prev_hash != last_block.hash {
                // The chain's tip changed (e.g. replaced wholesale by sync_chain) while this
                // block was being mined, so it's mining against a stale parent. Retryable, same
                // as losing the race on idx.
                return ValidateChainRes::AttemptedLateAdd;
            } else {
                if Block::validate(new_block) {
                    return ValidateChainRes::AddBlock;
                } else {
                    return ValidateChainRes::IgnoreBlock;
                }
            }
        } else {
            return ValidateChainRes::IgnoreBlock;
        }
    }

    pub fn hash(&self) -> String {
        let as_bytes = wincode::serialize(&self.blocks).unwrap();
        let digest = md5::compute(&as_bytes);
        format!("{:x}", digest)
    }
}

impl std::ops::Deref for Chain {
    type Target = [Block];

    fn deref(&self) -> &[Block] {
        &self.blocks
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum ValidateChainRes {
    IgnoreBlock,
    AddBlock,
    AttemptedLateAdd,
}
