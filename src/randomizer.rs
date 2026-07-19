use enum_iterator::all;

use crate::types::block::{Block, Candidate};

pub fn mine_random_block(last_block: Option<&Block>, last_idx: usize) -> Block {
    println!("MINE RANDOM BLOCK");
    let candidate_idx = rand::random_range(0..2);
    let rand_candidate = all::<Candidate>().nth(candidate_idx).unwrap();
    let block = Block::new(
        if last_block.is_none() {
            0
        } else {
            last_idx + 1
        },
        rand_candidate,
        last_block
            .as_ref()
            .map(|b| b.hash.clone())
            .unwrap_or_else(|| vec![]),
    );
    return block;
}
