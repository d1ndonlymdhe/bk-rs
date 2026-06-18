use crate::block::{Block, Candidate};
mod block;
fn main() {
    let block = Block::new(0, Candidate::A, vec![]);
    let block_2 = Block::new(1, Candidate::B, block.hash.clone());
    println!("Block 1: {:?}", block);
    println!("Block 2: {:?}", block_2);
}
