use enum_iterator::Sequence;
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use wincode::{SchemaRead, SchemaWrite};

const DIFFICULTY: usize = 20;
fn check_leading_zeroes(hash: &[u8], difficulty: usize) -> bool {
    let full_bytes = difficulty / 8;
    let remaining_bits = difficulty % 8;
    if hash.iter().take(full_bytes).any(|&b| b != 0) {
        return false;
    }
    if remaining_bits > 0 {
        if full_bytes >= hash.len() {
            return false;
        }
        let next_byte = hash[full_bytes];
        if next_byte >> (8 - remaining_bits) != 0 {
            return false;
        }
    }
    return true;
}

#[derive(SchemaWrite, SchemaRead, PartialEq, Eq, Clone, Copy, Debug, Sequence)]
pub enum Candidate {
    A,
    B,
    C,
}

impl Into<String> for Candidate {
    fn into(self) -> String {
        match self {
            Candidate::A => "A".into(),
            Candidate::B => "B".into(),
            Candidate::C => "C".into(),
        }
    }
}

#[derive(SchemaWrite, SchemaRead, Clone, Debug)]
pub struct Block {
    pub idx: usize,
    pub data: Candidate,
    pub prev_hash: Vec<u8>,
    pub nonce: Option<u128>,
    pub hash: Vec<u8>,
}


impl Block {
    pub fn validate(block: &Self) -> bool {
        // The block needs to have a nonce
        if block.nonce.is_none() {
            return false;
        }
        // the block needs to have a hash
        if block.hash.is_empty() {
            return false;
        }
        // the block needs to have set number of leading zeroes
        if !check_leading_zeroes(block.hash.as_slice(), DIFFICULTY) {
            return false;
        }
        let mut block_clone = block.clone();
        block_clone.hash = vec![];
        block_clone.nonce = None;
        // Check if the reported hash is correct
        // convert to bytes without hash and nonce
        let p = wincode::serialize(&block_clone);
        if p.is_err() {
            return false;
        }
        let mut hasher = Sha256::new();
        hasher.update(&p.unwrap());
        // update hasher with reported nonce
        hasher.update(block.nonce.unwrap().to_be_bytes());
        let h = hasher.finalize();
        // check if final hash matches reported hash
        return *h == block.hash;
    }

    pub fn new(idx: usize, data: Candidate, prev_hash: Vec<u8>) -> Self {
        let mut block = Block {
            idx,
            data,
            prev_hash,
            nonce: None,
            hash: vec![],
        };
        block.hash();
        return block;
    }
    pub fn hash(&mut self) {
        self.hash = vec![];
        self.nonce = None;
        // Convert to bytes without the hash and nonce
        let p = wincode::serialize(self).expect("Error while serializing block");
        let mut base_hasher = Sha256::new();
        base_hasher.update(&p);
        let v = (0u128..u128::MAX).into_par_iter().find_any(|v| {
            let mut hasher = base_hasher.clone();
            // append candidate nonce to hasher
            hasher.update(&v.to_be_bytes());
            let h = hasher.finalize();
            // check for leading zeroes
            return check_leading_zeroes(h.as_slice(), DIFFICULTY);
        });

        // update the block with hash and nonce
        let nonce = v.expect("No nonce found");
        self.nonce = Some(nonce);
        let mut final_hasher = base_hasher.clone();
        final_hasher.update(nonce.to_be_bytes());
        // p.extend(nonce.to_be_bytes().as_ref());
        self.hash = final_hasher.finalize().to_vec();
    }
}