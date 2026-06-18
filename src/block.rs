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

#[derive(SchemaWrite, SchemaRead, PartialEq, Eq, Clone, Copy, Debug)]
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
        let p = wincode::serialize(self).expect("Error while serializing block");
        let mut base_hasher = Sha256::new();
        base_hasher.update(&p);
        let v = (0u128..u128::MAX).into_par_iter().find_any(|v| {
            let mut hasher = base_hasher.clone();
            hasher.update(&v.to_be_bytes());
            let h = hasher.finalize();
            return check_leading_zeroes(h.as_slice(), DIFFICULTY);
        });
        let nonce = v.expect("No nonce found");

        self.nonce = Some(nonce);
        let mut final_hasher = base_hasher.clone();
        final_hasher.update(nonce.to_be_bytes());
        // p.extend(nonce.to_be_bytes().as_ref());
        self.hash = final_hasher.finalize().to_vec();
    }
}