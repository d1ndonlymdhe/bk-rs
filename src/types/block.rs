use enum_iterator::Sequence;
use rayon::prelude::*;
use rocket::request::FromParam;
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};
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

#[derive(SchemaWrite, SchemaRead, PartialEq, Eq, Hash, Clone, Copy, Debug, Sequence)]
pub enum Candidate {
    A,
    B,
    C,
    D,
    E,
    F,
}

impl<'a> FromParam<'a> for Candidate {
    type Error = &'static str;

    fn from_param(param: &'a str) -> Result<Self, Self::Error> {
        match param {
            "A" => Ok(Candidate::A),
            "B" => Ok(Candidate::B),
            "C" => Ok(Candidate::C),
            "D" => Ok(Candidate::D),
            "E" => Ok(Candidate::E),
            "F" => Ok(Candidate::F),
            _ => Err("Invalid candidate"),
        }
    }
}

impl Into<String> for Candidate {
    fn into(self) -> String {
        match self {
            Candidate::A => "A".into(),
            Candidate::B => "B".into(),
            Candidate::C => "C".into(),
            Candidate::D => "D".into(),
            Candidate::E => "E".into(),
            Candidate::F => "F".into(),
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
    // Could be something else later LRS identifier
    pub voter_id: String,
    // Milliseconds since UNIX_EPOCH when the block was mined
    pub timestamp: u128,
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

        // Just checking if valid uuid
        if uuid::Uuid::parse_str(&block.voter_id).is_err() {
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

    pub fn new(idx: usize, data: Candidate, prev_hash: Vec<u8>, voter_id: &str) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time is before UNIX_EPOCH")
            .as_millis();
        let mut block = Block {
            idx,
            data,
            prev_hash,
            nonce: None,
            hash: vec![],
            voter_id: voter_id.to_string(),
            timestamp,
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
