use std::collections::HashMap;

use crate::block::block::Candidate;


// Keeps voter_id -> vote and per-candidate tallies so lookups don't require
// scanning the chain. Rebuilt/updated whenever the chain changes.
pub struct VoteCache {
    votes_by_voter: HashMap<String, Candidate>,
    tally: HashMap<Candidate, usize>,
}

impl VoteCache {
    pub fn new() -> Self {
        Self {
            votes_by_voter: HashMap::new(),
            tally: HashMap::new(),
        }
    }

    pub fn record_vote(&mut self, voter_id: String, candidate: Candidate) {
        if self.votes_by_voter.insert(voter_id, candidate).is_none() {
            *self.tally.entry(candidate).or_insert(0) += 1;
        }
    }

    pub fn get_vote(&self, voter_id: &str) -> Option<Candidate> {
        self.votes_by_voter.get(voter_id).copied()
    }

    pub fn tally(&self) -> HashMap<Candidate, usize> {
        self.tally.clone()
    }

    pub fn total_votes(&self) -> usize {
        self.votes_by_voter.len()
    }
}
