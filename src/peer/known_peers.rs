use std::collections::HashMap;

use crate::peer::peer::Peer;


pub struct KnownPeers {
    pub peers: Vec<Peer>,
    // id -> (Peer, ChainHash)
    pub id_map: HashMap<String, (Peer, String)>,
}

impl KnownPeers {
    // While adding a peer we never have their chain_hash, that is obtained with chain sync later
    pub fn add_peer(&mut self, peer: Peer, id: String) {
        self.peers.push(peer);
        self.id_map.insert(id, (peer, "".into()));
    }
    pub fn remove_peer_with_peer(&mut self, peer: &Peer) -> bool {
        let original_len = self.peers.len();
        self.peers.retain(|p| p != peer);
        self.id_map.retain(|_, v| v.0 != *peer);
        return original_len != self.peers.len();
    }
    pub fn as_entries(&self) -> Vec<(Peer, String)> {
        self.id_map
            .iter()
            .map(|(id, peer)| (peer.0, id.clone()))
            .collect()
    }
    // Peer, Id, Chain Hash
    pub fn as_entries_with_hash(&self) -> Vec<(Peer, String, String)> {
        self.id_map
            .iter()
            .map(|(id, peer)| (peer.0, id.clone(), peer.1.clone()))
            .collect()
    }
    pub fn update_chain_hash(&mut self, id: &str, new_hash: String) {
        if let Some((_, chain_hash)) = self.id_map.get_mut(id) {
            *chain_hash = new_hash;
        }
    }
    pub fn update_chain_hash_all(&mut self, new_hash: String) {
        for (_, (_, chain_hash)) in &mut self.id_map {
            *chain_hash = new_hash.clone();
        }
    }
}
