use std::net::{IpAddr, SocketAddr};

use wincode::{SchemaRead, SchemaWrite};

#[derive(SchemaRead, SchemaWrite, Clone, Copy, Debug, PartialEq)]
pub struct Peer {
    pub ip: IpAddr,
    pub port: u16,
}

impl Into<SocketAddr> for Peer {
    fn into(self) -> SocketAddr {
        SocketAddr::new(self.ip, self.port)
    }
}

impl From<SocketAddr> for Peer {
    fn from(value: SocketAddr) -> Self {
        return Self {
            ip: value.ip(),
            port: value.port(),
        };
    }
}

pub fn peer_exists(known_peers: &[Peer], peer: &Peer) -> bool {
    known_peers.iter().any(|p| p == peer)
}
