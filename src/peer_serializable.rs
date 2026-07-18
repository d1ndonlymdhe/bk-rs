use std::net::{IpAddr, SocketAddr};

use wincode::{SchemaRead, SchemaWrite};

#[derive(SchemaRead, SchemaWrite, Clone, Copy, Debug, PartialEq)]
pub struct PeerSerializable {
    pub ip: IpAddr,
    pub port: u16,
}

impl Into<SocketAddr> for PeerSerializable {
    fn into(self) -> SocketAddr {
        SocketAddr::new(self.ip, self.port)
    }
}

impl From<SocketAddr> for PeerSerializable {
    fn from(value: SocketAddr) -> Self {
        return Self {
            ip: value.ip(),
            port: value.port(),
        };
    }
}
