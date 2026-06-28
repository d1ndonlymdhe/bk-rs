use std::{io::Error, net::SocketAddr, time::Duration};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, select, time::Sleep};

use crate::types::{NetworkMessage, Peer, PeerSerializable};


pub async fn send_packet_and_wait(
    stream: &mut TcpStream,
    packet: NetworkMessage,
) -> Result<NetworkMessage, NetError> {
    let _sent = send_packet(stream, packet).await?;

    select! {
        _ = stream.readable() => {
            let mut buff = [0;1024];
            stream.read(&mut buff).await?;
            let message = wincode::deserialize::<NetworkMessage>(&buff).expect("Error deserializing message");
            return Ok(message);
        }
        _ = timeout() => {
            return Err(NetError::Timeout);
        }
    }
}

pub fn peer_exists(known_peers: &[Peer], peer: &PeerSerializable) -> bool {
    known_peers.iter().any(|p| p == peer)
}


pub fn timeout() -> Sleep {
    return tokio::time::sleep(Duration::new(5, 0));
}

pub async fn open_stream(remote_peer: &PeerSerializable) -> Result<TcpStream, Error> {
    let stream = TcpStream::connect(SocketAddr::new(remote_peer.ip, remote_peer.port)).await?;
    return Ok(stream);
}

pub async fn send_packet(
    // remote_peer: &PeerSerializable,
    stream: &mut TcpStream,
    packet: NetworkMessage,
) -> Result<usize, Error> {
    // let mut stream = TcpStream::connect(SocketAddr::new(remote_peer.ip, remote_peer.port)).await?;
    let message_bytes = wincode::serialize(&packet).expect("Error serializing packet");
    let p = stream.write(&message_bytes).await;
    return p;
}

pub enum NetError {
    #[allow(dead_code)]
    IoError(std::io::Error),
    Timeout,
}

impl From<std::io::Error> for NetError {
    fn from(value: std::io::Error) -> Self {
        return NetError::IoError(value);
    }
}
