use std::{io::Error, net::SocketAddr, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    select,
    time::Sleep,
};

use crate::{
    block::block::Block,
    net::network_message::{NetworkMessageReq, NetworkMessageRes},
    peer::peer::Peer,
};

pub async fn send_packet_and_wait(
    stream: &mut TcpStream,
    packet: NetworkMessageReq,
) -> Result<NetworkMessageRes, NetError> {
    let _sent = send_packet_req(stream, packet).await?;

    select! {
        _ = stream.readable() => {
            let mut buff = [0;1024];
            let mut final_buff = Vec::new();
            loop {
                let n = stream.read(&mut buff).await?;
                final_buff.extend_from_slice(&buff[..n]);
                if n < 1024 {
                    break;
                }
                // add a max limit?
            }
            let message = wincode::deserialize::<NetworkMessageRes>(&final_buff).expect("Error deserializing message");
            return Ok(message);
        }
        _ = timeout() => {
            return Err(NetError::Timeout);
        }
    }
}

pub fn timeout() -> Sleep {
    return tokio::time::sleep(Duration::new(15, 0));
}

pub async fn open_stream(remote_peer: &Peer) -> Result<TcpStream, Error> {
    let stream = TcpStream::connect(SocketAddr::new(remote_peer.ip, remote_peer.port)).await?;
    stream.set_zero_linger().unwrap();
    return Ok(stream);
}

pub async fn send_packet_req(
    stream: &mut TcpStream,
    packet: NetworkMessageReq,
) -> Result<usize, Error> {
    let message_bytes = wincode::serialize(&packet).expect("Error serializing packet");
    let p = stream.write(&message_bytes).await;
    return p;
}

pub async fn send_packet_res(
    stream: &mut TcpStream,
    packet: NetworkMessageRes,
) -> Result<usize, Error> {
    let message_bytes = wincode::serialize(&packet).expect("Error serializing packet");
    let p = stream.write(&message_bytes).await;
    return p;
}

pub enum NetError {
    #[allow(dead_code)]
    IoError(std::io::Error),
    Timeout,
}

impl NetError {
    pub fn to_string(self) -> String {
        return self.into();
    }
}

impl From<std::io::Error> for NetError {
    fn from(value: std::io::Error) -> Self {
        return NetError::IoError(value);
    }
}

impl Into<String> for NetError {
    fn into(self) -> String {
        return match self {
            NetError::IoError(error) => error.to_string(),
            NetError::Timeout => "Timed out".into(),
        };
    }
}

#[allow(dead_code)]
pub fn save_chain_to_file(
    chain: &[Block],
    filename: &str,
    self_peer: Peer,
    known_peers: &[Peer],
) -> Result<(), Error> {
    use std::fs::File;
    use std::io::Write;

    let mut file = File::create(filename)?;
    let line = format!("Self Peer: Ip: {} Port: {}\n", self_peer.ip, self_peer.port);
    file.write_all(line.as_bytes())?;
    for peer in known_peers {
        let line = format!("Known Peer: Ip: {} Port: {}\n", peer.ip, peer.port);
        file.write_all(line.as_bytes())?;
    }
    for block in chain {
        let line = format!(
            "Block {}: voter_id = {:?}, data={:?}, prev_hash={:?}, nonce={:?}, hash={:?}, timestamp={}\n",
            block.idx,
            block.voter_id,
            block.data,
            hex::encode(&block.prev_hash),
            block.nonce,
            hex::encode(&block.hash),
            block.timestamp
        );
        file.write_all(line.as_bytes())?;
    }

    Ok(())
}
