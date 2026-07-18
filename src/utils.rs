use std::{io::Error, net::SocketAddr, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    select,
    time::Sleep,
};

use crate::{block::Block, network_message::{NetworkMessageReq, NetworkMessageRes}, peer_serializable::PeerSerializable};

pub async fn send_packet_and_wait(
    stream: &mut TcpStream,
    packet: NetworkMessageReq,
) -> Result<NetworkMessageRes, NetError> {
    let _sent = send_packet_req(stream, packet).await?;

    select! {
        _ = stream.readable() => {
            let mut buff = [0;1024];
            stream.read(&mut buff).await?;
            let message = wincode::deserialize::<NetworkMessageRes>(&buff).expect("Error deserializing message");
            return Ok(message);
        }
        _ = timeout() => {
            return Err(NetError::Timeout);
        }
    }
}

pub fn timeout() -> Sleep {
    return tokio::time::sleep(Duration::new(5, 0));
}

pub async fn open_stream(remote_peer: &PeerSerializable) -> Result<TcpStream, Error> {
    let stream = TcpStream::connect(SocketAddr::new(remote_peer.ip, remote_peer.port)).await?;
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

impl From<std::io::Error> for NetError {
    fn from(value: std::io::Error) -> Self {
        return NetError::IoError(value);
    }
}

#[allow(dead_code)]
pub fn save_chain_to_file(chain: &[Block], filename: &str) -> Result<(), Error> {
    use std::fs::File;
    use std::io::Write;

    let mut file = File::create(filename)?;

    for block in chain {
        let line = format!(
            "Block {}: data={:?}, prev_hash={:?}, nonce={:?}, hash={:?}\n",
            block.idx,
            block.data,
            hex::encode(&block.prev_hash),
            block.nonce,
            hex::encode(&block.hash)
        );
        file.write_all(line.as_bytes())?;
    }

    Ok(())
}
