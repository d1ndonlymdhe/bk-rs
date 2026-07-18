use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
};
use wincode::{SchemaRead, SchemaWrite};

use crate::{
    block::{Block, validate_chain, validate_chain_addition},
    utils::{NetError, open_stream, peer_exists, send_packet_and_wait, timeout},
};


