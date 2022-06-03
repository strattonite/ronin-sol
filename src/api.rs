use base64::decode;
use ed25519_dalek::{PublicKey, Signature};
use futures_util::stream::StreamExt;
use rustls_pemfile::{certs, rsa_private_keys};
use solana_sdk::pubkey::Pubkey;
use std::{
    convert::TryInto,
    fs::File,
    io::{self, BufRead, BufReader},
    path::Path,
    sync::Arc,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{
    rustls::{Certificate, PrivateKey, ServerConfig},
    TlsAcceptor,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[derive(Debug)]
pub enum RpcMethod {
    GET_WALLET(Pubkey),
    CREATE_WALLET,
    SEND(Vec<(Pubkey, u64)>),
    CREATE_TASK(Pubkey),
    START_TASK(u64),
    STOP_TASK(u64),
    TASK_STATUS(u64),
    ANALYTICS(u8),
    AUTHORISE(PublicKey, Signature),
}

#[derive(Debug)]
pub enum RpcDecodeError {
    INVALID_COMMAND,
    INVALID_DATA_LENGTH,
}

use RpcDecodeError::*;
use RpcMethod::*;

fn load_certs(path: &Path) -> io::Result<Vec<Certificate>> {
    certs(&mut BufReader::new(File::open(path)?))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid cert"))
        .map(|mut certs| certs.drain(..).map(Certificate).collect())
}

fn load_keys(path: &Path) -> io::Result<Vec<PrivateKey>> {
    let reader = BufReader::new(File::open(path)?);
    let lines: Vec<String> = reader.lines().map(|l| l.unwrap()).collect();
    let der = decode(lines[1..lines.len() - 1].join("")).unwrap();
    Ok(vec![PrivateKey(der)])
}

pub async fn start_api(
    cert_path: &Path,
    key_path: &Path,
    socket_addr: &str,
    keys: Arc<Vec<[u8; 32]>>,
) {
    let certificates = load_certs(cert_path).unwrap();
    let mut private_keys = load_keys(key_path).unwrap();

    let conf = ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(certificates, private_keys.remove(0))
        .unwrap();

    let acceptor = TlsAcceptor::from(Arc::new(conf));
    let listener = TcpListener::bind(socket_addr).await.unwrap();

    loop {
        let (tcp, addr) = listener.accept().await.unwrap();
        println!("connected to {}", addr);
        tokio::spawn(handle_tcp(tcp, acceptor.clone()));
    }
}

async fn handle_tcp(tcp: TcpStream, acceptor: TlsAcceptor) {
    if let Ok(tls) = acceptor.accept(tcp).await {
        if let Ok(mut ws_stream) = accept_async(tls).await {
            while let Some(Ok(data)) = ws_stream.next().await {
                match data {
                    Message::Binary(b) => println!("binary data: {:?}", b),
                    Message::Text(t) => println!("text data: {}", t),
                    _ => println!("other data received"),
                }
            }
        }
    }
}

pub fn decode_method(bytes: &[u8]) -> Result<RpcMethod, RpcDecodeError> {
    return match bytes[0] {
        0 => {
            if bytes.len() != 33 {
                return Err(INVALID_DATA_LENGTH);
            }
            Ok(GET_WALLET(Pubkey::new(&bytes[1..])))
        }
        1 => {
            if bytes.len() != 1 {
                return Err(INVALID_DATA_LENGTH);
            }
            Ok(CREATE_WALLET)
        }
        2 => {
            let l = bytes.len() - 1;
            if l % 40 != 0 {
                return Err(INVALID_DATA_LENGTH);
            }

            let m = l / 40;
            let mut v = Vec::<(Pubkey, u64)>::with_capacity(m);
            for n in 0..m {
                let s = (32 * n) + 1;
                let f = (32 * (n + 1)) + 1;
                v.push((
                    Pubkey::new(&bytes[s..f]),
                    u64::from_le_bytes(bytes[f..(f + 8)].try_into().unwrap()),
                ));
            }
            Ok(SEND(v))
        }
        3 => {
            if bytes.len() != 33 {
                return Err(INVALID_DATA_LENGTH);
            }
            Ok(CREATE_TASK(Pubkey::new(&bytes[1..])))
        }
        4 => {
            if bytes.len() != 9 {
                return Err(INVALID_DATA_LENGTH);
            }
            Ok(START_TASK(u64::from_le_bytes(
                bytes[1..].try_into().unwrap(),
            )))
        }
        5 => {
            if bytes.len() != 9 {
                return Err(INVALID_DATA_LENGTH);
            }
            Ok(STOP_TASK(u64::from_le_bytes(
                bytes[1..].try_into().unwrap(),
            )))
        }
        6 => {
            if bytes.len() != 9 {
                return Err(INVALID_DATA_LENGTH);
            }
            Ok(TASK_STATUS(u64::from_le_bytes(
                bytes[1..].try_into().unwrap(),
            )))
        }
        7 => {
            if bytes.len() != 2 {
                return Err(INVALID_DATA_LENGTH);
            }
            Ok(ANALYTICS(bytes[1]))
        }
        _ => Err(INVALID_COMMAND),
    };
}
