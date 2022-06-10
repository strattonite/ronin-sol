use crate::ws_json::*;
use bincode::serialize;
use bs58::decode;
use futures::future::join_all;
use futures_util::{SinkExt, StreamExt};
use serde_json::from_str;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{hash::Hash, pubkey::Pubkey, transaction::Transaction};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    net::{TcpStream, UdpSocket},
    sync::{
        broadcast::{channel, Receiver, Sender},
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    },
    task::{spawn_blocking, JoinHandle},
    time::sleep,
};
use tokio_rustls::rustls::{ClientConfig, OwnedTrustAnchor, RootCertStore};
use tokio_tungstenite::{
    connect_async_tls_with_config, tungstenite::Message, Connector, MaybeTlsStream, WebSocketStream,
};
use url::Url;
use webpki_roots::TLS_SERVER_ROOTS;

// TODO:
// handle subscription request queue
//  create controller abstraction
//   check for TX's on every loop (not just slot)
//    send gracefull shutdown ready on tx channel

#[allow(non_camel_case_types)]
#[derive(Debug, Error, Clone)]
pub enum TxError {
    #[error("no leaders cached for slot {0}")]
    LEADER_SCHEDULE_OVERFLOW(u64),
    #[error("UDP error: {0}")]
    UDP_ERROR(String),
    #[error("Error updating leaders: {0}")]
    ERR_UPDATING_LEADERS(String),
    #[error("no leader found for slot: {0}")]
    NO_LEADER_FOUND(u64),
    #[error("error creating subscription: {0}")]
    SUBSCRIPTION_ERR(String),
    #[error("websocket error: {0}")]
    WEBSOCKET_ERR(String),
    #[error("error deserializing json")]
    DESERIALIZE_ERR,
    #[error("rpc error: {0}")]
    RPC_ERR(String),
    #[error("pubsub listener error: {0}")]
    PUBSUB_ERR(String),
}

use TxError::*;

#[allow(non_camel_case_types)]
#[derive(Debug, Clone)]
pub enum Incoming {
    TXS(Vec<(Vec<u8>, [u8; 32])>, Option<u64>),
    STOP,
    SUBS(ManageSubs),
}

#[derive(Debug, Clone)]
pub enum Outgoing {
    NOTIF(Notification),
    SUBSCRIBED(Subscription),
    ERROR(TxError),
    TX(TxResult),
    STOPPED,
}

#[derive(Debug, Clone)]
pub enum ManageSubs {
    SUBSCRIBE(SubRequest),
    UNSUBSCRIBE(Subscription),
}

impl JsonSerialize for ManageSubs {
    fn to_json(&self) -> String {
        match self {
            ManageSubs::SUBSCRIBE(s) => s.to_json(),
            ManageSubs::UNSUBSCRIBE(u) => u.to_json(),
        }
    }
}

use Incoming::*;
use Notification::*;
use Outgoing::*;

type Nodes = HashMap<Pubkey, SocketAddr>;
type Leaders = HashMap<u64, SocketAddr>;
type Websocket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type TxResult = ([u8; 32], Result<(), TxError>);

async fn connect_websocket(url: &str) -> Websocket {
    let mut root = RootCertStore::empty();
    root.add_server_trust_anchors(TLS_SERVER_ROOTS.0.iter().map(|ta| {
        OwnedTrustAnchor::from_subject_spki_name_constraints(
            ta.subject,
            ta.spki,
            ta.name_constraints,
        )
    }));
    let conf = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root)
        .with_no_client_auth();

    let connector = Connector::Rustls(Arc::new(conf));
    let (socket, _) =
        connect_async_tls_with_config(Url::parse(url).unwrap(), None, Some(connector))
            .await
            .unwrap();

    socket
}

async fn get_node_map(rpc: &RpcClient) -> Result<Nodes, TxError> {
    let nodes = rpc
        .get_cluster_nodes()
        .await
        .map_err(|e| RPC_ERR(e.to_string()))?;

    let mut map = HashMap::new();
    for node in nodes.iter() {
        if let Some(socket) = node.tpu {
            let pk = Pubkey::new(&decode(&node.pubkey).into_vec().unwrap());
            map.insert(pk, socket);
        }
    }

    Ok(map)
}

async fn get_schedule(
    rpc: &RpcClient,
    nodes: &Nodes,
    slot: u64,
) -> Result<(Leaders, u64), TxError> {
    let mut map = HashMap::new();
    let leaders = rpc
        .get_slot_leaders(slot, 5000)
        .await
        .map_err(|e| ERR_UPDATING_LEADERS(e.to_string()))?;

    for i in 0..leaders.len() {
        let l = leaders.get(i).ok_or(NO_LEADER_FOUND(i as u64))?;
        let s = nodes.get(&l).ok_or(NO_LEADER_FOUND(i as u64))?;
        map.insert(i as u64 + slot, *s);
    }

    Ok((map, slot + leaders.len() as u64))
}

pub struct TpuClient {
    handle: JoinHandle<Result<(), TxError>>,
    tx: Sender<Incoming>,
    rx: Receiver<Outgoing>,
    tx_o: Sender<Outgoing>,
}

impl TpuClient {
    pub fn new(socket: &'static str, ws_url: &'static str, rpc_url: &str) -> Self {
        let rpc = RpcClient::new(rpc_url.to_string());
        let (tx_o, rx_o) = channel(100);
        let (tx_i, rx_i) = channel(100);

        let tx2 = tx_o.clone();
        let handle = tokio::spawn(async move { start(rpc, socket, ws_url, tx2, rx_i).await });

        TpuClient {
            handle,
            tx: tx_i,
            rx: rx_o,
            tx_o,
        }
    }

    pub async fn submit_txs(
        &mut self,
        txs: Vec<Transaction>,
        slot: Option<u64>,
    ) -> Result<Vec<Hash>, TxError> {
        let hashes = txs
            .iter()
            .map(|tx| tx.verify_and_hash_message().unwrap())
            .collect();
        let t = txs
            .iter()
            .map(|tx| {
                (
                    serialize(tx).unwrap(),
                    tx.verify_and_hash_message().unwrap().to_bytes(),
                )
            })
            .collect();

        self.tx.send(TXS(t, slot)).unwrap();

        Ok(hashes)
    }
}

async fn start(
    rpc: RpcClient,
    socket: &str,
    ws_url: &str,
    tx: Sender<Outgoing>,
    mut rx: Receiver<Incoming>,
) -> Result<(), TxError> {
    let mut pubsub = connect_websocket(ws_url).await;
    let udp = Arc::new(UdpSocket::bind(socket).await.unwrap());
    let _s = rpc.get_slot().await.map_err(|e| RPC_ERR(e.to_string()))?;
    let slot = Arc::new(Mutex::new(_s));
    let nodes = get_node_map(&rpc).await?;
    let (mut leaders, mut max_slot) = get_schedule(&rpc, &nodes, _s).await?;

    let subscriptions = Arc::new(Mutex::new(Vec::<Subscription>::new()));
    let tx_schedule = Arc::new(Mutex::new(HashMap::<u64, Vec<(Vec<u8>, [u8; 32])>>::new()));
    let sub_reqs = Arc::new(Mutex::new(VecDeque::<ManageSubs>::new()));
    let stopping = Arc::new(Mutex::new(false));
    let mut current_req = None::<ManageSubs>;

    let t = tx_schedule.clone();
    let r = sub_reqs.clone();
    let s = subscriptions.clone();
    let st = stopping.clone();
    let slt = slot.clone();

    let handle = tokio::spawn(async move {
        while let Ok(i) = rx.recv().await {
            match i {
                TXS(mut txs, s) => {
                    let mut tx_s = t.lock().unwrap();
                    let sl = match s {
                        Some(n) => n,
                        None => *slt.lock().unwrap() + 1,
                    };
                    if let Some(v) = tx_s.get_mut(&sl) {
                        v.append(&mut txs);
                    } else {
                        tx_s.insert(sl, txs);
                    }
                }
                STOP => {
                    let mut reqs = r.lock().unwrap();
                    let mut schedule = t.lock().unwrap();
                    reqs.clear();
                    schedule.clear();
                    for sub in s.lock().unwrap().iter() {
                        reqs.push_back(ManageSubs::UNSUBSCRIBE(sub.clone()))
                    }
                    *st.lock().unwrap() = true;
                    break;
                }
                SUBS(m) => {
                    let mut reqs = r.lock().unwrap();
                    reqs.push_back(m);
                }
            }
        }
    });

    while let Some(ws_msg) = pubsub.next().await {
        let msg = Notification::from_msg(
            &ws_msg.map_err(|e| WEBSOCKET_ERR(format!("error reading from websocket: {:?}", e)))?,
        )
        .map_err(|e| WEBSOCKET_ERR(format!("could not deserialize websocket message: {:?}", e)))?;

        match &msg {
            SLOT(sn) => {
                let _s = {
                    let mut s = slot.lock().unwrap();
                    *s = sn.params.result.slot;
                    *s
                };
                if _s >= max_slot - 1 {
                    (leaders, max_slot) = get_schedule(&rpc, &nodes, _s).await?;
                }
            }
            SUBSCRIPTION(sub) => {
                if let Some(ManageSubs::SUBSCRIBE(sr)) = current_req {
                    let sub = match sr {
                        SubRequest::SLOT => Subscription::SLOT(sub.id),
                        SubRequest::PROGRAM(p) => Subscription::PROGRAM(sub.id, p),
                        SubRequest::ACCOUNT(p) => Subscription::ACCOUNT(sub.id, p),
                        SubRequest::SIGNATURE(p) => Subscription::SIGNATURE(sub.id, p),
                    };
                    subscriptions.lock().unwrap().push(sub.clone());
                    tx.send(SUBSCRIBED(sub)).unwrap();
                    current_req = None;
                }
            }
            UNSUBSCRIBED(_) => {
                if let Some(ManageSubs::UNSUBSCRIBE(us)) = current_req {
                    let mut subs = subscriptions.lock().unwrap();
                    for (i, s) in subs.iter().enumerate() {
                        if s.id() == us.id() {
                            subs.remove(i);
                            break;
                        }
                    }
                    current_req = None;
                }
            }
            _ => {
                tx.send(NOTIF(msg)).unwrap();
            }
        }

        {
            let (tx_o, leader) = {
                let s = slot.lock().unwrap();
                match tx_schedule.lock().unwrap().remove(&s) {
                    Some(ts) => (
                        Some(ts),
                        Some(leaders.get(&s).expect("could not get leader for slot")),
                    ),
                    None => (None, None),
                }
            };
            if let Some(txs) = tx_o {
                join_all(txs.iter().map(|(_tx, hash)| async {
                    let l = _tx.len();
                    let res = (
                        *hash,
                        udp.send_to(_tx, *leader.unwrap())
                            .await
                            .or_else(|x| Err(UDP_ERROR(x.to_string())))
                            .and_then(|x| {
                                if x == l {
                                    Ok(())
                                } else {
                                    Err(UDP_ERROR(format!("only {} of {} bytes sent", x, l)))
                                }
                            }),
                    );
                    tx.send(TX(res)).unwrap();
                }))
                .await;
            }
        }

        match current_req {
            Some(_) => {}
            None => {
                let p = sub_reqs.lock().unwrap().pop_front();
                if let Some(r) = p {
                    pubsub
                        .send(Message::Text(r.to_json()))
                        .await
                        .map_err(|e| WEBSOCKET_ERR(format!("error sending request: {:?}", e)))?;
                    current_req = Some(r);
                } else if *stopping.lock().unwrap() {
                    tx.send(STOPPED).unwrap();
                    break;
                }
            }
        }
    }

    handle.abort();
    Ok(())
}
