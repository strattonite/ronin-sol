use crate::ws_json::*;
use bincode::serialize;
use bs58::{decode, encode};
use crossbeam_channel;
use futures::{
    future::{self, join_all, FutureExt, RemoteHandle},
    task::Context,
};
use futures_util::{stream, task::Poll, SinkExt, StreamExt};
use serde_json::from_str;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    hash::Hash,
    pubkey::Pubkey,
    transaction::{Transaction, TransactionError},
};
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
// create client
// error handling in start function
// refactor start into struct?

#[allow(non_camel_case_types)]
#[derive(Debug, Error, Clone)]
pub enum TxError {
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
    TX(TxResult),
    ERROR(TxError),
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
type RxStream = futures_util::stream::PollFn<dyn FnMut(&mut Context) -> Poll<Option<Outgoing>>>;

async fn connect_websocket(url: &str) -> Result<Websocket, TxError> {
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
            .map_err(|e| WEBSOCKET_ERR(format!("could not connect to websocket: {:?}", e)))?;

    Ok(socket)
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

// signature subscription yields future?
// use remote_handle for signature subscriptions
// use async channel for account and program subscriptions e.g pub fn subscribe(id) -> Receiver

pub struct TpuClient {
    handle: JoinHandle<()>,
    tx: crossbeam_channel::Sender<Incoming>,
    rx: crossbeam_channel::Receiver<Outgoing>,
}

impl TpuClient {
    pub async fn new(rpc_url: &str, socket: &str, ws_url: &str) -> Result<Self, TxError> {
        let (tx_o, rx_o) = crossbeam_channel::unbounded();
        let (tx_i, rx_i) = crossbeam_channel::unbounded();

        let websocket = connect_websocket(ws_url).await?;
        let udp = Arc::new(
            UdpSocket::bind(socket)
                .await
                .map_err(|e| UDP_ERROR(e.to_string()))?,
        );

        let rpc = RpcClient::new(rpc_url.to_string());
        let tx2 = tx_o.clone();
        let handle = tokio::spawn(async move {
            let res = start(rpc, udp, websocket, tx_o, rx_i).await;
            if let Err(e) = res {
                tx2.send(ERROR(e)).unwrap();
            }
        });

        Ok(TpuClient {
            handle,
            tx: tx_i,
            rx: rx_o,
        })
    }

    // pub fn confirm_tx(&self, hash: Hash) -> RemoteHandle<Result<(), TransactionError>> {
    //     let rx2 = self.rx.clone();
    //     self.tx
    //         .send(SUBS(ManageSubs::SUBSCRIBE(SubRequest::SIGNATURE(
    //             encode(hash.to_bytes()).into_string(),
    //         ))))
    //         .unwrap();
    //     let f = future::poll_fn(|_| match rx2.try_recv() {
    //         Ok(o) => Poll::Pending,
    //         Err(_) => Poll::Pending,
    //     });
    // }

    pub fn send_txs(txs: Vec<Transaction>) {}
}

enum StreamRes {
    WS(Notification),
    CLIENT(Incoming),
}

use StreamRes::*;

async fn send_txs(
    udp: Arc<UdpSocket>,
    txs: Vec<(Vec<u8>, [u8; 32])>,
    leader: SocketAddr,
) -> Vec<TxResult> {
    join_all(txs.iter().map(|(tx, hash)| async {
        let l = tx.len();
        (
            *hash,
            udp.send_to(tx, leader)
                .await
                .or_else(|x| Err(UDP_ERROR(x.to_string())))
                .and_then(|x| {
                    if x == l {
                        Ok(())
                    } else {
                        Err(UDP_ERROR(format!("only {} of {} bytes sent", x, l)))
                    }
                }),
        )
    }))
    .await
}

async fn start(
    rpc: RpcClient,
    udp: Arc<UdpSocket>,
    websocket: Websocket,
    tx: crossbeam_channel::Sender<Outgoing>,
    rx: crossbeam_channel::Receiver<Incoming>,
) -> Result<(), TxError> {
    let mut slot = rpc.get_slot().await.map_err(|e| RPC_ERR(e.to_string()))?;
    let nodes = get_node_map(&rpc).await?;
    let (mut leaders, mut max_slot) = get_schedule(&rpc, &nodes, slot).await?;

    let mut subscriptions = Vec::<Subscription>::new();
    let mut tx_schedule = HashMap::<u64, Vec<(Vec<u8>, [u8; 32])>>::new();
    let mut sub_reqs = VecDeque::<ManageSubs>::new();
    let mut current_req = None::<ManageSubs>;
    let mut stopping = false;

    let client_stream = stream::poll_fn(|_| {
        return match rx.try_recv() {
            Ok(i) => Poll::Ready(Some(Ok(CLIENT(i)))),
            Err(_) => Poll::Pending,
        };
    });

    let (mut ps, ws) = websocket.split();
    let ws_stream = ws.map(|m| {
        Ok(WS(
            Notification::from_msg(&m.unwrap()).map_err(|e| WEBSOCKET_ERR(e.to_string()))?
        ))
    });
    let (ps_tx, mut ps_rx) = unbounded_channel::<String>();

    let ps_handle = tokio::spawn(async move {
        while let Some(m) = ps_rx.recv().await {
            ps.send(Message::Text(m)).await.unwrap();
        }
    });

    let mut joint_stream = stream::select(client_stream, ws_stream);

    let submit_txs = |txs, s, lds: &Leaders| -> Result<(), TxError> {
        let u = udp.clone();
        let l = *lds.get(&s).ok_or(NO_LEADER_FOUND(s))?;
        let _tx = tx.clone();
        tokio::spawn(async move {
            let mut res = send_txs(u, txs, l).await;
            for r in res.drain(..) {
                _tx.send(TX(r)).unwrap();
            }
        });
        Ok(())
    };

    while let Some(i) = joint_stream.next().await {
        match i? {
            CLIENT(incoming) => match incoming {
                TXS(mut txs, slt) => match slt {
                    Some(n) => {
                        if let Some(v) = tx_schedule.get_mut(&n) {
                            v.append(&mut txs);
                        } else {
                            tx_schedule.insert(n, txs);
                        }
                    }
                    None => {
                        submit_txs(txs, slot, &leaders)?;
                    }
                },
                STOP => {
                    sub_reqs.clear();
                    tx_schedule.clear();
                    for sub in subscriptions.drain(..) {
                        sub_reqs.push_back(ManageSubs::UNSUBSCRIBE(sub))
                    }
                    stopping = true;
                }
                SUBS(req) => match current_req {
                    Some(_) => {
                        sub_reqs.push_back(req);
                    }
                    None => {
                        ps_tx.send(req.to_json()).unwrap();
                        current_req = Some(req);
                    }
                },
            },
            WS(notification) => match &notification {
                SLOT(sn) => {
                    slot = sn.params.result.slot;
                    if slot >= max_slot {
                        (leaders, max_slot) = get_schedule(&rpc, &nodes, slot).await?;
                    }
                    if let Some(txs) = tx_schedule.remove(&slot) {
                        submit_txs(txs, slot, &leaders)?;
                    }
                }
                SUBSCRIPTION(sr) => {
                    if let Some(ManageSubs::SUBSCRIBE(req)) = current_req {
                        let sub = match req {
                            SubRequest::SLOT => Subscription::SLOT(sr.id),
                            SubRequest::PROGRAM(p) => Subscription::PROGRAM(sr.id, p),
                            SubRequest::ACCOUNT(p) => Subscription::ACCOUNT(sr.id, p),
                            SubRequest::SIGNATURE(p) => Subscription::SIGNATURE(sr.id, p),
                        };
                        subscriptions.push(sub.clone());
                        tx.send(SUBSCRIBED(sub)).unwrap();

                        if let Some(req) = sub_reqs.pop_front() {
                            ps_tx.send(req.to_json()).unwrap();
                            current_req = Some(req);
                        } else {
                            current_req = None;
                        }
                    }
                }
                UNSUBSCRIBED(_) => {
                    if let Some(ManageSubs::UNSUBSCRIBE(us)) = current_req {
                        for (i, s) in subscriptions.iter().enumerate() {
                            if s.id() == us.id() {
                                subscriptions.remove(i);
                                break;
                            }
                        }
                    }
                    if let Some(req) = sub_reqs.pop_front() {
                        ps_tx.send(req.to_json()).unwrap();
                        current_req = Some(req);
                    } else {
                        current_req = None;
                        if subscriptions.len() == 0 && stopping {
                            ps_handle.abort();
                            break;
                        }
                    }
                }
                SIGNATURE(sig) => {
                    for (i, s) in subscriptions.iter().enumerate() {
                        if s.id() == sig.params.subscription {
                            subscriptions.remove(i);
                            break;
                        }
                    }
                    tx.send(NOTIF(notification)).unwrap();
                }
                _ => {
                    tx.send(NOTIF(notification)).unwrap();
                }
            },
        }
    }
    Ok(())
}
