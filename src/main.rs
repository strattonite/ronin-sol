use chrono::Utc;
use futures_util::stream::StreamExt;
use serde::Deserialize;
use serde_json::from_str;
use solana_client::{rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig};
use solana_sdk::{signature::Signature, signer::Signer, transaction::Transaction};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_rustls::{server::TlsStream, TlsAcceptor};
use tokio_tungstenite::accept_async;

pub mod api;
pub mod cmv2;
pub mod state;
pub mod tpu_client;
pub mod wallet;
pub mod ws_json;

use api::*;
use cmv2::*;

const POLL_DELAY_MS: u64 = 100;

pub struct TransactionRunner {
    rpc: RpcClient,
    txs: Vec<Transaction>,
    pub success: Vec<Signature>,
    pub failed: Vec<Signature>,
    processing: Vec<Signature>,
}

impl TransactionRunner {
    pub fn new(txs: Vec<Transaction>, rpc: RpcClient) -> Self {
        TransactionRunner {
            rpc,
            txs,
            success: vec![],
            failed: vec![],
            processing: vec![],
        }
    }

    fn submit(&mut self) {
        let c = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: None,
            encoding: None,
            max_retries: None,
            min_context_slot: None,
        };
        self.processing = self
            .txs
            .iter()
            .map(|t| self.rpc.send_transaction_with_config(&t, c).unwrap())
            .collect();
    }

    fn poll(&mut self) -> bool {
        let l = self.processing.len();
        if l == 0 {
            return true;
        } else {
            let mut p2 = Vec::<Signature>::with_capacity(l);
            for sig in self.processing.iter() {
                match self.rpc.get_signature_status(sig).unwrap() {
                    Some(s) => match s {
                        Ok(()) => self.success.push(sig.clone()),
                        Err(_) => self.failed.push(sig.clone()),
                    },
                    None => p2.push(sig.clone()),
                }
            }
            self.processing = p2;
            return false;
        }
    }

    pub fn send_and_confirm(&mut self) {
        self.submit();
        let mut finished = false;
        while !finished {
            thread::sleep(Duration::from_millis(POLL_DELAY_MS));
            finished = self.poll();
        }
    }

    pub fn refresh(&mut self, txs: Vec<Transaction>) {
        self.txs = txs;
        self.processing = vec![];
        self.success = vec![];
        self.failed = vec![];
    }
}

pub fn log(msg: String) {
    let now = Utc::now().format("%k:%M:%S-%3f");
    println!("[{}]    [RONIN-SOL]    {}", now, msg);
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // let cert_path = Path::new("./ssl/server.crt");
    // let key_path = Path::new("./ssl/server.key");
    // let keys = Arc::new(Vec::new());
    // start_api(cert_path, key_path, "45.143.147.119:443", keys).await;
    // Ok(())

    let v = vec![ws_json::Subscription::PROGRAM(
        10,
        "hello world".to_string(),
    )];
    let el = ws_json::Subscription::PROGRAM(10, "hello".to_string());

    println!("{}", matches!(v.get(0).unwrap(), el));

    Ok(())

    // let candy_id = String::from("GFxvJ8p2LLXWsZELbPEmTFENrjfmywDpAzvK9nn7VC1w");
    // let rpc = RpcClient::new("https://api.devnet.solana.com");

    // let mut tx_runner = TransactionRunner::new(vec![], rpc);
    // let rpc = RpcClient::new("https://api.devnet.solana.com");

    // let cf = File::open("./testing/config.json").unwrap();
    // let mut j = String::new();
    // let reader = BufReader::new(cf);
    // for chunk in reader.lines() {
    //     j += &chunk.unwrap();
    // }
    // let conf: ConfigJSON = from_str(&j).unwrap();
    // let funding = wallet::MintWallet::from_json(conf.wallet);

    // let block = rpc.get_latest_blockhash().unwrap();
    // let b = rpc.get_balance(&funding.main_pair.pubkey()).unwrap();
    // log(format!(
    //     "funding account address: {}",
    //     funding.main_pair.pubkey()
    // ));
    // log(format!("funding account balance: {}", b));

    // let mw1 = wallet::MintWallet::new();
    // let mw2 = wallet::MintWallet::new();
    // let tx = wallet::MintWallet::fund_many(vec![&mw1, &mw2], block, &funding.main_pair, 125000000);
    // rpc.send_and_confirm_transaction(&tx).unwrap();
    // let b1 = rpc.get_balance(&mw1.main_pair.pubkey()).unwrap();
    // let b2 = rpc.get_balance(&mw2.main_pair.pubkey()).unwrap();
    // log(format!("funded wallet balances: {}, {}", b1, b2));

    // run_drop(candy_id, vec![mw1, mw2], &rpc, &mut tx_runner);
}

#[derive(Deserialize)]
struct ConfigJSON {
    wallet: wallet::WalletJson,
}
