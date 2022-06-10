use serde::Deserialize;
use serde_json::{from_str, from_value, Value};
use solana_sdk::transaction::TransactionError;
use thiserror::Error;
use tokio_tungstenite::tungstenite::Message;

#[derive(Deserialize, Debug, Clone)]
pub struct SignatureNotif {
    pub jsonrpc: String,
    pub method: String,
    pub params: SignatureParams,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SignatureParams {
    pub result: SignatureResult,
    pub subscription: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SignatureResult {
    pub context: Context,
    pub value: SignatureValue,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SignatureValue {
    pub err: Option<TransactionError>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ProgramNotif {
    pub jsonrpc: String,
    pub method: String,
    pub params: ProgramParams,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ProgramParams {
    pub result: ProgramResult,
    pub subscription: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ProgramResult {
    pub context: Context,
    pub value: ProgramValue,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ProgramValue {
    pub pubkey: String,
    pub account: AccountValue,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AccountNotif {
    pub jsonrpc: String,
    pub method: String,
    pub params: AccountParams,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AccountParams {
    pub result: AccountResult,
    pub subscription: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AccountResult {
    pub context: Context,
    pub value: AccountValue,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Context {
    pub slot: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AccountValue {
    pub data: Vec<String>,
    pub executable: bool,
    pub lamports: u64,
    pub owner: String,
    pub rentEpoch: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SlotNotif {
    pub jsonrpc: String,
    pub method: String,
    pub params: SlotParams,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SlotParams {
    pub result: SlotResult,
    pub subscription: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SlotResult {
    pub parent: u64,
    pub root: u64,
    pub slot: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SubResult {
    pub jsonrpc: String,
    pub result: u64,
    pub id: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct UnsubResult {
    pub jsonrpc: String,
    pub result: bool,
    pub id: u64,
}

#[derive(Debug, Clone)]
pub enum Notification {
    SLOT(SlotNotif),
    PROGRAM(ProgramNotif),
    ACCOUNT(AccountNotif),
    SIGNATURE(SignatureNotif),
    SUBSCRIPTION(SubResult),
    UNSUBSCRIBED(UnsubResult),
}

#[derive(Debug, Clone)]
pub enum Subscription {
    SLOT(u64),
    PROGRAM(u64, String),
    ACCOUNT(u64, String),
    SIGNATURE(u64, String),
}

impl Subscription {
    pub fn id(&self) -> u64 {
        match self {
            Subscription::SLOT(id) => *id,
            Subscription::PROGRAM(id, _) => *id,
            Subscription::ACCOUNT(id, _) => *id,
            Subscription::SIGNATURE(id, _) => *id,
        }
    }
}

pub trait JsonSerialize {
    fn to_json(&self) -> String;
}

impl JsonSerialize for Subscription {
    fn to_json(&self) -> String {
        let (method, id) = match self {
            Subscription::SLOT(id) => ("slotUnsubscribe", id),
            Subscription::PROGRAM(id, _) => ("programUnsubscribe", id),
            Subscription::ACCOUNT(id, _) => ("accountUnsubscribe", id),
            Subscription::SIGNATURE(id, _) => ("signatureUnsubscribe", id),
        };
        format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"{}\"\"params\":[{}]}}",
            method, id
        )
    }
}

#[derive(Debug, Clone)]
pub enum SubRequest {
    SLOT,
    PROGRAM(String),
    ACCOUNT(String),
    SIGNATURE(String),
}

impl JsonSerialize for SubRequest {
    fn to_json(&self) -> String {
        return match self {
            SubRequest::SLOT => {
                String::from("{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"slotSubscribe\"}}")
            }
            _ => {
                let (params, method) = match self {
                    SubRequest::SIGNATURE(s) => (format!("[{}]", s), "signatureSubscribe"),
                    SubRequest::ACCOUNT(s) => (
                        format!("[{},{{\"encoding\":\"base64\"}}]", s),
                        "accountSubscribe",
                    ),
                    SubRequest::PROGRAM(s) => (
                        format!("[{},{{\"encoding\":\"base64\"}}]", s),
                        "programSubscribe",
                    ),
                    _ => panic!(),
                };

                format!(
                    "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"{}\",\"params\":\"{}\"}}",
                    method, params
                )
            }
        };
    }
}

use Notification::*;

#[allow(non_camel_case_types)]
#[derive(Error, Debug)]
pub enum PubSubError {
    #[error("error deserializing json")]
    DESERIALIZE_ERR,
    #[error("ws received invalid data type: {0}")]
    INVALID_DATA_TYPE(String),
    #[error("unknown rpc method: {0}")]
    UNKNOWN_METHOD(String),
}

use PubSubError::*;

impl Notification {
    pub fn from_msg(msg: &Message) -> Result<Self, PubSubError> {
        return if let Message::Text(t) = msg {
            let v: Value = from_str(t).map_err(|_| DESERIALIZE_ERR)?;
            if let Value::String(s) = &v["method"] {
                return match s.as_str() {
                    "accountNotification" => Ok(ACCOUNT(from_value::<AccountNotif>(v).unwrap())),
                    "programNotification" => Ok(PROGRAM(from_value::<ProgramNotif>(v).unwrap())),
                    "slotNotification" => Ok(SLOT(from_value::<SlotNotif>(v).unwrap())),
                    "signatureNotification" => {
                        Ok(SIGNATURE(from_value::<SignatureNotif>(v).unwrap()))
                    }
                    _ => Err(UNKNOWN_METHOD(s.clone())),
                };
            } else {
                return match &v["result"] {
                    Value::Number(_) => Ok(SUBSCRIPTION(from_value::<SubResult>(v).unwrap())),
                    Value::Bool(_) => Ok(UNSUBSCRIBED(from_value::<UnsubResult>(v).unwrap())),
                    _ => Err(UNKNOWN_METHOD(v.to_string())),
                };
            }
        } else {
            Err(INVALID_DATA_TYPE(msg.to_string()))
        };
    }
}
