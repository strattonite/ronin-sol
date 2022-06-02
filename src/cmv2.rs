use crate::state::*;
use crate::wallet::*;
use crate::{log, TransactionRunner};
use borsh::BorshDeserialize;
use bs58::decode;
use chrono::NaiveDateTime;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature, signer::Signer,
};
use std::{
    fmt,
    str::FromStr,
    thread,
    time::{Duration, SystemTime},
};

pub const PRELOAD_OFFSET: u64 = 10;

pub struct MintTask<'a> {
    tx_runner: &'a TransactionRunner,
    rpc: &'a RpcClient,
    wallets: Vec<MintWallet>,
}

pub fn run_drop(
    candy_id: String,
    wallets: Vec<MintWallet>,
    rpc: &RpcClient,
    tx_runner: &mut TransactionRunner,
) {
    let mut drop = MintDrop::new(&rpc, candy_id);
    let set_collection = drop.set_collection_on_mint(&rpc);
    log(format!("found CMV2 drop:\n{}", drop));

    log("initialising wallets".to_string());
    let block = rpc.get_latest_blockhash().unwrap();
    let init_txs = wallets
        .iter()
        .map(|w| w.init(block.clone(), &w.main_pair))
        .collect();

    tx_runner.refresh(init_txs);
    tx_runner.send_and_confirm();
    let s = tx_runner.success.len();

    log(format!(
        "initialised wallets:\nsuccess - {}\nfailed - {}",
        s,
        tx_runner.failed.len()
    ));
    if s == 0 {
        return;
    }
    // filter out failed wallets
    let mint_wallets: Vec<&MintWallet> = wallets
        .iter()
        .filter(|w| {
            match rpc
                .get_signatures_for_address(&w.main_pair.pubkey())
                .unwrap()
                .get(0)
            {
                Some(sig) => {
                    let sig = Signature::from_str(&sig.signature).unwrap();
                    for t in &tx_runner.success {
                        if sig == *t {
                            return true;
                        }
                    }
                    return false;
                }
                None => return false,
            }
        })
        .collect();

    log(format!(
        "waiting for mint with {} wallets",
        mint_wallets.len()
    ));

    if let Some(d) = drop
        .duration_until_drop()
        .checked_sub(Duration::from_secs(PRELOAD_OFFSET))
    {
        log(format!("sleeping for {} seconds", d.as_secs()));
        thread::sleep(d);
    }

    let block = rpc.get_latest_blockhash().unwrap();
    let mint_txs = wallets
        .iter()
        .map(|w| w.mint_one(block, &drop, set_collection))
        .collect();
    tx_runner.refresh(mint_txs);

    let d = drop.duration_until_drop();
    log(format!("sleeping for {} seconds", d.as_secs()));
    thread::sleep(d);

    log("attempting to mint".to_string());
    tx_runner.send_and_confirm();
    let s = tx_runner.success.len();
    let f = tx_runner.failed.len();
    let l: Vec<String> = tx_runner
        .success
        .iter()
        .map(|sig| format!("https://solscan.io/tx/{}", sig))
        .collect();
    log(format!(
        "[MINT-RESULT]    SUCCESS - {}  |  FAILED - {}",
        s, f
    ));
    println!("{}", l.join("\n"));
}

#[derive(Debug)]
pub struct MintDrop {
    pub candy_id: Pubkey,
    candy_machine: CandyMachine,
    collection_pda: Option<CollectionPDA>,
}

impl MintDrop {
    pub fn new(rpc: &RpcClient, candy_id: String) -> Self {
        let candy_id = Pubkey::new(&decode(candy_id).into_vec().unwrap());
        let acc = rpc.get_account(&candy_id).unwrap();
        let candy_machine = CandyMachine::deserialize(&mut &acc.data[8..]).unwrap();

        MintDrop {
            candy_id,
            candy_machine,
            collection_pda: None,
        }
    }

    fn collection_pda(&self) -> Pubkey {
        let (pk, _) = Pubkey::find_program_address(
            &[b"collection", &self.candy_id.to_bytes()],
            &CANDY_PROGRAM,
        );
        pk
    }

    fn collection_authority_pda(&self, mint: &Pubkey, new_auth: &Pubkey) -> Pubkey {
        let (pk, _) = Pubkey::find_program_address(
            &[
                b"metadata",
                &METADATA_PROGRAM.to_bytes(),
                &mint.to_bytes(),
                b"collection_authority",
                &new_auth.to_bytes(),
            ],
            &METADATA_PROGRAM,
        );
        pk
    }

    fn master_edition(&self, mint: &Pubkey) -> Pubkey {
        let (pk, _) = Pubkey::find_program_address(
            &[
                b"metadata",
                &METADATA_PROGRAM.to_bytes(),
                &mint.to_bytes(),
                b"edition",
            ],
            &METADATA_PROGRAM,
        );
        pk
    }

    fn metadata(&self, mint: &Pubkey) -> Pubkey {
        let (pk, _) = Pubkey::find_program_address(
            &[b"metadata", &METADATA_PROGRAM.to_bytes(), &mint.to_bytes()],
            &METADATA_PROGRAM,
        );
        pk
    }

    fn creator(&self) -> (Pubkey, u8) {
        Pubkey::find_program_address(
            &[b"candy_machine", &self.candy_id.to_bytes()],
            &CANDY_PROGRAM,
        )
    }

    pub fn get_mint_accounts(&self, mint: &Pubkey) -> MintAccounts {
        let (creator, creator_bump) = self.creator();
        MintAccounts {
            candy_machine: self.candy_id.clone(),
            creator,
            creator_bump,
            wallet: self.candy_machine.wallet.clone(),
            metadata: self.metadata(mint),
            master_edition: self.master_edition(mint),
        }
    }

    pub fn get_collection_accounts(&self, mint: &Pubkey) -> CollectionAccounts {
        let collection_mint = self
            .collection_pda
            .as_ref()
            .expect("collection PDA not fetched")
            .mint
            .clone();
        let collection_pda = self.collection_pda();
        let collection_authority_record =
            self.collection_authority_pda(&collection_mint, &collection_pda);
        let collection_metadata = self.metadata(&collection_mint);
        let collection_master_edition = self.master_edition(&collection_mint);

        CollectionAccounts {
            candy_machine: self.candy_id.clone(),
            metadata: self.metadata(mint),
            authority: self.candy_machine.authority.clone(),
            collection_pda,
            collection_mint,
            collection_metadata,
            collection_master_edition,
            collection_authority_record,
        }
    }

    pub fn set_collection_on_mint(&mut self, rpc: &RpcClient) -> bool {
        if !self.candy_machine.data.retain_authority {
            return false;
        }
        let conf = CommitmentConfig::processed();
        let res = rpc
            .get_account_with_commitment(&self.collection_pda(), conf)
            .unwrap();
        match res.value {
            Some(acc) => {
                self.collection_pda = Some(CollectionPDA::deserialize(&mut &acc.data[..]).unwrap());
                return true;
            }
            None => return false,
        }
    }

    pub fn duration_until_drop(&self) -> Duration {
        return match self.candy_machine.data.go_live_date {
            Some(d) => {
                let n = SystemTime::now();
                let l = SystemTime::UNIX_EPOCH
                    .checked_add(Duration::from_secs(d as u64))
                    .unwrap();
                if n > l {
                    return Duration::from_secs(0);
                } else {
                    return l.duration_since(n).unwrap();
                }
            }
            None => Duration::from_secs(0),
        };
    }
}

impl fmt::Display for MintDrop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let md = match self.candy_machine.data.go_live_date {
            Some(l) => NaiveDateTime::from_timestamp(l, 0)
                .format("%v %k:%M:%S")
                .to_string(),
            None => String::from("n/a"),
        };

        write!(
            f,
            "[CMV2 DROP]\n    UUID - {}\n    SUPPLY - {}\n    MINT_DATE - {}\n    PRICE - {} SOL",
            self.candy_machine.data.uuid,
            self.candy_machine.data.items_available,
            md,
            self.candy_machine.data.price as f64 / 1000_000_000 as f64
        )
    }
}

pub struct MintAccounts {
    pub candy_machine: Pubkey,
    pub creator: Pubkey,
    pub creator_bump: u8,
    pub wallet: Pubkey,
    pub metadata: Pubkey,
    pub master_edition: Pubkey,
}

pub struct CollectionAccounts {
    pub candy_machine: Pubkey,
    pub metadata: Pubkey,
    pub authority: Pubkey,
    pub collection_pda: Pubkey,
    pub collection_mint: Pubkey,
    pub collection_metadata: Pubkey,
    pub collection_master_edition: Pubkey,
    pub collection_authority_record: Pubkey,
}
