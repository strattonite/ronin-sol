use bs58::decode;
use lazy_static::lazy_static;
use serde::Deserialize;
use solana_sdk::{
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signer::{keypair, Signer},
    system_instruction, system_program, sysvar,
    transaction::Transaction,
};
use spl_associated_token_account::{
    get_associated_token_address as get_ata,
    instruction::create_associated_token_account as create_ata,
};
use spl_token::instruction as spl;

use crate::cmv2::*;

const MINT_SIZE: u64 = 82;

lazy_static! {
    pub static ref CANDY_PROGRAM: Pubkey = Pubkey::new(
        &decode("cndy3Z4yapfJBmL3ShUp5exZKqR3z33thTzeNMm2gRZ")
            .into_vec()
            .unwrap()
    );
    pub static ref METADATA_PROGRAM: Pubkey = Pubkey::new(
        &decode("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s")
            .into_vec()
            .unwrap()
    );
}

#[derive(Debug)]
pub struct MintWallet {
    pub main_pair: keypair::Keypair,
    pub ata: Pubkey,
    pub mint_pair: keypair::Keypair,
}

#[derive(Deserialize)]
pub struct WalletJson {
    pub keypair: String,
    pub mint_pair: String,
}

impl MintWallet {
    pub fn from_keypair(main_pair: keypair::Keypair) -> Self {
        let mint_pair = keypair::Keypair::new();
        let ata = get_ata(&main_pair.pubkey(), &mint_pair.pubkey());

        MintWallet {
            main_pair,
            ata,
            mint_pair,
        }
    }

    pub fn new() -> Self {
        let main_pair = keypair::Keypair::new();
        Self::from_keypair(main_pair)
    }

    pub fn from_json(wj: WalletJson) -> Self {
        let main_pair = keypair::Keypair::from_base58_string(&wj.keypair);
        let mint_pair = keypair::Keypair::from_base58_string(&wj.mint_pair);
        let ata = get_ata(&main_pair.pubkey(), &mint_pair.pubkey());
        MintWallet {
            main_pair,
            mint_pair,
            ata,
        }
    }

    pub fn init(&self, block: Hash, funding: &keypair::Keypair) -> Transaction {
        let mint_rent = sysvar::rent::Rent::default().minimum_balance(MINT_SIZE as usize);
        let spl_id = &spl_token::id();
        let main_pub = &self.main_pair.pubkey();
        let mint_pub = &self.mint_pair.pubkey();
        let fund_pub = &funding.pubkey();
        let ata = &self.ata;

        println!("mint wallet main: {:?}", main_pub);
        println!("ata account: {:?}", ata);
        println!("mint account: {:?}", mint_pub);
        let ixs = vec![
            system_instruction::create_account(fund_pub, mint_pub, mint_rent, MINT_SIZE, spl_id),
            spl::initialize_mint(spl_id, mint_pub, main_pub, Some(main_pub), 0).unwrap(),
            create_ata(main_pub, main_pub, mint_pub),
            spl::mint_to(spl_id, mint_pub, ata, main_pub, &[main_pub], 1).unwrap(),
        ];

        Transaction::new_signed_with_payer(
            &ixs,
            Some(fund_pub),
            &[funding, &self.main_pair, &self.mint_pair],
            block,
        )
    }

    pub fn fund_many(
        wallets: Vec<&Self>,
        block: Hash,
        funding: &keypair::Keypair,
        amount: u64,
    ) -> Transaction {
        let r: Vec<(Pubkey, u64)> = wallets
            .iter()
            .map(|w| (w.main_pair.pubkey(), amount))
            .collect();
        let ix = system_instruction::transfer_many(&funding.pubkey(), &r);

        Transaction::new_signed_with_payer(&ix, Some(&funding.pubkey()), &[funding], block)
    }

    pub fn mint_one(&self, block: Hash, drop: &MintDrop, set_collection: bool) -> Transaction {
        let main_pub = &self.main_pair.pubkey();
        let mint_pub = &self.mint_pair.pubkey();

        let MintAccounts {
            candy_machine,
            creator,
            creator_bump,
            wallet,
            metadata,
            master_edition,
        } = drop.get_mint_accounts(mint_pub);

        let ixd = [211, 57, 6, 167, 15, 219, 35, 251, creator_bump];

        let accounts = vec![
            AccountMeta {
                pubkey: candy_machine,
                is_writable: true,
                is_signer: false,
            },
            AccountMeta {
                pubkey: creator,
                is_writable: false,
                is_signer: false,
            },
            AccountMeta {
                pubkey: main_pub.clone(),
                is_writable: true,
                is_signer: true,
            },
            AccountMeta {
                pubkey: wallet,
                is_writable: true,
                is_signer: false,
            },
            AccountMeta {
                pubkey: metadata,
                is_writable: true,
                is_signer: false,
            },
            AccountMeta {
                pubkey: mint_pub.clone(),
                is_writable: true,
                is_signer: true,
            },
            AccountMeta {
                pubkey: main_pub.clone(),
                is_writable: true,
                is_signer: true,
            },
            AccountMeta {
                pubkey: main_pub.clone(),
                is_writable: true,
                is_signer: true,
            },
            AccountMeta {
                pubkey: master_edition,
                is_writable: true,
                is_signer: false,
            },
            AccountMeta {
                pubkey: METADATA_PROGRAM.clone(),
                is_writable: false,
                is_signer: false,
            },
            AccountMeta {
                pubkey: spl_token::id(),
                is_writable: false,
                is_signer: false,
            },
            AccountMeta {
                pubkey: system_program::id(),
                is_writable: false,
                is_signer: false,
            },
            AccountMeta {
                pubkey: sysvar::rent::id(),
                is_writable: false,
                is_signer: false,
            },
            AccountMeta {
                pubkey: sysvar::clock::id(),
                is_writable: false,
                is_signer: false,
            },
            AccountMeta {
                pubkey: sysvar::recent_blockhashes::ID.clone(),
                is_writable: false,
                is_signer: false,
            },
            AccountMeta {
                pubkey: sysvar::instructions::id(),
                is_writable: false,
                is_signer: false,
            },
        ];
        let mut ixs = vec![Instruction::new_with_bytes(
            CANDY_PROGRAM.clone(),
            &ixd,
            accounts,
        )];
        if set_collection {
            let CollectionAccounts {
                candy_machine,
                metadata,
                authority,
                collection_pda,
                collection_mint,
                collection_metadata,
                collection_master_edition,
                collection_authority_record,
            } = drop.get_collection_accounts(mint_pub);

            let ixd = [103, 17, 200, 25, 118, 95, 125, 61];
            let accounts = vec![
                AccountMeta {
                    pubkey: candy_machine,
                    is_writable: false,
                    is_signer: false,
                },
                AccountMeta {
                    pubkey: metadata,
                    is_writable: false,
                    is_signer: false,
                },
                AccountMeta {
                    pubkey: main_pub.clone(),
                    is_writable: false,
                    is_signer: true,
                },
                AccountMeta {
                    pubkey: collection_pda,
                    is_writable: true,
                    is_signer: false,
                },
                AccountMeta {
                    pubkey: METADATA_PROGRAM.clone(),
                    is_writable: false,
                    is_signer: false,
                },
                AccountMeta {
                    pubkey: sysvar::instructions::id(),
                    is_writable: false,
                    is_signer: false,
                },
                AccountMeta {
                    pubkey: collection_mint,
                    is_writable: false,
                    is_signer: false,
                },
                AccountMeta {
                    pubkey: collection_metadata,
                    is_writable: false,
                    is_signer: false,
                },
                AccountMeta {
                    pubkey: collection_master_edition,
                    is_writable: false,
                    is_signer: false,
                },
                AccountMeta {
                    pubkey: authority,
                    is_writable: false,
                    is_signer: false,
                },
                AccountMeta {
                    pubkey: collection_authority_record,
                    is_writable: false,
                    is_signer: false,
                },
            ];

            ixs.push(Instruction::new_with_bytes(
                CANDY_PROGRAM.clone(),
                &ixd,
                accounts,
            ));
        }
        Transaction::new_signed_with_payer(
            &ixs,
            Some(main_pub),
            &[&self.main_pair, &self.mint_pair],
            block,
        )
    }

    pub fn to_json(&self) -> String {
        format!(
            "{{\"keypair\":\"{}\",\"mint_pair\":\"{}\"}}",
            &self.main_pair.to_base58_string(),
            &self.mint_pair.to_base58_string()
        )
    }
}
