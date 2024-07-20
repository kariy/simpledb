use std::sync::Arc;

use crate::storage::engine::Engine;

type TxnId = u64;

#[derive(Debug)]
pub struct Txn<E: Engine> {
    id: TxnId,
    engine: Arc<E>,
    txn_manager: TxnManager,
}

impl<E: Engine> Txn<E> {
    pub fn commit(&self) {}
}

#[derive(Debug, Clone)]
pub struct TxnManager {
    // the next transaction id
    next_id: TxnId,
    // concurrent read txns
    read_txns: Vec<TxnId>,
    // there could only be 1 write txn at a time.
    write_txn: Option<TxnId>,
}
