use crate::storage::engine::Engine;
use anyhow::Result;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

type TxnId = u64;

#[derive(Debug)]
pub struct TxnRO<E: Engine> {
    inner: Txn<E>,
}

impl<E: Engine> TxnRO<E> {
    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }

    // try to read from the cache first.
    // if its not in the cache, get it from the engine then put it in the cache. pub fn get(&self, key: Vec<u8>) {}
    pub fn commit(self) {
        let tx_id = self.inner.id;
        let mut txn_manager = self.inner.txn_manager.inner.lock();
        txn_manager.read_txns.remove(&tx_id);
    }
}

#[derive(Debug)]
pub struct TxnRW<E: Engine> {
    inner: Txn<E>,
    write_cache: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl<E: Engine> TxnRW<E> {
    // try to read from the cache first.
    // if its not in the cache, get it from the engine then put it in the cache.
    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        if let value @ Some(_) = self.write_cache.get(&key) {
            Ok(value.cloned())
        } else {
            self.inner.get(key)
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.write_cache.insert(key, value);
    }

    pub fn commit(self) -> Result<()> {
        let version = self.inner.id;
        let mut engine = self.inner.engine.lock();
        let mut versioned_cache = self.inner.versioned_cache.lock();

        // remove the write txn id from the txn manager
        let mut txn_manager = self.inner.txn_manager.inner.lock();
        let _ = txn_manager.write_txn.take();

        // write the cache to the engine
        for (key, value) in self.write_cache {
            engine.set(key.clone(), value.clone())?;
            versioned_cache
                .entry(key)
                .or_default()
                .insert(version, value);
        }

        Ok(())
    }
}

#[derive(Debug)]
struct Txn<E: Engine> {
    id: TxnId,
    engine: Arc<Mutex<E>>,
    txn_manager: TxnManager<E>,
    versioned_cache: Arc<Mutex<BTreeMap<Vec<u8>, BTreeMap<TxnId, Vec<u8>>>>>,
}

impl<E: Engine> Txn<E> {
    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        if let Some(entries) = self.versioned_cache.lock().get(&key) {
            // Find the first entry whose version is less than or equal
            // to the current txn id.
            let mut versioned_entries = entries.iter().rev();
            let val = versioned_entries
                .find(|(ver, _)| &self.id >= *ver)
                .map(|(_, value)| value.clone());

            Ok(val)
        } else {
            // get from the underlying engine
            let engine = self.engine.lock();
            let value = engine.get(key)?.map(|v| v.as_ref().to_vec());
            Ok(value)
        }
    }
}

#[derive(Debug)]
pub struct TxnManager<E: Engine> {
    engine: Arc<Mutex<E>>,
    inner: Arc<Mutex<TxnManagerInner>>,
    versioned_cache: Arc<Mutex<BTreeMap<Vec<u8>, BTreeMap<TxnId, Vec<u8>>>>>,
}

impl<E: Engine> TxnManager<E> {
    fn create_ro_txn(&self) -> TxnRO<E> {
        let mut this = self.inner.lock();

        // return error if there's a ongoing write txn
        if this.write_txn.is_some() {
            panic!("There's already a write txn ongoing");
        }

        let id = this.next_id;
        this.next_id += 1;
        this.read_txns.insert(id);

        let txn_manager = self.clone();
        let engine = Arc::clone(&self.engine);
        let versioned_cache = Arc::clone(&self.versioned_cache);

        TxnRO {
            inner: Txn {
                id,
                engine,
                txn_manager,
                versioned_cache,
            },
        }
    }

    fn create_rw_txn(&self) -> TxnRW<E> {
        let mut this = self.inner.lock();

        // return error if there's a ongoing write txn
        if this.write_txn.is_some() {
            panic!("There's already a write txn ongoing");
        }

        let id = this.next_id;
        this.next_id += 1;
        this.write_txn = Some(id);

        let txn_manager = self.clone();
        let engine = Arc::clone(&self.engine);
        let versioned_cache = Arc::clone(&self.versioned_cache);

        TxnRW {
            write_cache: BTreeMap::new(),
            inner: Txn {
                id,
                engine,
                txn_manager,
                versioned_cache,
            },
        }
    }
}

impl<E: Engine> Clone for TxnManager<E> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            engine: Arc::clone(&self.engine),
            versioned_cache: Arc::clone(&self.versioned_cache),
        }
    }
}

#[derive(Debug)]
struct TxnManagerInner {
    // the next transaction id
    next_id: TxnId,
    // concurrent read txns
    read_txns: HashSet<TxnId>,
    // there could only be 1 write txn at a time.
    write_txn: Option<TxnId>,
}
