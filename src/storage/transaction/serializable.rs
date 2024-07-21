use crate::storage::engine::Engine;
use anyhow::{bail, Result};
use parking_lot::Mutex;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

type TxnId = u64;

#[derive(Debug)]
pub struct TxnRO<'a, E: Engine>(Txn<'a, E>);

impl<'a, E: Engine> TxnRO<'a, E> {
    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.0.get(key)
    }

    // try to read from the cache first.
    // if its not in the cache, get it from the engine then put it in the cache. pub fn get(&self, key: Vec<u8>) {}
    pub fn commit(self) {
        let tx_id = self.0.id;
        let mut txn_manager = self.0.txn_manager.inner.lock();
        txn_manager.read_txns.remove(&tx_id);
    }
}

#[derive(Debug)]
pub struct TxnRW<'a, E: Engine> {
    inner: Txn<'a, E>,
    write_cache: BTreeMap<Vec<u8>, Cow<'a, Vec<u8>>>,
}

impl<'a, E: Engine> TxnRW<'a, E> {
    // try to read from the cache first.
    // if its not in the cache, get it from the engine then put it in the cache.
    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        if let Some(value) = self.write_cache.get(&key) {
            Ok(Some(value.as_ref().clone()))
        } else {
            self.inner.get(key)
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.write_cache.insert(key, Cow::Owned(value));
    }

    pub fn commit(self) -> Result<()> {
        let version = self.inner.id;
        let mut engine = self.inner.engine.lock();
        // create an independent copy of the versioned cache
        let mut versioned_cache = self.inner.versioned_cache.lock().clone();

        // remove the write txn id from the txn manager
        let mut txn_manager = self.inner.txn_manager.inner.lock();
        let _ = txn_manager.write_txn.take();

        // write the cache to the engine and insert into our independent versioned cache
        for (key, value) in self.write_cache {
            engine.set(&key, value.as_ref().clone())?;

            let key = Cow::Owned(key);
            if let Some(Some(entry)) = versioned_cache.get_mut(&key) {
                entry.insert(version, value);
            } else {
                // create a btreemap and insert the current version and value
                let entry = BTreeMap::from([(version, value)]);
                versioned_cache.insert(key, Some(entry));
            }
        }

        // Update the shared versioned cache with our independent copy
        *self.inner.txn_manager.versioned_cache.lock() = Arc::new(versioned_cache.into());
        Ok(())
    }
}

#[derive(Debug)]
struct Txn<'a, E: Engine> {
    id: TxnId,
    engine: Arc<Mutex<E>>,
    txn_manager: TxnManager<'a, E>,
    versioned_cache:
        Arc<Mutex<BTreeMap<Cow<'a, Vec<u8>>, Option<BTreeMap<TxnId, Cow<'a, Vec<u8>>>>>>>,
}

impl<'a, E: Engine> Txn<'a, E> {
    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        match self.versioned_cache.lock().get(&key) {
            Some(Some(entries)) => {
                // Find the first entry whose version is less than or equal
                // to the current txn id.
                let mut versioned_entries = entries.iter().rev();
                let val = versioned_entries
                    .find(|(ver, _)| &self.id >= *ver)
                    .map(|(_, value)| value.as_ref().clone());

                return Ok(val);
            }

            Some(None) => return Ok(None),
            None => {}
        }

        // get from the underlying engine
        let engine = self.engine.lock();
        let value = engine.get(&key)?.map(|v| v.as_ref().to_vec());

        // put it in the cache and return the value
        let cache_value = if let Some(ref value) = value {
            Some(BTreeMap::from([(self.id, Cow::Owned(value.clone()))]))
        } else {
            None
        };

        let mut cache = self.versioned_cache.lock();
        let entry = cache.entry(Cow::Owned(key)).or_default();
        *entry = cache_value;

        Ok(value)
    }
}

#[derive(Debug)]
pub struct TxnManager<'a, E: Engine> {
    engine: Arc<Mutex<E>>,
    inner: Arc<Mutex<TxnManagerInner>>,

    // the first layer of Arc<Mutex<...>> is for filling up the cache from the engine query.
    // the second layer of Arc<Mutex<...>> is setting the new BTreemap to the cache.
    // idk what im talking, at least i understand this in my head (for now).
    versioned_cache: Arc<Mutex<VersionedCache<'a>>>,
}

type VersionedCache<'a> =
    Arc<Mutex<BTreeMap<Cow<'a, Vec<u8>>, Option<BTreeMap<TxnId, Cow<'a, Vec<u8>>>>>>>;

impl<'a, E: Engine> TxnManager<'a, E> {
    fn create_ro_txn(&self) -> Result<TxnRO<'a, E>> {
        let mut this = self.inner.lock();

        let id = this.next_id;
        this.next_id += 1;
        this.read_txns.insert(id);

        let txn_manager = self.clone();
        let engine = Arc::clone(&self.engine);
        let versioned_cache = self.versioned_cache.lock().clone();

        Ok(TxnRO(Txn {
            id,
            engine,
            txn_manager,
            versioned_cache,
        }))
    }

    fn create_rw_txn(&self) -> Result<TxnRW<'a, E>> {
        let mut this = self.inner.lock();

        // return error if there's a ongoing write txn
        if this.write_txn.is_some() {
            bail!("There's already a write txn ongoing");
        }

        let id = this.next_id;
        this.next_id += 1;
        this.write_txn = Some(id);

        let txn_manager = self.clone();
        let engine = Arc::clone(&self.engine);
        let versioned_cache = self.versioned_cache.lock().clone();

        Ok(TxnRW {
            write_cache: BTreeMap::new(),
            inner: Txn {
                id,
                engine,
                txn_manager,
                versioned_cache,
            },
        })
    }
}

impl<'a, E: Engine> Clone for TxnManager<'a, E> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            engine: Arc::clone(&self.engine),
            versioned_cache: self.versioned_cache.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::memory::MemoryEngine;

    fn setup() -> TxnManager<'static, MemoryEngine> {
        let engine = Arc::new(Mutex::new(MemoryEngine::new()));
        TxnManager {
            engine,
            inner: Arc::new(Mutex::new(TxnManagerInner {
                next_id: 0,
                write_txn: None,
                read_txns: HashSet::new(),
            })),
            versioned_cache: Default::default(),
        }
    }

    #[test]
    fn test_single_write_transaction() {
        let txn_manager = setup();

        // Start a write transaction
        let write_txn1 = txn_manager.create_rw_txn().unwrap();

        // Attempt to start another write transaction while there's an ongoing
        // write transaction should fail
        let err = txn_manager.create_rw_txn().unwrap_err().to_string();
        assert!(err.contains("There's already a write txn ongoing"));

        // Commit the first write transaction
        write_txn1.commit().unwrap();

        // Now we should be able to create a new write transaction
        let _write_txn2 = txn_manager.create_rw_txn();
    }

    #[test]
    fn test_no_non_repeatable_reads() {
        let txn_manager = setup();

        // Initialize data
        let mut init_txn = txn_manager.create_rw_txn().unwrap();
        init_txn.put(b"key".to_vec(), b"value1".to_vec());
        init_txn.commit().unwrap();

        // Start a read transaction
        let read_txn = txn_manager.create_ro_txn().unwrap();
        assert_eq!(
            read_txn.get(b"key".to_vec()).unwrap(),
            Some(b"value1".to_vec())
        );

        // Start a write transaction and modify the data
        let mut write_txn = txn_manager.create_rw_txn().unwrap();
        write_txn.put(b"key".to_vec(), b"value2".to_vec());
        write_txn.commit().unwrap();

        // The read transaction should still see the old value
        assert_eq!(
            read_txn.get(b"key".to_vec()).unwrap(),
            Some(b"value1".to_vec())
        );
    }

    #[test]
    fn test_no_phantom_reads() {
        let txn_manager = setup();

        // Start a read transaction
        let read_txn = txn_manager.create_ro_txn().unwrap();
        assert_eq!(read_txn.get(b"key1".to_vec()).unwrap(), None);
        assert_eq!(read_txn.get(b"key2".to_vec()).unwrap(), None);

        // Start a write transaction and add new data
        let mut write_txn = txn_manager.create_rw_txn().unwrap();
        write_txn.put(b"key1".to_vec(), b"value1".to_vec());
        write_txn.put(b"key2".to_vec(), b"value2".to_vec());
        write_txn.commit().unwrap();

        // The first read transaction should still not see the new data
        assert_eq!(read_txn.get(b"key1".to_vec()).unwrap(), None);
        assert_eq!(read_txn.get(b"key2".to_vec()).unwrap(), None);

        // A new read transaction should see the new data
        let new_read_txn = txn_manager.create_ro_txn().unwrap();
        assert_eq!(
            new_read_txn.get(b"key1".to_vec()).unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            new_read_txn.get(b"key2".to_vec()).unwrap(),
            Some(b"value2".to_vec())
        );
    }

    // this could be merged with the non repeatable read test
    #[test]
    fn test_read_only_with_ongoing_write() {
        let txn_manager = setup();

        // Initialize data
        let mut init_txn = txn_manager.create_rw_txn().unwrap();
        init_txn.put(b"key".to_vec(), b"initial_value".to_vec());
        init_txn.commit().unwrap();

        // Start a write transaction
        let mut write_txn = txn_manager.create_rw_txn().unwrap();
        write_txn.put(b"key".to_vec(), b"new_value".to_vec());

        // Create a read-only transaction while the write transaction is ongoing
        let read_txn = txn_manager.create_ro_txn().unwrap();

        // The read-only transaction should see the initial value
        assert_eq!(
            read_txn.get(b"key".to_vec()).unwrap(),
            Some(b"initial_value".to_vec())
        );

        // Commit the write transaction
        write_txn.commit().unwrap();

        // The read-only transaction should still see the initial value
        assert_eq!(
            read_txn.get(b"key".to_vec()).unwrap(),
            Some(b"initial_value".to_vec())
        );

        // A new read-only transaction should see the new value
        let new_read_txn = txn_manager.create_ro_txn().unwrap();
        assert_eq!(
            new_read_txn.get(b"key".to_vec()).unwrap(),
            Some(b"new_value".to_vec())
        );
    }
}
