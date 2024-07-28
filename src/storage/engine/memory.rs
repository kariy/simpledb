use parking_lot::RwLock;
use std::collections::BTreeMap;

use super::Engine;

/// An in-memory storage engine for testing purposes
#[derive(Debug)]
pub struct MemoryEngine {
    data: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl MemoryEngine {
    pub fn new() -> Self {
        MemoryEngine {
            data: RwLock::new(BTreeMap::new()),
        }
    }
}

impl Engine for MemoryEngine {
    type Error = std::io::Error;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let data = self.data.read();
        Ok(data.get(key.as_ref()).cloned())
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let mut data = self.data.write();
        data.insert(key.as_ref().to_vec(), value.as_ref().to_vec());
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error> {
        let mut data = self.data.write();
        data.remove(key.as_ref());
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
