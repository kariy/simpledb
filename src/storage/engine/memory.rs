
use std::collections::HashMap;
use parking_lot::RwLock;

use super::Engine;

/// An in-memory storage engine for testing purposes
pub struct MemoryEngine {
    data: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
}

impl MemoryEngine {
    pub fn new() -> Self {
        MemoryEngine {
            data: RwLock::new(HashMap::new()),
        }
    }
}

impl Engine for MemoryEngine {
    type Error = std::io::Error;

    fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<impl AsRef<[u8]>>, Self::Error> {
        let data = self.data.read();
        Ok(data.get(key.as_ref()).cloned())
    }

    fn set(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<(), Self::Error> {
        let mut data = self.data.write();
        data.insert(key.as_ref().to_vec(), value.as_ref().to_vec());
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        // No-op for in-memory engine
        Ok(())
    }
}
