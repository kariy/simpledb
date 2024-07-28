pub mod bitcask;
pub mod memory;

/// A simple trait for allowing pluggable key/value storage engine
pub trait Engine {
    type Error: std::error::Error + Send + Sync + 'static;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;

    fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error>;

    fn flush(&mut self) -> Result<(), Self::Error>;
}
