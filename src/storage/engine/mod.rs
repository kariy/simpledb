pub mod bitcask;

/// A simple trait for allowing pluggable key/value storage engine
pub trait Engine {
    type Error: std::error::Error;

    fn get(&self, key: impl AsRef<[u8]>) -> Result<impl AsRef<[u8]>, Self::Error>;

    fn set(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<(), Self::Error>;

    fn flush(&mut self) -> Result<(), Self::Error>;
}
