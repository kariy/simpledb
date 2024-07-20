//! Loosely implemented based on the BitCask paper: <https://riak.com/assets/bitcask-intro.pdf>
//!
//! +------------------------------------------------------------------------------+
//! |                               BITCASK FILE                                   |
//! +------------------------------------------------------------------------------+
//! | File ID (1 byte)                                                             |
//! +------------------------------------------------------------------------------+
//! |                                LOG ENTRIES                                   |
//! +------------------------------------------------------------------------------+
//! | Timestamp  | Key Size   | Value Size | Key              | Value              |
//! | (8 bytes)  | (8 bytes)  | (8 bytes)  | (variable size)  | (variable size)    |
//! +------------+------------+------------+------------------+--------------------+
//! | Timestamp  | Key Size   | Value Size | Key              | Value              |
//! +------------+------------+------------+------------------+--------------------+
//! | ...        | ...        | ...        | ...              | ...                |
//! +------------+------------+------------+------------------+--------------------+
//! | Timestamp  | Key Size   | Value Size | Key              | Value              |
//! +------------+------------+------------+------------------+--------------------+
//!

use std::collections::btree_map;
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
    time::SystemTime,
};

/// Tombstone value to indicate a deleted key.
const TOMBSTONE: i8 = -1;
/// The file extension for the data files.
const FILE_EXT: &str = "cask";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("file id {0} is missing")]
    MissingFile(FileId),

    #[error("failed to get current timestamp: {0}")]
    TimestampError(#[from] std::time::SystemTimeError),
}

type Result<T> = std::result::Result<T, Error>;

type Key = Vec<u8>;
type Value = Vec<u8>;
type FileId = u8;

/// Represents the location of a value in the file.
#[derive(Debug)]
struct ValueLocation {
    /// File id
    file_id: FileId,
    /// Offset in the file
    offset: u64,
    /// Size of the value
    size: u64,
    /// The timestamp of the log entry
    timestamp: u64,
}

/// An index of keys to their value's locations in the file.
type KeyDir = BTreeMap<Key, ValueLocation>;

#[derive(Debug)]
pub struct BitCask {
    keydir: KeyDir,
    /// The id of the latest file.
    latest_file: FileId,
    files: HashMap<FileId, File>,
}

impl BitCask {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut bitcask = Self {
            latest_file: 0,
            files: HashMap::new(),
            keydir: BTreeMap::new(),
        };

        let root_path = path.as_ref();
        for entry in root_path.read_dir()? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let path = entry.path();
                let mut file = fs::OpenOptions::new().read(true).append(true).open(path)?;
                bitcask.build_keydir_from_file(&mut file)?;
            }
        }

        if bitcask.files.is_empty() {
            let id: u8 = 0;
            let file_path = root_path.join(0.to_string()).with_extension(FILE_EXT);
            let mut file = fs::File::create_new(file_path)?;
            file.write_all(&id.to_be_bytes())?;
            bitcask.files.insert(id, file);
        }

        Ok(bitcask)
    }

    pub fn get(&self, key: &Key) -> Result<Option<Vec<u8>>> {
        let Some(value) = self.keydir.get(key) else {
            return Ok(None);
        };

        let ValueLocation {
            file_id,
            offset,
            size,
            ..
        } = value;

        let file = self
            .files
            .get(file_id)
            .ok_or(Error::MissingFile(*file_id))?;
        let mut reader = BufReader::new(file);

        let mut buffer = vec![0; *size as usize];
        reader.seek(SeekFrom::Start(*offset))?;
        reader.read_exact(&mut buffer)?;
        Ok(Some(buffer))
    }

    // if the file has reached a certain size, insert to a new file
    pub fn put(&mut self, key: Key, value: Value) -> Result<()> {
        let file_id = self.latest_file;
        let file = self
            .files
            .get(&file_id)
            .ok_or(Error::MissingFile(file_id))?;
        let mut bufwriter = BufWriter::new(file);

        let pos = bufwriter.seek(SeekFrom::End(0))?;

        let timestamp = unix_epoch_now()?;
        let offset = pos + 8 + 8 + 8 + key.len() as u64;
        let location = ValueLocation {
            offset,
            file_id,
            timestamp,
            size: value.len() as u64,
        };

        let capacity = 8 + 8 + 8 + key.len() + value.len();
        let mut buffer = Vec::with_capacity(capacity);
        buffer.extend(timestamp.to_be_bytes());
        buffer.extend(key.len().to_be_bytes());
        buffer.extend(value.len().to_be_bytes());
        buffer.extend(&key);
        buffer.extend(value);

        bufwriter.write_all(&buffer)?;
        bufwriter.flush()?;
        self.keydir.insert(key, location);

        Ok(())
    }

    /// Deletion in bitcask is done by simply appending a log entry with a special tombstone value
    /// indicating that the key has been deleted. The key is then removed from the keydir.
    pub fn delete(&mut self, key: Key) -> Result<()> {
        self.put(key.clone(), TOMBSTONE.to_be_bytes().to_vec())?;
        self.keydir.remove(&key);
        Ok(())
    }

    /// Returns an iterator over the key-value pairs in the database.
    pub fn iter(&self) -> Iter<'_> {
        Iter::new(self)
    }

    /// Returns an iterator over the keys in the database.
    pub fn keys(&self) -> Keys<'_> {
        Keys::new(self)
    }

    /// Returns an iterator over the values in the database.
    pub fn values(&self) -> Values<'_> {
        Values::new(self)
    }

    pub fn fold(&mut self) {
        unimplemented!()
    }

    pub fn sync(&mut self) -> Result<()> {
        for file in self.files.values() {
            file.sync_all()?;
        }
        Ok(())
    }

    fn build_keydir_from_file(&mut self, file: &mut File) -> Result<()> {
        let file_len = file.metadata()?.len();

        let mut reader = std::io::BufReader::new(file);
        reader.seek(SeekFrom::Start(0))?;
        let mut position: u64 = 0;

        // the first byte of the file is always the file id
        let mut file_id = [0u8; 1];
        reader.read_exact(&mut file_id)?;
        let file_id = FileId::from_be_bytes(file_id);

        // read the contents of the file until eof and build the keydir
        while position < file_len {
            let mut timestamp = [0u8; std::mem::size_of::<u64>()];
            reader.read_exact(&mut timestamp)?;

            let mut key_size = [0u8; std::mem::size_of::<u64>()];
            reader.read_exact(&mut key_size)?;

            let mut value_size = [0u8; std::mem::size_of::<u64>()];
            reader.read_exact(&mut value_size)?;

            let key_size = usize::from_be_bytes(key_size);
            let mut key = Vec::with_capacity(key_size);
            reader.read_exact(&mut key)?;

            let timestamp = u64::from_be_bytes(timestamp);
            let value_size = u64::from_be_bytes(value_size);
            // offset = current_position + t_sz + k_sz + v_sz + key + value
            let offset = position + 8 + 8 + 8 + key_size as u64 + value_size;

            self.keydir
                .entry(key)
                .and_modify(|e: &mut ValueLocation| {
                    // update in-place if an existing entry with lower timestamp exist
                    if e.timestamp < timestamp {
                        e.file_id = file_id;
                        e.offset = offset;
                        e.size = value_size;
                    }
                })
                .or_insert(ValueLocation {
                    file_id,
                    offset,
                    timestamp,
                    size: value_size,
                });

            position = offset + value_size;
            reader.seek(SeekFrom::Start(position))?;
        }

        if self.latest_file < file_id {
            self.latest_file = file_id;
        }

        Ok(())
    }
}

/// Close the BitCask instance and flush all data to disk.
impl Drop for BitCask {
    fn drop(&mut self) {
        if let Err(e) = self.sync() {
            eprintln!("Failed to sync files: {:?}", e);
        }
    }
}

pub struct Iter<'a> {
    inner: btree_map::Iter<'a, Key, ValueLocation>,
    files: &'a HashMap<FileId, File>,
}

impl<'a> Iter<'a> {
    fn new(b: &'a BitCask) -> Self {
        let files = &b.files;
        let inner = b.keydir.iter();
        Self { inner, files }
    }
}

impl<'a> std::iter::Iterator for Iter<'a> {
    type Item = (&'a [u8], Result<Vec<u8>>);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, loc) = self.inner.next()?;
        let file = match self.files.get(&loc.file_id) {
            Some(file) => file,
            None => return Some((key, Err(Error::MissingFile(loc.file_id)))),
        };

        let mut reader = BufReader::new(file);
        let offset = SeekFrom::Start(loc.offset);
        let seek_result = reader.seek(offset).map_err(Error::from);
        if let Err(e) = seek_result {
            return Some((key, Err(e)));
        }

        let mut value = vec![0; loc.size as usize];
        let read_result = reader.read_exact(&mut value).map_err(Error::from);
        if let Err(e) = read_result {
            return Some((key, Err(e)));
        }

        Some((key, Ok(value)))
    }
}

pub struct Keys<'a> {
    inner: Iter<'a>,
}

impl<'a> Keys<'a> {
    fn new(b: &'a BitCask) -> Self {
        let inner = b.iter();
        Self { inner }
    }
}

impl<'a> std::iter::Iterator for Keys<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, _)| k.as_ref())
    }
}

pub struct Values<'a> {
    inner: Iter<'a>,
}

impl<'a> Values<'a> {
    fn new(b: &'a BitCask) -> Self {
        let inner = b.iter();
        Self { inner }
    }
}

impl<'a> std::iter::Iterator for Values<'a> {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        let (_, value) = self.inner.next()?;
        Some(value)
    }
}

fn unix_epoch_now() -> Result<u64> {
    let time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
    Ok(time.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn open_empty() {
        let temp_dir = tempdir().unwrap();
        let db = BitCask::open(temp_dir.path()).unwrap();

        assert!(db.keydir.is_empty());
        assert_eq!(db.files.len(), 1);
        assert_eq!(db.latest_file, 0);

        let mut file = db.files.get(&0).unwrap();
        let mut buf = [0u8; 1];
        file.read(&mut buf).unwrap();

        assert_eq!(u8::from_be_bytes(buf), 0);
        assert_eq!(file.metadata().unwrap().len(), 1);
    }

    #[test]
    fn put() {
        let temp_dir = tempdir().unwrap();
        let mut db = BitCask::open(temp_dir.path()).unwrap();

        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        db.put(key.clone(), value.clone()).unwrap();
        let retrieved_value = db.get(&key).unwrap().unwrap();

        assert!(db.keydir.contains_key(&key));
        assert_eq!(retrieved_value, value);

        // Check if the file has been updated
        let file = db.files.get(&db.latest_file).unwrap();
        assert!(file.metadata().unwrap().len() > 1);
    }

    #[test]
    fn delete() {
        let temp_dir = tempdir().unwrap();
        let mut db = BitCask::open(temp_dir.path()).unwrap();

        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        // Insert a key-value pair
        db.put(key.clone(), value).unwrap();
        assert!(db.get(&key).unwrap().is_some());

        // Delete the key
        db.delete(key.clone()).unwrap();

        // Verify that the key has been deleted
        assert_eq!(db.get(&key).unwrap(), None);
    }

    #[test]
    fn iter_keys() {
        let temp_dir = tempdir().unwrap();
        let mut db = BitCask::open(temp_dir.path()).unwrap();

        let keys = vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()];
        let value = b"value".to_vec();

        for key in &keys {
            db.put(key.clone(), value.clone()).unwrap();
        }

        let mut iter_keys: Vec<Vec<u8>> = db.keys().map(|k| k.to_vec()).collect();
        iter_keys.sort();

        assert_eq!(iter_keys, keys);
    }

    #[test]
    fn iter_values() {
        let temp_dir = tempdir().unwrap();
        let mut db = BitCask::open(temp_dir.path()).unwrap();

        let keys = vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()];
        let values = vec![b"value1".to_vec(), b"value2".to_vec(), b"value3".to_vec()];

        for (key, value) in keys.iter().zip(values.iter()) {
            db.put(key.clone(), value.clone()).unwrap();
        }

        let mut iter_values: Vec<Vec<u8>> = db.values().map(|v| v.unwrap()).collect();
        iter_values.sort();

        let mut expected_values = values.clone();
        expected_values.sort();

        assert_eq!(iter_values, expected_values);
    }
}
