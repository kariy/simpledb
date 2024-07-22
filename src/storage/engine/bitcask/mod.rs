//! Loosely implemented based on the BitCask paper: <https://riak.com/assets/bitcask-intro.pdf>
//!
//! +------------------------------------------------------------------------------+
//! |                               BITCASK FILE                                   |
//! +------------------------------------------------------------------------------+
//! | File ID (1 byte)                                                             |
//! +------------------------------------------------------------------------------+
//! | Data length (8 bytes)                                                        |
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

mod iter;

pub use self::iter::{Iter, Keys, Values};

use memmap2::{MmapMut, MmapOptions};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self};
use std::io::Write;
use std::path::PathBuf;
use std::{path::Path, time::SystemTime};

/// Tombstone value to indicate a deleted key.
const TOMBSTONE: i8 = -1;
/// The file extension for the data files.
const FILE_EXT: &str = "cask";

const GIGABYTE: u64 = 1024 * 1024 * 1024;
/// The initial size of the data files and their growth factor when they reach capacity.
const GROWTH_STEP: u64 = 1 * GIGABYTE;

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
    offset: usize,
    /// Size of the value
    size: usize,
    /// The timestamp of the log entry
    timestamp: u64,
}

/// An index of keys to their value's locations in the file.
type KeyDir = BTreeMap<Key, ValueLocation>;

#[derive(Debug)]
struct File {
    len: usize,
    mmapped: MmapMut,
    file: std::fs::File,
}

impl File {
    fn len(&self) -> usize {
        self.len
    }
}

#[derive(Debug)]
pub struct BitCask {
    /// The key dir which maps the active keys to their value's locations in the file.
    keydir: KeyDir,
    /// The id of the latest file.
    latest_file: FileId,
    /// The data files.
    files: HashMap<FileId, File>,
    /// The root path where the data files are located.
    root_path: PathBuf,
}

impl BitCask {
    pub fn init<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut this = Self {
            latest_file: 0,
            files: HashMap::new(),
            keydir: BTreeMap::new(),
            root_path: path.as_ref().to_path_buf(),
        };

        for entry in this.root_path.read_dir()? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                this.process_file(entry.path())?;
            }
        }

        if this.files.is_empty() {
            this.add_new_file()?;
        }

        Ok(this)
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

        let start = *offset as usize;
        let end = start + (*size as usize);
        let buffer = &file.mmapped[start..end];

        Ok(Some(buffer.to_vec()))
    }

    // if the file has reached a certain size, insert to a new file
    pub fn put(&mut self, key: Key, value: Value) -> Result<()> {
        let file_id = self.latest_file;
        let file = self
            .files
            .get_mut(&file_id)
            .ok_or(Error::MissingFile(file_id))?;

        let offset = file.len();
        let value_offset = offset + 8 + 8 + 8 + key.len();

        // TODO: if value_offset + value.len() > mmapped.len(), then have to inesrt to a new file
        // we always mmap the maximum size per file

        let timestamp = unix_epoch_now()?;
        let capacity = 8 + 8 + 8 + key.len() + value.len();
        let mut buffer = Vec::with_capacity(capacity);
        buffer.extend(timestamp.to_be_bytes());
        buffer.extend(key.len().to_be_bytes());
        buffer.extend(value.len().to_be_bytes());
        buffer.extend(&key);
        buffer.extend(&value);

        // mmap the file again because the file has grown
        (&mut file.mmapped[offset..(offset + capacity)]).write_all(&buffer)?;

        self.keydir.insert(
            key,
            ValueLocation {
                file_id,
                timestamp,
                size: value.len(),
                offset: value_offset,
            },
        );

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
            file.mmapped.flush()?;
        }
        Ok(())
    }

    // TODO: add check for magic number, if not present, skip instead of blindly
    // processing the file.
    fn process_file<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let file = fs::OpenOptions::new().read(true).append(true).open(path)?;
        let mmapped = unsafe { MmapOptions::new().populate().map_mut(&file)? };

        let id = self.build_keydir(mmapped)?;

        // update the latest file id
        if id > self.latest_file {
            self.latest_file = id;
        }

        Ok(())
    }

    fn build_keydir(&mut self, file: MmapMut) -> Result<FileId> {
        let file_len = file.len();
        // the first byte of the file is always the file id
        let file_id = file[0];

        // the starting position of the log entries in the file
        let mut position: usize = 1;

        // process a single log entry
        // read the contents of the file until eof and build the keydir
        while position < file_len {
            let buffer = &file[position..(position + 8)];
            let timestamp = unsafe { &*(buffer.as_ptr() as *const [u8; 8]) };
            position += 8;

            let buffer = &file[position..(position + 8)];
            let key_size = unsafe { &*(buffer.as_ptr() as *const [u8; 8]) };
            position += 8;

            let buffer = &file[position..(position + 8)];
            let value_size = unsafe { &*(buffer.as_ptr() as *const [u8; 8]) };
            position += 8;

            let key_size = usize::from_be_bytes(key_size.clone());
            let buffer = &file[position..(position + key_size)];
            let key = buffer.to_vec();

            let timestamp = u64::from_be_bytes(timestamp.clone());
            let value_size = usize::from_be_bytes(value_size.clone());
            // offset = current_position + t_sz + k_sz + v_sz + key + value
            let offset = position + 8 + 8 + 8 + key_size + value_size;

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
        }

        Ok(file_id)
    }

    fn add_new_file(&mut self) -> Result<()> {
        let new_id = if self.latest_file == 0 {
            0
        } else {
            self.latest_file + 1
        };

        let file = Self::create_file(new_id, &self.root_path)?;
        self.files.insert(new_id, file);
        self.latest_file = new_id;

        Ok(())
    }

    fn create_file<P: AsRef<Path>>(id: FileId, root_path: P) -> Result<File> {
        let root = root_path.as_ref();
        let path = root.join(id.to_string()).with_extension(FILE_EXT);

        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        file.set_len(GROWTH_STEP)?;
        let mmapped = unsafe { MmapOptions::new().populate().map_mut(&file)? };

        Ok(File { file, mmapped })
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
        let db = BitCask::init(temp_dir.path()).unwrap();

        assert!(db.keydir.is_empty());
        assert_eq!(db.files.len(), 1);
        assert_eq!(db.latest_file, 0);

        let file = db.files.get(&0).unwrap();
        let id = file.mmapped[0];

        assert_eq!(id, 0);
        assert_eq!(file.mmapped.len(), GIGABYTE as usize);
        assert_eq!(file.file.metadata().unwrap().len(), GIGABYTE);
    }

    #[test]
    fn put() {
        let temp_dir = tempdir().unwrap();
        let mut db = BitCask::init(temp_dir.path()).unwrap();

        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        db.put(key.clone(), value.clone()).unwrap();
        let retrieved_value = db.get(&key).unwrap().unwrap();

        assert!(db.keydir.contains_key(&key));
        assert_eq!(retrieved_value, value);

        // Check if the file has been updated
        let file = db.files.get(&db.latest_file).unwrap();
        assert!(file.mmapped.len() > 1);
        assert!(file.file.metadata().unwrap().len() > 1);
    }

    #[test]
    fn delete() {
        let temp_dir = tempdir().unwrap();
        let mut db = BitCask::init(temp_dir.path()).unwrap();

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
}
