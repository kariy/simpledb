use std::collections::btree_map;
use std::collections::HashMap;

use super::BitCask;
use super::Error;
use super::File;
use super::FileId;
use super::Key;
use super::Result;
use super::ValueLocation;

pub struct Iter<'a> {
    inner: btree_map::Iter<'a, Key, ValueLocation>,
    files: &'a HashMap<FileId, File>,
}

impl<'a> Iter<'a> {
    pub(super) fn new(b: &'a BitCask) -> Self {
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

        let value = &file.mmapped[loc.offset..loc.offset + loc.size];
        Some((key, Ok(value.to_vec())))
    }
}

pub struct Keys<'a> {
    inner: Iter<'a>,
}

impl<'a> Keys<'a> {
    pub(super) fn new(b: &'a BitCask) -> Self {
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
    pub(super) fn new(b: &'a BitCask) -> Self {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn iter_keys() {
        let temp_dir = tempdir().unwrap();
        let mut db = BitCask::init(temp_dir.path()).unwrap();

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
        let mut db = BitCask::init(temp_dir.path()).unwrap();

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
