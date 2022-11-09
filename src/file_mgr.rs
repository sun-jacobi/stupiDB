use anyhow::Result;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::io::{Seek, SeekFrom};
use std::mem::size_of;
use std::os::unix::prelude::FileExt;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Block {
    filename: String,
    id: u64,
}

pub(crate) trait Data<T> {
    fn from_bytes(bytes: Vec<u8>) -> T;
    fn to_bytes(&self) -> Vec<u8>;
}

impl Data<u32> for u32 {
    fn from_bytes(bytes: Vec<u8>) -> u32 {
        let data = bytes.try_into().unwrap();
        u32::from_le_bytes(data)
    }
    fn to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl Data<String> for String {
    fn from_bytes(bytes: Vec<u8>) -> String {
        String::from_utf8(bytes[9..].try_into().unwrap()).unwrap()
    }
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.bytes().collect::<Vec<u8>>();
        let mut len = bytes.len().to_le_bytes().to_vec();
        len.append(&mut bytes);
        len
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Page {
    content: Vec<u8>,
    //size: usize,
    used: usize,
}
pub(crate) struct FileMgr {
    dir: String,
    block_size: usize,
}

#[allow(dead_code)]
impl FileMgr {
    pub(crate) fn new(dir: String, block_size: usize) -> Self {
        Self { dir, block_size }
    }

    pub(crate) fn read(&self, block: &Block, page: &mut Page) -> Result<()> {
        let offset = self.block_size as u64 * block.id;
        let path = Path::new(&self.dir).join(&block.filename);
        let stream = File::open(path)?;
        stream.read_exact_at(&mut page.content.as_mut_slice(), offset)?;
        Ok(())
    }
    pub(crate) fn write(&self, block: &Block, page: &Page) -> Result<()> {
        let offset = self.block_size as u64 * block.id;
        let path = Path::new(&self.dir).join(&block.filename);
        println!("{}", path.display());
        let mut stream = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        stream.seek(SeekFrom::Start(offset))?;
        stream.write(&mut page.content.as_slice())?;
        Ok(())
    }

    pub(crate) fn pwd(&self) -> String {
        self.dir.clone()
    }
    pub(crate) fn block_size(&self) -> usize {
        self.block_size
    }
}

#[allow(dead_code)]
impl Page {
    pub(crate) fn new(size: usize) -> Self {
        Self {
            content: vec![0; size],
            used: 0,
        }
    }
    pub(crate) fn get_byte(&self, offset: usize) -> Option<&u8> {
        self.content.get(offset)
    }
    pub(crate) fn set_byte(&mut self, offset: usize, value: u8) -> Option<()> {
        if let Some(byte) = self.content.get_mut(offset) {
            *byte = value;
            return Some(());
        }
        None
    }

    pub(crate) fn get<T: Data<T>>(&self, offset: usize) -> Option<T> {
        let size = size_of::<T>();
        let mut data = Vec::<u8>::new();
        for index in 0..size {
            if let Some(&byte) = self.content.get(offset + index) {
                data.push(byte);
            }
        }
        Some(<T>::from_bytes(data))
    }

    pub(crate) fn set<T: Data<T>>(&mut self, offset: usize, value: T) -> Option<()> {
        let data = value.to_bytes().to_vec();
        for (index, &byte) in data.iter().enumerate() {
            if let Some(pos) = self.content.get_mut(offset + index) {
                *pos = byte;
            } else {
                return None;
            }
        }
        Some(())
    }

    pub(crate) fn append(&mut self, data: &Vec<u8>) -> usize {
        let data_len = data.len();
        for (index, value) in data.iter().enumerate() {
            let offset = self.used + index;
            if let None = self.set_byte(offset, value.clone()) {
                self.used += index;
                return index;
            }
        }
        self.used += data_len;
        data_len
    }

    pub(crate) fn add(&mut self, data: u8) -> usize {
        let offset = self.used;
        if let None = self.set_byte(offset, data.clone()) {
            self.used += 1;
            return self.used;
        }
        0
    }

    pub(crate) fn avail_space(&self) -> usize {
        self.content.len() - self.used
    }

    pub(crate) fn flush(&mut self) {
        for data in self.content.iter_mut() {
            *data = 0;
        }
        self.used = 0;
    }
}

#[allow(dead_code)]
impl Block {
    pub(crate) fn new(filename: String, id: u64) -> Self {
        Self { filename, id }
    }
}

#[cfg(test)]
mod test {
    use super::{Block, FileMgr, Page};

    #[test]
    fn simple_test() {
        let file_mgr = FileMgr::new(String::from("example/"), 400);
        let block = Block::new(String::from("filetest.tbl"), 2);
        let mut page = Page::new(400);
        for i in 0..100 {
            page.set_byte(i, 1);
        }
        file_mgr.write(&block, &page).expect("failed to write");
        let mut new_page = Page::new(400);
        file_mgr
            .read(&block, &mut new_page)
            .expect("failed to read");
        for i in 0..100 {
            assert_eq!(*new_page.get_byte(i).unwrap(), 1);
        }

        let block = Block::new(String::from("filetest.tbl"), 2);
        file_mgr.write(&block, &page).expect("failed to write");
        let mut new_page = Page::new(400);
        file_mgr
            .read(&block, &mut new_page)
            .expect("failed to read");
        for i in 0..100 {
            assert_eq!(*new_page.get_byte(i).unwrap(), 1);
        }
    }
}
