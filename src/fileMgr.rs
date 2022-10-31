use anyhow::Result;
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::prelude::FileExt;
use std::path::Path;

pub(crate) struct Block {
    filename: String,
    id: u64,
}

pub(crate) struct Page {
    content: Vec<u8>,
    size: usize,
}
pub(crate) struct FileMgr {
    dir: String,
    block_size: u64,
}

impl FileMgr {
    pub(crate) fn new(dir: String, block_size: u64) -> Self {
        Self { dir, block_size }
    }

    pub(crate) fn read(&self, block: &Block, page: &mut Page) -> Result<()> {
        let offset = self.block_size * block.id;
        let path = Path::new(&self.dir).join(&block.filename);
        let stream = File::open(path)?;
        stream.read_exact_at(&mut page.content.as_mut_slice(), offset)?;
        Ok(())
    }
    pub(crate) fn write(&self, block: &Block, page: &Page) -> Result<()> {
        let offset = self.block_size * block.id;
        let path = Path::new(&self.dir).join(&block.filename);
        println!("{}", path.display());
        let stream = File::create(path)?;
        stream.write_all_at(&mut page.content.as_slice(), offset)?;
        Ok(())
    }
}

impl Page {
    pub(crate) fn new(size: usize) -> Self {
        Self {
            content: vec![0; size],
            size,
        }
    }
    fn get_byte(&self, offset: usize) -> Option<&u8> {
        self.content.get(offset)
    }
    fn set_byte(&mut self, offset: usize, value: u8) {
        if let Some(byte) = self.content.get_mut(offset) {
            *byte = value;
        }
    }
}

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
        let block = Block::new(String::from("stupid.tbl"), 2);
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
    }
}
