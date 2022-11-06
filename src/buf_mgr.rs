use crate::file_mgr::{Block, FileMgr, Page};
use anyhow::Result;
use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum BufMgrErr {
    #[error("no page to allocate")]
    NoPageErr,
    #[error("unpin the unpinned buffer")]
    UnPinErr,
}

pub(crate) struct BufMgr {
    file_mgr: FileMgr,
    pool_size: usize,
    pool: Vec<Buffer>,
}

#[derive(Debug, Clone)]
pub(crate) struct Buffer {
    page: Page,
    block: Option<Block>,
    pins: usize,
}

#[allow(dead_code)]
impl BufMgr {
    pub(crate) fn new(file_mgr: FileMgr, pool_size: usize, page_size: usize) -> Self {
        let pool = vec![Buffer::new(page_size); pool_size];
        Self {
            file_mgr,
            pool_size,
            pool,
        }
    }

    pub(crate) fn pin(&mut self, block: Block) -> Result<&mut Page> {
        for i in 0..self.pool_size {
            let buf = &mut self.pool[i];
            if let Some(pinned_block) = &mut buf.block {
                if *pinned_block == block {
                    buf.pins += 1;
                    self.file_mgr.read(&pinned_block, &mut buf.page)?;
                    return Ok(&mut self.pool[i].page);
                }
            }
        }

        // using the naive policy
        for i in 0..self.pool_size {
            let buf = &mut self.pool[i];
            if buf.pins == 0 {
                if let Some(unpinned_block) = &buf.block {
                    self.file_mgr.write(&unpinned_block, &buf.page)?;
                }
                self.file_mgr.read(&block, &mut buf.page)?;
                buf.block = Some(block);
                buf.pins += 1;
                return Ok(&mut self.pool[i].page);
            }
        }

        return Err(BufMgrErr::NoPageErr.into());
    }

    pub(crate) fn unpin(&mut self, block: Block) -> Result<()> {
        for buf_id in 0..self.pool_size {
            let buf = &mut self.pool[buf_id];
            if let Some(pool_block) = &mut buf.block {
                if *pool_block == block {
                    if buf.pins > 0 {
                        buf.pins -= 1;
                    } else {
                        return Err(BufMgrErr::UnPinErr.into());
                    }
                }
            }
        }
        Ok(())
    }
}

impl Buffer {
    fn new(page_size: usize) -> Self {
        Buffer {
            page: Page::new(page_size),
            block: None,
            pins: 0,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::file_mgr::{Block, FileMgr, Page};

    use super::BufMgr;

    #[test]
    fn buf_simple_test() {
        let fm = FileMgr::new(String::from("example/"), 100);
        let blk_1 = Block::new(String::from("buftest.tbl"), 1);
        let blk_2 = Block::new(String::from("buftest.tbl"), 2);
        let blk_3 = Block::new(String::from("buftest.tbl"), 3);
        let blk_4 = Block::new(String::from("buftest.tbl"), 4);
        // init the table
        //fm.write(&Block::new(String::from("buftest.tbl"), 0), &Page::new(100))
        //.unwrap();
        fm.write(&blk_1, &Page::new(100)).unwrap();
        fm.write(&blk_2, &Page::new(100)).unwrap();
        fm.write(&blk_3, &Page::new(100)).unwrap();
        fm.write(&blk_4, &Page::new(100)).unwrap();
        let mut bm = BufMgr::new(fm, 3, 100);
        let page1 = bm.pin(Block::new(String::from("buftest.tbl"), 1)).unwrap();
        for i in 0..100 {
            page1.set_byte(i, 1);
        }
        bm.unpin(Block::new(String::from("buftest.tbl"), 1))
            .unwrap();
        bm.pin(blk_2).unwrap();
        bm.pin(blk_3).unwrap();
        bm.pin(blk_4).unwrap();
        bm.unpin(Block::new(String::from("buftest.tbl"), 2))
            .unwrap();
        let new_page = bm.pin(blk_1).unwrap();
        for i in 0..100 {
            assert_eq!(*new_page.get_byte(i).unwrap(), 1);
        }
    }
}
