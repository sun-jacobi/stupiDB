use std::mem::size_of;

use crate::file_mgr::{self, Block, FileMgr, Page};
use anyhow::{Ok, Result};

pub(crate) struct Record {
    msg: Vec<u8>,
    lsn: usize,
}

pub(crate) struct LogMgr {
    file_mgr: FileMgr,
    log_pg: Page,
    log_file: String,
    last_staged: usize,   // added to log_pg, but not commit to log_file
    last_commited: usize, // commited to log_file
    curr_block_id: u64,   // current used blokck
}

impl LogMgr {
    pub fn new(log_file: String) -> Self {
        let file_mgr = FileMgr::new(String::from("example/"), 100);
        let page_size = file_mgr.block_size();
        let log_pg = Page::new(page_size);
        Self {
            file_mgr,
            log_pg,
            log_file,
            last_staged: 0,
            last_commited: 0,
            curr_block_id: 0,
        }
    }

    pub fn add(&mut self, msg: Vec<u8>) -> Result<Record> {
        let rec_size = msg.len() + size_of::<usize>();

        if self.log_pg.avail_space() < rec_size {
            self.flush()?;
            self.log_pg.append(&msg);
            self.last_staged += 1;
            let lsn = self.last_staged.to_le_bytes().to_vec();
            self.log_pg.append(&lsn);
            return Ok(Record {
                msg,
                lsn: self.last_staged,
            });
        }

        self.log_pg.append(&msg);
        self.last_staged += 1;
        let lsn = self.last_staged.to_le_bytes().to_vec();
        self.log_pg.append(&lsn);
        return Ok(Record {
            msg,
            lsn: self.last_staged,
        });
    }

    pub fn commit(&mut self, lsn: usize) -> Result<()> {
        if lsn > self.last_commited {
            self.last_commited = self.last_staged;
            self.flush()?;
        }
        Ok(())
    }

    fn pg_size(&self) -> usize {
        self.file_mgr.block_size()
    }

    // clean the whole page, and write back to the physic disk
    fn flush(&mut self) -> Result<()> {
        let curr_block = Block::new(self.log_file.clone(), self.curr_block_id);
        self.file_mgr.write(&curr_block, &self.log_pg)?;
        self.curr_block_id += 1;
        self.log_pg.flush();
        Ok(())
    }
}

impl Iterator for LogMgr {
    type Item = Record;
    fn next(&mut self) -> Option<Self::Item> {
        // TODO
        None
    }
}

impl Record {
    pub(crate) fn size(&self) -> usize {
        self.msg.len() + size_of::<usize>()
    }
}

#[cfg(test)]
mod test {
    use super::LogMgr;
    #[test]
    fn log_simple_test() {
        let mut lm = LogMgr::new(String::from("logtest.tbl"));
        for i in 0..50 {
            let msg = format!("A{}", i + 1).bytes().collect::<Vec<u8>>();
            let record = lm.add(msg).unwrap();
        }
        lm.commit(10).unwrap();
        assert_eq!(lm.last_staged, 50);
        assert_eq!(lm.last_commited, 50);
    }
}
