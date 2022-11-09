use std::sync::Mutex;

use crate::file_mgr::Block;

pub(crate) struct TxnMgr {
    mutex: Mutex<Block>,
}
