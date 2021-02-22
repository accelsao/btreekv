use std::collections::BTreeMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::{self, Read};
use std::path::Path;
use std::path::PathBuf;

macro_rules! read_or_break {
    ($file:expr, $buf:expr, $count:expr) => {
        match $file.read(&mut $buf) {
            Ok(n) if n == $buf.len() => {
                $count += n;
            } 
            Ok(_) => {
                break;
            }
            Err(_) => {
                break;
            }
        }
    };
}

pub fn usize_to_array(u: usize) -> [u8; 4] {
    [(u >> 24) as u8, (u >> 16) as u8, (u >> 8) as u8, u as u8]
}

pub fn array_to_usize(ip: [u8; 4]) -> usize {
    ((ip[0] as usize) << 24) as usize
        + ((ip[1] as usize) << 16) as usize
        + ((ip[2] as usize) << 8) as usize
        + (ip[3] as usize)
}

pub struct RSDB {
    path: PathBuf,
    log: File,
    store: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl RSDB {
    pub fn new(path: &Path) -> std::io::Result<RSDB> {
        recover(path).map(|(log, store)| RSDB {
            path: path.to_owned(),
            log,
            store,
        })
    }
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        self.log(key.clone(), value.clone())?;
        self.store.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> std::io::Result<Option<&Vec<u8>>> {
        Ok(self.store.get(key))
    }

    pub fn get_path(&self) -> PathBuf {
        self.path.to_path_buf()
    }

    fn log(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.log.write_all(&usize_to_array(key.len()))?;
        self.log.write_all(&usize_to_array(value.len()))?;
        self.log.write_all(key)?;
        self.log.write_all(value)?;
        Ok(())
    }
}

fn recover(path: &Path) -> std::io::Result<(File, BTreeMap<Vec<u8>, Vec<u8>>)> {
    let filename = path.join("rsdb.log");
    std::fs::create_dir_all(path)?;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(filename)?;
    let mut store = BTreeMap::new();
    let mut read = 0;
    loop {
        let (mut k_len_buf, mut v_len_buf) = ([0u8; 4], [0u8; 4]);
        read_or_break!(file, k_len_buf, read);
        read_or_break!(file, v_len_buf, read);
        let (klen, vlen) = (array_to_usize(k_len_buf), array_to_usize(v_len_buf));
        let (mut k_buf, mut v_buf) = (Vec::with_capacity(klen), Vec::with_capacity(vlen));
        read_or_break!(file, k_buf, read);
        read_or_break!(file, v_buf, read);
        store.insert(k_buf, v_buf);
    }

    // clear potential tears
    file.set_len(read as u64)?;

    Ok((file, store))
}