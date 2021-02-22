extern crate btreekv;

use btreekv::RSDB;
use std::path::Path;
use std::path::PathBuf;

#[test]
fn it_works() {
    let mut db = RSDB::new(Path::new("/tmp/rsdb")).unwrap();
    db.set(b"k1", b"v1").unwrap();
    assert!(db.get(b"k1").unwrap().unwrap() == b"v1");
    db.set(b"k1", b"v2").unwrap();
    assert!(db.get(b"k1").unwrap().unwrap() == b"v2");
    assert_eq!(db.get_path(), PathBuf::from("/tmp/rsdb"));
}
