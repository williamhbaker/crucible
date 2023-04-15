use crucible::{
    wal::{Operation, WalRecord},
    Store,
};
use tempdir::TempDir;

#[test]
fn test_store() {
    let dir = TempDir::new("testing").unwrap();
    let mut store = Store::new(dir.path());

    let records = vec![
        WalRecord {
            op: Operation::Put,
            key: b"key1".to_vec(),
            val: b"val1".to_vec(),
        },
        WalRecord {
            op: Operation::Put,
            key: b"key2".to_vec(),
            val: b"val2".to_vec(),
        },
        WalRecord {
            op: Operation::Delete,
            key: b"key1".to_vec(),
            val: b"val1".to_vec(),
        },
        WalRecord {
            op: Operation::Put,
            key: b"key2".to_vec(),
            val: b"val2updated".to_vec(),
        },
        WalRecord {
            op: Operation::Put,
            key: b"key3".to_vec(),
            val: b"val3".to_vec(),
        },
    ];

    for rec in records {
        match rec.op {
            Operation::Put => store.put(&rec.key, &rec.val),
            Operation::Delete => store.del(&rec.key),
        }
    }

    assert_eq!(None, store.get(b"key1".to_vec().as_ref()));
    assert_eq!(
        Some(b"val2updated".to_vec().as_ref()),
        store.get(b"key2".to_vec().as_ref())
    );
    assert_eq!(
        Some(b"val3".to_vec().as_ref()),
        store.get(b"key3".to_vec().as_ref())
    );

    // Re-open and the results are the same.
    drop(store);
    let mut store = Store::new(dir.path());
    assert_eq!(None, store.get(b"key1".to_vec().as_ref()));
    assert_eq!(
        Some(b"val2updated".to_vec().as_ref()),
        store.get(b"key2".to_vec().as_ref())
    );
    assert_eq!(
        Some(b"val3".to_vec().as_ref()),
        store.get(b"key3".to_vec().as_ref())
    );

    // Modify the opened store then re-open it.
    store.del(b"key2".as_ref());

    drop(store);
    let store = Store::new(dir.path());
    assert_eq!(None, store.get(b"key1".to_vec().as_ref()));
    assert_eq!(None, store.get(b"key2".to_vec().as_ref()));
    assert_eq!(
        Some(b"val3".to_vec().as_ref()),
        store.get(b"key3".to_vec().as_ref())
    );
}
