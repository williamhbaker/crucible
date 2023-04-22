use crucible::{protocol::ReadRecord, store::Store};
use tempdir::TempDir;

#[test]
fn test_store() {
    let dir = TempDir::new("testing").unwrap();
    let mut store = Store::new(dir.path());

    let records = vec![
        ReadRecord::Exists {
            key: b"key1".to_vec(),
            val: b"val1".to_vec(),
        },
        ReadRecord::Exists {
            key: b"key2".to_vec(),
            val: b"val2".to_vec(),
        },
        ReadRecord::Deleted {
            key: b"key1".to_vec(),
        },
        ReadRecord::Exists {
            key: b"key2".to_vec(),
            val: b"val2updated".to_vec(),
        },
        ReadRecord::Exists {
            key: b"key3".to_vec(),
            val: b"val3".to_vec(),
        },
    ];

    for rec in records {
        match rec {
            ReadRecord::Exists { key, val } => store.put(&key, &val),
            ReadRecord::Deleted { key } => store.del(&key),
        }
    }

    assert_eq!(None, store.get(b"key1".to_vec().as_ref()));
    assert_eq!(
        Some(b"val2updated".to_vec()),
        store.get(b"key2".to_vec().as_ref())
    );
    assert_eq!(Some(b"val3".to_vec()), store.get(b"key3".to_vec().as_ref()));

    // Re-open and the results are the same.
    drop(store);
    let mut store = Store::new(dir.path());
    assert_eq!(None, store.get(b"key1".to_vec().as_ref()));
    assert_eq!(
        Some(b"val2updated".to_vec()),
        store.get(b"key2".to_vec().as_ref())
    );
    assert_eq!(Some(b"val3".to_vec()), store.get(b"key3".to_vec().as_ref()));

    // Modify the opened store then re-open it.
    store.del(b"key2".as_ref());

    drop(store);
    let mut store = Store::new(dir.path());
    assert_eq!(None, store.get(b"key1".to_vec().as_ref()));
    assert_eq!(None, store.get(b"key2".to_vec().as_ref()));
    assert_eq!(Some(b"val3".to_vec()), store.get(b"key3".to_vec().as_ref()));
}
