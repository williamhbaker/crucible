use crucible::{
    memtable::MemTable,
    wal::{Operation, Wal, WalRecord},
};
use tempdir::TempDir;

#[test]
fn memtabel_recovery_from_wal() {
    let dir = TempDir::new("testing").unwrap();
    let file = "data.wal";
    let dir_path = dir.path();
    let file_path = dir_path.join(&file);

    let mut wal = Wal::new(&file_path);

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

    for r in &records {
        wal.append(&r.op, &r.key, &r.val)
    }

    let mt: MemTable = wal.into();

    assert_eq!(None, mt.get(b"key1".to_vec()));
    assert_eq!(
        Some(b"val2updated".to_vec().as_ref()),
        mt.get(b"key2".to_vec())
    );
    assert_eq!(Some(b"val3".to_vec().as_ref()), mt.get(b"key3".to_vec()));
}
