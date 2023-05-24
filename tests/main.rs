use std::collections::{HashMap, VecDeque};

use crucible::{protocol::ReadRecord, store::Store};
use rand::{
    distributions::{Alphanumeric, DistString},
    Rng,
};
use tempdir::TempDir;

#[test]
fn test_store() {
    let dir = TempDir::new("testing").unwrap();
    let mut store = Store::new(dir.path(), None).unwrap();

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
            ReadRecord::Exists { key, val } => store.put(&key, &val).unwrap(),
            ReadRecord::Deleted { key } => store.del(&key).unwrap(),
        }
    }

    assert_eq!(None, store.get(b"key1".to_vec().as_ref()).unwrap());
    assert_eq!(
        Some(b"val2updated".to_vec()),
        store.get(b"key2".to_vec().as_ref()).unwrap()
    );
    assert_eq!(
        Some(b"val3".to_vec()),
        store.get(b"key3".to_vec().as_ref()).unwrap()
    );

    // Re-open and the results are the same.
    drop(store);
    let mut store = Store::new(dir.path(), None).unwrap();
    assert_eq!(None, store.get(b"key1".to_vec().as_ref()).unwrap());
    assert_eq!(
        Some(b"val2updated".to_vec()),
        store.get(b"key2".to_vec().as_ref()).unwrap()
    );
    assert_eq!(
        Some(b"val3".to_vec()),
        store.get(b"key3".to_vec().as_ref()).unwrap()
    );

    // Delete from the store then re-open it.
    store.del(b"key2".as_ref()).unwrap();
    drop(store);
    let mut store = Store::new(dir.path(), None).unwrap();
    assert_eq!(None, store.get(b"key1".to_vec().as_ref()).unwrap());
    assert_eq!(None, store.get(b"key2".to_vec().as_ref()).unwrap());
    assert_eq!(
        Some(b"val3".to_vec()),
        store.get(b"key3".to_vec().as_ref()).unwrap()
    );

    // Update a value in the store then re-open it. This will create a second SST.
    store.put(b"key3".as_ref(), b"val3updated").unwrap();
    drop(store);
    let store = Store::new(dir.path(), None).unwrap();
    assert_eq!(None, store.get(b"key1".to_vec().as_ref()).unwrap());
    assert_eq!(None, store.get(b"key2".to_vec().as_ref()).unwrap());
    assert_eq!(
        Some(b"val3updated".to_vec()),
        store.get(b"key3".to_vec().as_ref()).unwrap()
    );
}

#[test]
#[ignore]
fn stress_test() {
    // This is a test to simulate some amount of random load on the storage engine consisting mostly
    // of writes with some updates and deletes mixed in. The store will be restarted randomly
    // throughout the test. It is not run by default with `cargo test` since it is rather lengthy.
    // It can be run with:
    //      cargo test -- --ignored

    // This will produce at least 200 memtable flushes, depending on how many times a restart is
    // triggered.
    let wal_size = 100 * 1024;

    // Total number of "actions", which are inserts, deletes, updates, or even restarts.
    let action_limit = 10_000;

    // 1 in 1000 is a restart.
    let restart_probability = 1000;
    // 1 in 100 is a delete.
    let delete_probability = 100;
    // 1 in 5 is an update.
    let update_probability = 5;
    // When updating pick from the most recent 100 inserted keys. This simulates some churn on more
    // recent data, while allowing the majority of data to become "old".
    let update_pool_size = 100;

    let key_length = 10..255;
    let val_length = 1..4096;

    let dir = TempDir::new("testing").unwrap();
    let mut store = Store::new(dir.path(), Some(wal_size)).unwrap();
    let mut ref_store: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut pool: VecDeque<Vec<u8>> = VecDeque::with_capacity(update_pool_size);

    let mut rng = rand::thread_rng();

    for i in 0..action_limit {
        if i % 100 == 0 {
            println!("processing action {} of {}...", i, action_limit);
        }

        let rand = rng.gen_range(1..=1000);

        if rand % restart_probability == 0 {
            // Close an re-open the store, which will convert any left-over wal file into an sst.
            drop(store);
            store = Store::new(dir.path(), Some(wal_size)).unwrap();
            continue;
        }

        if rand % delete_probability == 0 {
            if pool.len() == 0 {
                continue;
            }

            // Pick a random item from the last `update_pool_size` items to delete.
            let idx = rand::random::<usize>() % pool.len();
            let k = pool.remove(idx).unwrap();

            // Delete it from both the reference store and the test store.
            ref_store.remove(&k).unwrap();
            store.del(&k).unwrap();

            continue;
        }

        if rand % update_probability == 0 {
            if pool.len() == 0 {
                // In case an update is randomly selected before any inserts, or if there was one
                // insert followed by a delete etc.
                continue;
            }

            // Pick a random item from the last `update_pool_size` items to update.
            let idx = rand % pool.len();
            let k = pool.remove(idx).unwrap();

            let new_val = Alphanumeric
                .sample_string(&mut rand::thread_rng(), rng.gen_range(val_length.clone()))
                .as_bytes()
                .to_vec();

            // Update it in both the reference store and the test store.
            ref_store.insert(k.to_vec(), new_val.clone()).unwrap();
            // TODO: It would be nice if this worked like HashMap, returning an option. But that
            // would require a lookup for every put.
            store.put(&k, &new_val).unwrap();

            continue;
        }

        // Add a new record if none of those happened.
        loop {
            let key = Alphanumeric
                .sample_string(&mut rand::thread_rng(), rng.gen_range(key_length.clone()))
                .as_bytes()
                .to_vec();

            let val = Alphanumeric
                .sample_string(&mut rand::thread_rng(), rng.gen_range(val_length.clone()))
                .as_bytes()
                .to_vec();

            if ref_store.contains_key(&key) {
                // Handle the case where we randomly generate a key for a new insert that is already
                // in the store, as strange things can happen in this unlikely event.
                continue;
            }

            // Add to the ring buffer for updates/deletes.
            if pool.len() >= update_pool_size {
                pool.pop_front();
            }
            pool.push_back(key.clone());

            store.put(&key, &val).unwrap();
            ref_store.insert(key, val);

            break;
        }
    }

    // All values in the reference in-memory store should exist in the store under test.
    for (ref_key, ref_val) in ref_store.iter() {
        let ref_got = store.get(ref_key).unwrap().unwrap();
        assert_eq!(ref_val, &ref_got);
    }

    // Double check after re-opening the store.
    drop(store);
    store = Store::new(dir.path(), Some(wal_size)).unwrap();

    for (ref_key, ref_val) in ref_store.iter() {
        let ref_got = store.get(ref_key).unwrap().unwrap();
        assert_eq!(ref_val, &ref_got);
    }

    // Likewise, all values in the store under test should exist in the reference in-memory store.
    // But that will have to wait until we implement iteration on the store.
}
