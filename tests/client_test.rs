use std::rc::Rc;
use std::io::{Error, ErrorKind};
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::{Arc};
use std::thread;
use std::time::{Instant};

use arc::{Client, Host, new_client};
use arc::antidote_pb::{CRDT_type};
use arc::transactions::{MapEntryKey, InteractiveTransaction, 
    Bucket, Key, CRDTUpdater, CRDTReader, MapReadResultExtractor, 
    counter_inc, set_add, set_remove, reg_put, map_update
};


/// private setup function: creates a new client to Host{127.0.0.1:8101} and a bucket
fn setup_interactive() -> Result<(Client, Bucket), Error> {
    let host = Host {
        name: String::from("127.0.0.1"),
        port: 8101,
    };
    let mut hosts = Vec::new();
    hosts.push(host);
    let client = new_client(hosts)?;

    let timestamp : u128;
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => {
            timestamp = n.as_nanos()
        },
        Err(e) => return Err(Error::new(ErrorKind::Other, format!("SystemTimeError:{}", e)))
    }

    let mut bucketname = String::from("bucket");
    bucketname.push_str(timestamp.to_string().as_str()); // always unique bucket name
    let bucket = Bucket {
        bucket: bucketname.as_bytes().to_vec(),
    };
    Ok((client, bucket))
}

#[test]
fn test_simple() -> Result<(), Error> {
    // setup: create client and connection, start interactive transaction
    let (client, bucket) = setup_interactive()?;

    let keyname = String::from("keyCounter");
    let key = Key(keyname.as_bytes().to_vec());

    // update
    let mut tx = client.start_transaction()?;
    bucket.update(&mut tx, vec!(counter_inc(&key, 1)))?;

    // read
    let counter_val = bucket.read_counter(&mut tx, &key)?;

    // commit
    tx.commit()?;

    // assert
    assert_eq!(1, counter_val);
    Ok(())
}

#[test]
fn test_set_update() -> Result<(), Error> {
    // setup: create client and connection, start interactive transaction
    let (client, bucket) = setup_interactive()?;

    let keyname = String::from("keySet");
    let key = Key(keyname.as_bytes().to_vec());

    // update
    let mut tx = client.start_transaction()?;
    let elems = vec!("test1".as_bytes().to_vec(), "value2".as_bytes().to_vec(), "inset3".as_bytes().to_vec());    
    bucket.update(&mut tx, vec!(set_add(&key, elems)))?;
    let set_val = bucket.read_set(&mut tx, &key)?;
    tx.commit()?;

    // assert
    for expected in vec!["test1", "value2", "inset3"].iter() {
        let mut found = false;
        for val in set_val.iter() {
            let expected_val = (*expected).as_bytes().to_vec();
            if *val == expected_val {
                found = true;
                break;
            }       
        }
        if !found {
            return Err(Error::new(ErrorKind::Other, format!("expected value {} not found in result ({:?})", expected, set_val)))
        }
    }
    Ok(())
}


#[test]
fn test_set_update_remove() -> Result<(), Error> {
        // setup: create client and connection, start interactive transaction
        let (client, bucket) = setup_interactive()?;

        let keyname = String::from("keySet");
        let key = Key(keyname.as_bytes().to_vec());
    
        // update->remove->read->commit each its own transaction
        let mut tx = client.start_transaction()?;
        let elems = vec!("test1".as_bytes().to_vec(), "value2".as_bytes().to_vec(), "inset3".as_bytes().to_vec());    
        bucket.update(&mut tx, vec!(set_add(&key, elems)))?;
        tx.commit()?;

        let mut tx = client.start_transaction()?;
        let elems = vec!("test1".as_bytes().to_vec());    
        bucket.update(&mut tx, vec!(set_remove(&key, elems)))?;
        tx.commit()?;

        let mut tx = client.start_transaction()?;
        let set_val = bucket.read_set(&mut tx, &key)?;
        tx.commit()?;

        // assert
        assert_eq!(2, set_val.len());
        for expected in vec!["value2", "inset3"].iter() {
            let mut found = false;
            for val in set_val.iter() {
                let expected_val = (*expected).as_bytes().to_vec();
                if *val == expected_val {
                    found = true;
                    break;
                }       
            }
            if !found {
                return Err(Error::new(ErrorKind::Other, format!("expected value {} not found in result ({:?})", expected, set_val)))
            }
        }
        Ok(())
}

#[test]
fn test_map() -> Result<(), Error> {
    // setup: create client and connection, start interactive transaction
    let (client, bucket) = setup_interactive()?;

    let keyname = String::from("keyMap");
    let key = Key(keyname.as_bytes().to_vec());

    // map test
    let mut tx = client.start_transaction()?;
    let key_counter = Key("counter".as_bytes().to_vec());
    let key_reg = Key("reg".as_bytes().to_vec());
    let key_set = Key("set".as_bytes().to_vec());
    let elems = vec!(
        counter_inc(&key_counter, 13),
        reg_put(&key_reg, "Hello World".as_bytes().to_vec()),
        set_add(&key_set, vec!("A".as_bytes().to_vec(), "B".as_bytes().to_vec()))
    );
    bucket.update(&mut tx, vec!(map_update(&key, elems)))?;
    let map_val = bucket.read_map(&mut tx, &key)?;
    tx.commit()?;

    // extracting results
    let counter_val = map_val.counter(&key_counter)?;
    let reg_val = map_val.reg(&key_reg)?;
    let set_val = map_val.set(&key_set)?;

    // asserts
    assert_eq!(13, counter_val);
    assert_eq!("Hello World".as_bytes().to_vec(), reg_val);
    assert_eq!(2, set_val.len());
    for expected in vec!("A", "B") {
        let mut found = false;
        for val in set_val.iter() {
            let expected_val = (*expected).as_bytes().to_vec();
            if *val == expected_val {
                found = true;
                break;
            }       
        }
        if !found {
            return Err(Error::new(ErrorKind::Other, format!("expected value {} not found in result ({:?})", expected, set_val)))
        }
    }
    Ok(())
}

#[test]
fn test_static() -> Result<(), Error> {
    // setup: create client and connection, start interactive transaction
    let (mut client, bucket) = setup_interactive()?;

    let keyname = String::from("keyStatic");
    let key = Key(keyname.as_bytes().to_vec());

    // static test
    let mut tx = client.create_static_transaction()?;
    
    bucket.update(&mut tx, vec!(counter_inc(&key, 42)))?;
    let counter_val = bucket.read_counter(&mut tx, &key)?;

    // assert
    assert_eq!(42, counter_val);
    Ok(())
}

#[test]
fn test_many_updates() -> Result<(), Error> {
    let now = Instant::now();
    // setup: create client and connection, start interactive transaction
    let (client, bucket) = setup_interactive()?;

    let keyname = String::from("keyMany");
    let key = Key(keyname.as_bytes().to_vec());

    // many updates test
    const NUM_THREADS: i32 = 5;
    let mut children: Vec<std::thread::JoinHandle<std::result::Result<(), Error>>> = vec![];

    // Thread safe references
    let arc_c_strong = Arc::new(client);
    let arc_b_strong = Arc::new(bucket);
    let arc_k_strong = Arc::new(key);

    for _thread in 0..NUM_THREADS {
        let builder = thread::Builder::new();
        let arc_c = arc_c_strong.clone();
        let arc_b = arc_b_strong.clone();
        let arc_k = arc_k_strong.clone();
        children.push(builder.spawn(move || {
            for _i in 0..6000 {
                
                let mut tx = arc_c.start_transaction()?;
                arc_b.update(&mut tx, vec!(counter_inc(&arc_k, 1)))?;
                // let counter_val = arc_b.read_counter(&mut tx, &arc_k).unwrap();
                tx.commit()?;

                // if i%1000 == 0 {
                //     println!("Thread {}: {}; Counter value: {}",thread, i, counter_val);
                // }   
            }
            Ok(())
        }).unwrap());
    }
    for child in children {
        // Wait for the thread to finish.
        let _ = child.join().expect("Could not join associated thread");
    }

    // reestablish client, bucket and key values
    let mut client : Client;
    // need to unwrap the client ref since create_static_transaction needs a mutable reference
    match Arc::try_unwrap(arc_c_strong) {
        Ok(c) => client = c,
        Err(_) => return Err(Error::new(ErrorKind::Other, format!("Could not unwrap client.")))
    }
    let mut tx = client.create_static_transaction()?;
    let counter_val = arc_b_strong.read_counter(&mut tx, &arc_k_strong)?;

    // assert
    assert_eq!(6000*NUM_THREADS, counter_val);
    println!("Counter value as expected: {}", counter_val);
    println!("Test duration: {}", now.elapsed().as_millis());
    Ok(())
}

#[test]
fn test_many_updates_seq() -> Result<(), Error> {
    let now = Instant::now();
    // setup: create client and connection, start interactive transaction
    let (mut client, bucket) = setup_interactive()?;

    let keyname = String::from("keyManySeq");
    let key = Key(keyname.as_bytes().to_vec());

    for i in 0..30000 {
        let mut tx = client.start_transaction()?;
        bucket.update(&mut tx, vec!(counter_inc(&key, 1)))?;
        tx.commit()?;
        if i%1000 == 0 {
            println!("{}",i);
        }   
    }

    let mut tx = client.create_static_transaction()?;
    let counter_val = bucket.read_counter(&mut tx, &key)?;

    // assert
    assert_eq!(30000, counter_val);
    println!("Counter value as expected: {}", counter_val);
    println!("Test duration: {}", now.elapsed().as_millis());
    Ok(())
}

#[test]
fn test_many_updates_seq_in_trans() -> Result<(), Error> {
    let now = Instant::now();
    // setup: create client and connection, start interactive transaction
    let (mut client, bucket) = setup_interactive()?;

    let keyname = String::from("keyManySeqTrans");
    let key = Key(keyname.as_bytes().to_vec());

    let mut tx = client.start_transaction()?;
    let mut rc_tx = Rc::new(&mut tx);
    for i in 0..30000 {
            let tx : &mut InteractiveTransaction = Rc::get_mut(&mut rc_tx).unwrap();
            bucket.update(tx, vec!(counter_inc(&key, 1)))?;
        if i%1000 == 0 {
            println!("{}",i);
        }   
    }
    tx.commit()?;

    let mut tx = client.create_static_transaction()?;
    let counter_val = bucket.read_counter(&mut tx, &key)?;

    // assert
    assert_eq!(30000, counter_val);
    println!("Counter value as expected: {}", counter_val);
    println!("Test duration: {}", now.elapsed().as_millis());
    Ok(())
}

#[test]
fn test_map_list_map_keys() -> Result<(), Error> {
    // setup: create client and connection, start interactive transaction
    let (client, bucket) = setup_interactive()?;

    let keyname = String::from("keyMap");
    let key = Key(keyname.as_bytes().to_vec());

    let mut tx = client.start_transaction()?;

    let key_counter = "counter".as_bytes().to_vec();
    let key_reg = "reg".as_bytes().to_vec();
    let key_set = "set".as_bytes().to_vec();
    bucket.update(&mut tx, vec!(
        map_update(&key, vec!(
            counter_inc(&Key(key_counter.clone()), 13),
            reg_put(&Key(key_reg.clone()), "Hello World".as_bytes().to_vec()),
            set_add(&Key(key_set.clone()), vec!("A".as_bytes().to_vec(), "B".as_bytes().to_vec())
        )))
    ))?;

    let map_v = bucket.read_map(&mut tx, &key)?;
    let key_list = map_v.list_map_keys();

    // commit
    tx.commit()?;

    // asserts
    let expected_map_entries = vec!(
        MapEntryKey{key:key_counter.clone(), crdt_type: CRDT_type::COUNTER},
        MapEntryKey{key:key_reg.clone(), crdt_type: CRDT_type::LWWREG},
        MapEntryKey{key:key_set.clone(), crdt_type: CRDT_type::ORSET},
    );

    let mut found = false;
    for expected in expected_map_entries.iter() {
        for entry in key_list.iter() {
            if entry.key == expected.key && entry.crdt_type == expected.crdt_type {
                found = true;
                break
            }
        }
        if !found {
            return Err(Error::new(ErrorKind::Other, format!("expected value {:?} not found in result ({:?})", expected, key_list)))
        }
    }
    Ok(())
}