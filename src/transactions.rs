use crate::antidote_pb::*;
use crate::coder;
use super::{Client, AntidoteConnectionManager};

use std::fmt;
use protobuf::{RepeatedField};
use std::io::{Error, ErrorKind};


/// Represents a bucket in the Antidote database.
/// Offers a high-level interface to issue read and write operations on objects in the bucket.
pub struct Bucket {
    pub bucket : Vec<u8>,
}

/// A transaction object offers low-level mechanisms to send protocol-buffer messages to Antidote in the context of
/// a highly-available transaction.
/// Typical representatives are interactive transactions handled by Antidote and static transactions handled on the client side.
pub trait Transaction {
    fn read(&mut self, objects: &Vec<ApbBoundObject>) -> Result<ApbReadObjectsResp, Error>;
    fn update(&mut self, updates: &Vec<ApbUpdateOp>) -> Result<(), Error>;
}

/// Type alias for byte-slices.
/// Used to represent keys of objects in buckets and maps
#[derive(Debug, Clone)]
pub struct Key(pub Vec<u8>);
impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Key({:#?})", self.0)
    }
}


/// Represents the result of reading from a map object.
/// Grants access to the keys of the map to access values of the nested CRDTs.
pub struct MapReadResult {
    pub map_resp: ApbGetMapResp,
}

// A transaction handled by Antidote on the server side.
// Interactive Transactions need to be started on the server and are kept open for their duration.
// Update operations are only visible to reads issued in the context of the same transaction or after committing the transaction.
// Always commit or abort interactive transactions to clean up the server side!
pub struct InteractiveTransaction {
    pub tx_id: Vec<u8>,
    // pub conn: Connection,
    // pub conn: TcpStream,
    pub conn: r2d2::PooledConnection<AntidoteConnectionManager>,
    pub committed: bool,
}

impl Transaction for InteractiveTransaction {

    fn update(&mut self, updates: &Vec<ApbUpdateOp>) -> Result<(), Error> {
        let mut apb_update = ApbUpdateObjects::new();
        apb_update.set_updates(RepeatedField::from_vec(updates.to_vec()));
        apb_update.set_transaction_descriptor(self.tx_id.to_vec());

        // apb_update.encode(self.conn.get_mut_ref())?;
        // let resp: ApbOperationResp = decode_operation_resp(self.conn.get_mut_ref())?;
        apb_update.encode(&mut *self.conn)?;
        let resp: ApbOperationResp = coder::decode_operation_resp(&mut *self.conn)?;
        if !resp.get_success() {
            return Err(Error::new(ErrorKind::Other, format!("operation not successful; error code {}", resp.get_errorcode())))
        }
        Ok(())
    }

    fn read(&mut self, objects: &Vec<ApbBoundObject>) -> Result<ApbReadObjectsResp, Error> {
        let mut apb_update = ApbReadObjects::new();
        apb_update.set_transaction_descriptor(self.tx_id.to_vec());
        apb_update.set_boundobjects(RepeatedField::from_vec(objects.to_vec()));

        // apb_update.encode(&mut self.conn.get_ref())?;
        // let result = decode_read_objects_resp(self.conn.get_mut_ref());
        apb_update.encode(&mut *self.conn)?;
        let result = coder::decode_read_objects_resp(&mut *self.conn);
        return result;
    }

}

impl InteractiveTransaction {

    pub fn commit(&mut self) -> Result<(), Error> {
        if !self.committed {
            let mut msg = ApbCommitTransaction::new();
            msg.set_transaction_descriptor(self.tx_id.to_vec());
            // msg.encode(self.conn.get_mut_ref())?;
            msg.encode(&mut *self.conn)?;
            // let op = decode_commit_resp(self.conn.get_mut_ref())?;
            let op = coder::decode_commit_resp(&mut *self.conn)?;
            // self.conn.close()?;
            if !op.get_success() {
                return Err(Error::new(ErrorKind::Other, format!("operation not successful; error code {}", op.get_errorcode())))
            }
        }
        Ok(())
    }

    pub fn abort(&mut self) -> Result<(), Error> {
        if !self.committed {
            let mut msg = ApbAbortTransaction::new();
            msg.set_transaction_descriptor(self.tx_id.to_vec());
            msg.encode(&mut *self.conn)?;
            let op = coder::decode_operation_resp(&mut *self.conn)?;
            // self.conn.close()?;
            if !op.get_success() {
                return Err(Error::new(ErrorKind::Other, format!("operation not successful; error code {}", op.get_errorcode())))
            }
        }
        Ok(())
    }

}

/// Pseudo transaction to issue reads and updated without starting an interactive transaction.
/// Can be interpreted as starting a transaction for each read or update and directly committing it.
pub struct StaticTransaction<'stlt> {
    pub client: &'stlt mut Client,
}

impl<'stlt> Transaction for StaticTransaction<'stlt> {
    fn update(&mut self, updates: &Vec<ApbUpdateOp>) -> Result<(), Error> {
        let mut apb_start_transaction = ApbStartTransaction::new();
        apb_start_transaction.set_properties(ApbTxnProperties::new());
        let mut apb_static_update = ApbStaticUpdateObjects::new();
        apb_static_update.set_transaction(apb_start_transaction);
        apb_static_update.set_updates(RepeatedField::from_vec(updates.to_vec()));

        // let mut con : Connection = self.client.get_connection()?;
        let mut conn = self.client.get_connection()?;
        // apb_static_update.encode(con.get_mut_ref())?;
        // let resp: ApbCommitResp = decode_commit_resp(con.get_mut_ref())?;
        apb_static_update.encode(&mut *conn)?;
        let resp: ApbCommitResp = coder::decode_commit_resp(&mut *conn)?;
        // conn.close()?;
        if !resp.get_success() {
            return Err(Error::new(ErrorKind::Other, format!("operation not successful; error code {}", resp.get_errorcode())))
        }
        Ok(())
    }
    fn read(&mut self, objects: &Vec<ApbBoundObject>) -> Result<ApbReadObjectsResp, Error> {
        let mut apb_start_transaction = ApbStartTransaction::new();
        apb_start_transaction.set_properties(ApbTxnProperties::new());
        let mut apb_static_read = ApbStaticReadObjects::new();
        apb_static_read.set_transaction(apb_start_transaction);
        apb_static_read.set_objects(RepeatedField::from_vec(objects.to_vec()));

        let mut conn = self.client.get_connection()?;
        apb_static_read.encode(&mut *conn)?;
        let sresp: ApbStaticReadObjectsResp = coder::decode_static_read_objects_resp(&mut *conn)?;
        // con.close()?;
        Ok(sresp.get_objects().clone())
    }
}

/// A CRDTReader allows to read the value of objects identified by keys in the context of a transaction.
pub trait CRDTReader {
    fn read_set(&self, tx: &mut dyn Transaction, key: &Key) -> Result<Vec<Vec<u8>>, Error>;
    fn read_reg(&self, tx: &mut dyn Transaction, key: &Key) -> Result<Vec<u8>, Error>;
    fn read_map(&self, tx: &mut dyn Transaction, key: &Key) -> Result<MapReadResult, Error>;
    fn read_mv_reg(&self, tx: &mut dyn Transaction, key: &Key) -> Result<Vec<Vec<u8>>, Error>;
    fn read_counter(&self, tx: &mut dyn Transaction, key: &Key) -> Result<i32, Error>;
}

// TODO: I am pretty sure all that boxing is NOT what you SHOULD do..
impl CRDTReader for Bucket {
    fn read_set(&self, tx: &mut dyn Transaction, key: &Key) -> Result<Vec<Vec<u8>>, Error> {
        let crdt_type = CRDT_type::ORSET;
        let mut apb_bound_object = ApbBoundObject::new();
        apb_bound_object.set_bucket(self.bucket.clone());
        apb_bound_object.set_key(key.0.clone());
        apb_bound_object.set_field_type(crdt_type);

        let mut objects = Vec::new();
        objects.push(apb_bound_object);
        let resp = tx.read(&objects)?;

        let val : &[Vec<u8>] = resp.get_objects()[0].get_set().get_value();
        Ok((*val).to_vec())
    }
    fn read_reg(&self, tx: &mut dyn Transaction, key: &Key) -> Result<Vec<u8>, Error> {
        let crdt_type = CRDT_type::LWWREG;
        let mut apb_bound_object = ApbBoundObject::new();
        apb_bound_object.set_bucket(self.bucket.clone());
        apb_bound_object.set_key(key.0.clone());
        apb_bound_object.set_field_type(crdt_type);

        let mut objects = Vec::new();
        objects.push(apb_bound_object);
        let resp = tx.read(&objects)?;

        let val : &[u8] = resp.get_objects()[0].get_reg().get_value();
        Ok((*val).to_vec())
    }
    fn read_map(&self, tx: &mut dyn Transaction, key: &Key) -> Result<MapReadResult, Error> {
        let crdt_type = CRDT_type::RRMAP;
        let mut apb_bound_object = ApbBoundObject::new();
        apb_bound_object.set_bucket(self.bucket.clone());
        apb_bound_object.set_key(key.0.clone());
        apb_bound_object.set_field_type(crdt_type);
        
        let mut objects = Vec::new();
        objects.push(apb_bound_object);
        let resp = tx.read(&objects)?;

        let val = MapReadResult {
            map_resp: (*(resp.get_objects()[0].get_map())).clone() // hmm ... TOCO ?
        };
        Ok(val)
    }
    fn read_mv_reg(&self, tx: &mut dyn Transaction, key: &Key) -> Result<Vec<Vec<u8>>, Error> {
        let crdt_type = CRDT_type::MVREG;
        let mut apb_bound_object = ApbBoundObject::new();
        apb_bound_object.set_bucket(self.bucket.clone());
        apb_bound_object.set_key(key.0.clone());
        apb_bound_object.set_field_type(crdt_type);
        
        let mut objects = Vec::new();
        objects.push(apb_bound_object);
        let resp = tx.read(&objects)?;

        let val = resp.get_objects()[0].get_mvreg().get_values();
        Ok((*val).to_vec())
    }
    fn read_counter(&self, tx: &mut dyn Transaction, key: &Key) -> Result<i32, Error> {
        let crdt_type = CRDT_type::COUNTER;
        let mut apb_bound_object = ApbBoundObject::new();
        apb_bound_object.set_bucket(self.bucket.clone());
        apb_bound_object.set_key(key.0.clone());
        apb_bound_object.set_field_type(crdt_type);
        
        let mut objects = Vec::new();
        objects.push(apb_bound_object);
        let resp = tx.read(&objects)?;

        let val = resp.get_objects()[0].get_counter().get_value();
        Ok(val)
    }
}

pub trait MapReadResultExtractor {
    fn set(&self, key: &Key) -> Result<Vec<Vec<u8>>, Error>;
    fn reg(&self, key: &Key) -> Result<Vec<u8>, Error>;
    fn map(&self, key: &Key) -> Result<MapReadResult, Error>;
    fn mv_reg(&self, key: &Key) -> Result<Vec<Vec<u8>>, Error>;
    fn counter(&self, key: &Key) -> Result<i32, Error>;
    fn list_map_keys(&self) -> Vec<MapEntryKey>;
}

impl MapReadResultExtractor for MapReadResult {
    fn set(&self, key: &Key) -> Result<Vec<Vec<u8>>, Error> {
        for (_, me) in self.map_resp.get_entries().iter().enumerate() {
            if me.get_key().get_field_type() == CRDT_type::ORSET && me.get_key().get_key() == key.0 {
                return Ok((*(me.get_value().get_set().get_value())).to_vec());
            }
        }
        Err(Error::new(ErrorKind::Other, format!("set entry with key {} not found", key)))
    }
    fn reg(&self, key: &Key) -> Result<Vec<u8>, Error> {
        for (_, me) in self.map_resp.get_entries().iter().enumerate() {
            if me.get_key().get_field_type() == CRDT_type::LWWREG && me.get_key().get_key() == key.0 {
                return Ok((*(me.get_value().get_reg().get_value())).to_vec());
            }
        }
        Err(Error::new(ErrorKind::Other, format!("register entry with key {} not found", key)))
    }
    fn map(&self, key: &Key) -> Result<MapReadResult, Error> {
        for (_, me) in self.map_resp.get_entries().iter().enumerate() {
            if me.get_key().get_field_type() == CRDT_type::RRMAP && me.get_key().get_key() == key.0 {
                return Ok(MapReadResult {map_resp: (*(me.get_value().get_map())).clone()});
            }
        }
        Err(Error::new(ErrorKind::Other, format!("map entry with key {} not found", key)))
    }
    fn mv_reg(&self, key: &Key) -> Result<Vec<Vec<u8>>, Error> {
        for (_, me) in self.map_resp.get_entries().iter().enumerate() {
            if me.get_key().get_field_type() == CRDT_type::MVREG && me.get_key().get_key() == key.0 {
                return Ok((*(me.get_value().get_mvreg().get_values())).to_vec());
            }
        }
        Err(Error::new(ErrorKind::Other, format!("mvreg entry with key {} not found", key)))
    }
    fn counter(&self, key: &Key) -> Result<i32, Error> {
        for (_, me) in self.map_resp.get_entries().iter().enumerate() {
            if me.get_key().get_field_type() == CRDT_type::COUNTER && me.get_key().get_key() == key.0 {
                return Ok(me.get_value().get_counter().get_value());
            }
        }
        Err(Error::new(ErrorKind::Other, format!("register entry with key {} not found", key)))
    }

    fn list_map_keys(&self) -> Vec<MapEntryKey> {
        let mut key_list : Vec<MapEntryKey> = Vec::new();
        for (_, me) in self.map_resp.get_entries().iter().enumerate() {
            key_list.push(MapEntryKey{
                key: me.get_key().get_key().to_vec(),
                crdt_type: me.get_key().get_field_type(),
            });
        }
        return key_list;
    }
}

/// Struct for Map-keys
pub struct MapEntryKey {
    pub key: Vec<u8>,
    pub crdt_type: CRDT_type,
}
impl fmt::Debug for MapEntryKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "MapEntryKey ({:?}, {:?})", self.key, self.crdt_type)
    }
}

/// Represents updates that can be converted to top-level updates applicable to a bucket
/// or nested updates applicable to a map
trait UpdateConverter {
    fn convert_to_top_level(&self, bucket: Vec<u8>) -> ApbUpdateOp;
    fn convert_to_nested(&self) -> ApbMapNestedUpdate;
}

pub struct CRDTUpdate {
    update: ApbUpdateOperation,
    key: Key,
    crdt_type: CRDT_type,
}

impl UpdateConverter for CRDTUpdate {
    fn convert_to_top_level(&self, bucket: Vec<u8>) -> ApbUpdateOp {
        let mut apb_bound_object = ApbBoundObject::new();
        apb_bound_object.set_key(self.key.0.clone());
        apb_bound_object.set_field_type(self.crdt_type.clone());
        apb_bound_object.set_bucket(bucket);

        let mut apb_update_op = ApbUpdateOp::new();
        apb_update_op.set_boundobject(apb_bound_object);
        apb_update_op.set_operation(self.update.clone());

        return apb_update_op;
    }
    fn convert_to_nested(&self) -> ApbMapNestedUpdate {
        let mut apb_map_key = ApbMapKey::new();
        apb_map_key.set_key(self.key.0.clone());
        apb_map_key.set_field_type(self.crdt_type.clone());

        let mut apb_map_nested_update = ApbMapNestedUpdate::new();
        apb_map_nested_update.set_key(apb_map_key);
        apb_map_nested_update.set_update(self.update.clone());

        return apb_map_nested_update;
    }
}

/// A CRDTUpdater allows to apply updates in the context of a transaction.
pub trait CRDTUpdater {
    fn update(&self, tx: &mut dyn Transaction, updates: Vec<CRDTUpdate>) -> Result<(), Error>;
}

impl CRDTUpdater for Bucket {
    fn update(&self, tx: &mut dyn Transaction, updates: Vec<CRDTUpdate>) -> Result<(), Error> {
        let mut update_ops: Vec<ApbUpdateOp> = Vec::new();
        for (_, v) in updates.iter().enumerate() {
            update_ops.push(v.convert_to_top_level(self.bucket.clone()));
        } 
        return tx.update(&update_ops);
    }
}


// CRDT update operations
pub fn set_add(key: &Key, elems: Vec<Vec<u8>>) -> CRDTUpdate {
    let op_type = ApbSetUpdate_SetOpType::ADD;
    let mut apb_set_update = ApbSetUpdate::new();
    apb_set_update.set_adds(RepeatedField::from_vec(elems));
    apb_set_update.set_optype(op_type);
    let mut apb_update_operation = ApbUpdateOperation::new();
    apb_update_operation.set_setop(apb_set_update);

    let crdt_update = CRDTUpdate {
        key: Key(key.0.clone()),
        crdt_type: CRDT_type::ORSET,
        update: apb_update_operation,
    };
    crdt_update
}

pub fn set_remove(key: &Key, elems: Vec<Vec<u8>>) -> CRDTUpdate {
    let op_type = ApbSetUpdate_SetOpType::REMOVE; 
    let mut apb_set_update = ApbSetUpdate::new();
    apb_set_update.set_rems(RepeatedField::from_vec(elems));
    apb_set_update.set_optype(op_type);
    let mut apb_update_operation = ApbUpdateOperation::new();
    apb_update_operation.set_setop(apb_set_update);

    let crdt_update = CRDTUpdate {
        key: Key(key.0.clone()),
        crdt_type: CRDT_type::ORSET,
        update: apb_update_operation,
    };
    crdt_update
}

pub fn counter_inc(key: &Key, inc: i64) -> CRDTUpdate {
    let mut apb_counter_update = ApbCounterUpdate::new();
    apb_counter_update.set_inc(inc);
    let mut apb_update_operation = ApbUpdateOperation::new();
    apb_update_operation.set_counterop(apb_counter_update);

    let crdt_update = CRDTUpdate {
        key: Key(key.0.clone()),
        crdt_type: CRDT_type::COUNTER,
        update: apb_update_operation,
    };
    crdt_update
}

pub fn reg_put(key: &Key, value: Vec<u8>) -> CRDTUpdate {
    let mut apb_reg_update = ApbRegUpdate::new();
    apb_reg_update.set_value(value);
    let mut apb_update_operation = ApbUpdateOperation::new();
    apb_update_operation.set_regop(apb_reg_update);

    let crdt_update = CRDTUpdate {
        key: Key(key.0.clone()),
        crdt_type: CRDT_type::LWWREG,
        update: apb_update_operation,
    };
    crdt_update
}

pub fn mv_reg_put(key: &Key, value: Vec<u8>) -> CRDTUpdate {
    let mut apb_reg_update = ApbRegUpdate::new();
    apb_reg_update.set_value(value);
    let mut apb_update_operation = ApbUpdateOperation::new();
    apb_update_operation.set_regop(apb_reg_update);

    let crdt_update = CRDTUpdate {
        key: Key(key.0.clone()),
        crdt_type: CRDT_type::MVREG,
        update: apb_update_operation,
    };
    crdt_update
}

pub fn map_update(key: &Key, updates: Vec<CRDTUpdate>) -> CRDTUpdate {
    let mut nupdates: Vec<ApbMapNestedUpdate> = Vec::new();
    for (_, v) in updates.iter().enumerate() {
        nupdates.push(v.convert_to_nested());
    }
    let mut apb_map_update = ApbMapUpdate::new();
    apb_map_update.set_updates(RepeatedField::from_vec(nupdates));
    let mut apb_update_operation = ApbUpdateOperation::new();
    apb_update_operation.set_mapop(apb_map_update);

    let crdt_update = CRDTUpdate {
        key: Key(key.0.clone()),
        crdt_type: CRDT_type::RRMAP,
        update: apb_update_operation,
    };
    crdt_update
}





