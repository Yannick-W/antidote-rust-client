/// TODO:
/// - error handling
/// - better r2d2 adapter setup
/// - at least randomize pools when getting a connection
/// - privacy for struct and functions

extern crate r2d2;
// extern crate scheduled_thread_pool;

use std::io::{Error, ErrorKind};
// use rand::{thread_rng, Rng};

// inline code from other modules
pub mod transactions;
pub mod antidote_pb; // generated pb file
mod r2d2_adapter;
mod coder;

// better access to transactions
use transactions::{InteractiveTransaction, StaticTransaction};
use r2d2_adapter::{AntidoteConnectionManager};


// constants
// const INITIAL_POOL_SIZE: usize = 5;
const MAX_POOL_SIZE: usize = 50;
const CONNECT_RETRY_PERIOD: u64 = 1000; // if connection is refused retry after every 1 sec

// Represents connections to the Antidote database.
pub struct Client {
    pools: Vec<r2d2::Pool<AntidoteConnectionManager>>,
}

// Represents an Antidote server.
// The port needs to be the port of the protocol-buffer interface (usually 8087)
pub struct Host {
    pub name: String,
    pub port: i32,
}

// Recreates a new Antidote client connected to the given Antidote servers.
pub fn new_client(hosts: Vec<Host>) -> Result<Client, Error> {
    let mut pools = Vec::new();
    for h in hosts.iter() {
        let addr : String = h.name.clone()+":"+&h.port.clone().to_string();

        let connection_manager = AntidoteConnectionManager::new(addr);
        let pool: r2d2::Pool<AntidoteConnectionManager> = r2d2::Pool::builder()
            .max_size(MAX_POOL_SIZE as u32)
            .build(connection_manager)
            .unwrap();
        pools.push(pool);
    }
    let client = Client {pools};
    Ok(client)
}

impl Client {
    fn get_connection(&self) -> Result<r2d2::PooledConnection<AntidoteConnectionManager>, Error> {
        // TODO: random ordering of pools
        for p in self.pools.iter() {
            let conn = p.get().unwrap();
            return Ok(conn);
        }
        Err(Error::new(ErrorKind::Other, format!("All connections dead")))
    }

    pub fn start_transaction(&self) -> Result<InteractiveTransaction, Error> {
        let mut conn = self.get_connection()?;
        let read_write: u32 = 0;
        let blue: u32 = 0;
        let mut apb_txn_properties = antidote_pb::ApbTxnProperties::new();
        apb_txn_properties.set_read_write(read_write);
        apb_txn_properties.set_red_blue(blue);
        let mut apb_txn = antidote_pb::ApbStartTransaction::new();
        apb_txn.set_properties(apb_txn_properties);

        apb_txn.encode(&mut *conn)?;
        let apb_txn_resp = coder::decode_start_transaction_resp(&mut *conn)?;

        let txn_desc = apb_txn_resp.get_transaction_descriptor();
        let tx = InteractiveTransaction {
            conn,
            tx_id: txn_desc.to_vec(),
            committed: false,
        };
        return Ok(tx)
    }

    pub fn create_static_transaction<'clt>(&'clt mut self) -> Result<StaticTransaction<'clt>, Error> {
        let static_transaction = StaticTransaction {
            client: self,
        };
        Ok(static_transaction)
    }

    pub fn create_dc(&mut self, node_names: Vec<String>) -> Result<(), Error> {
        let mut conn = self.get_connection()?;
        let mut create_dc = antidote_pb::ApbCreateDC::new();
        create_dc.set_nodes(protobuf::RepeatedField::from_vec(node_names));
        create_dc.encode(&mut *conn)?;
        let resp = coder::decode_apb_create_dc_resp(&mut *conn)?;
        if !resp.get_success() {
            return Err(Error::new(ErrorKind::Other, format!("Could not create DC, error code {}", resp.get_errorcode())))
        }
        Ok(())
    }

    pub fn get_connection_descriptor(&mut self) -> Result<Vec<u8>, Error> {
        let mut conn = self.get_connection()?;
        let get_cd = antidote_pb::ApbGetConnectionDescriptor::new();
        get_cd.encode(&mut *conn)?;
        let mut resp = coder::decode_apb_get_connection_descriptor_resp(&mut *conn)?;
        if !resp.get_success() {
            return Err(Error::new(ErrorKind::Other, format!("Could not get connection descriptor, error code {}", resp.get_errorcode())))
        }
        let descriptor = resp.take_d();
        Ok(descriptor)
    }

    pub fn connect_to_dcs(&mut self, descriptors: Vec<Vec<u8>>) -> Result<(), Error> {
        let mut conn = self.get_connection()?;
        let mut connect_to_dcs = antidote_pb::ApbConnectToDCs::new();
        connect_to_dcs.set_descriptors(protobuf::RepeatedField::from_vec(descriptors));
        connect_to_dcs.encode(&mut *conn)?;
        let resp = coder::decode_apb_connect_to_dcs_resp(&mut *conn)?;
        if !resp.get_success() {
            return Err(Error::new(ErrorKind::Other, format!("Could not connect DCs, error code {}", resp.get_errorcode())))
        }
        Ok(())
    }
}
