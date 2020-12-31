use std::fmt;
use std::{thread, time};
use std::net::{TcpStream};

use super::{CONNECT_RETRY_PERIOD};


// r2d2 pool error definition
#[derive(Debug)]
pub struct PoolError {
    message: String,
}
impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "error message: {}", self.message)
    }
}
impl ::std::error::Error for PoolError {
}
impl PoolError {
    fn _new(msg: &str) -> PoolError {
        PoolError {
            message: String::from(msg),
        }
    }
}

// r2d2 connection manager definition
pub struct AntidoteConnectionManager {
    addr: String,
}
impl AntidoteConnectionManager {
    pub fn new(addr: String) -> AntidoteConnectionManager {
        AntidoteConnectionManager {
            addr
        }
    }
}
impl r2d2::ManageConnection for AntidoteConnectionManager {

    type Connection = TcpStream;
    type Error = PoolError;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        // let conn = TcpStream::connect(self.addr.clone()).unwrap();
        // Ok(conn)
        if let Ok(conn) = TcpStream::connect(self.addr.clone()) {
            Ok(conn)
        } else {
            thread::sleep(time::Duration::from_millis(CONNECT_RETRY_PERIOD));
            // Err(PoolError::new("Connection invalid"))
            // I guess thats a dangerous recursive retry? ^.^
            self.connect()
        }
    }
    fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Self::Error> {
        // This check takes A LOT of time... (~ nearly doubles the time for an interactive transaction) 

        // let get_cd = ApbGetConnectionDescriptor::new();
        // match get_cd.encode(conn) {
        //     Ok(()) => {},
        //     Err(e) => return Err(PoolError::new(format!("Connection invalid; Error: {}", e).as_str()))
        // }
        // let resp = decode_apb_get_connection_descriptor_resp(conn).unwrap();
        // if !resp.get_success() {
        //     return Err(PoolError::new("Connection invalid"))
        // }
        // let descriptor = resp.take_d();

        // Well we will just get an error while trying to write on the stream if the connection is dead 
        // and antidote will handle invalid calls and return an error that is captured in the coder as well...
        Ok(())
    }
    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}