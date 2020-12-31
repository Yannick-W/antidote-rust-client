use crate::antidote_pb::*;
use byteorder::{ByteOrder, BigEndian};
use protobuf::{Message};
use std::io::{Read, Write, Error, ErrorKind};

fn read_msg_raw(reader: &mut dyn Read) -> Result<Vec<u8>, Error> {
    let mut size_b : [u8; 4] = [0; 4];
    // read the size of the message
    let mut count : usize = 0;
    while count < 4 {
        let n = reader.read(&mut size_b[count..])?;
        count += usize::from(n);
    }
    let size_i : usize = BigEndian::read_u32(&size_b) as usize;
    let mut data : Vec<u8> = Vec::new();
    data.resize(size_i, 0);

    count = 0;
    while count < size_i {
        let n = reader.read(&mut data[count..])?;
        count += usize::from(n);
    }     
    Ok(data)
}

fn encode_msg(message: &dyn Message, msg_code: u8, writer: &mut dyn Write) -> Result<(), Error> {
    let mut msg : Vec<u8> = message.write_to_bytes().unwrap();
    let msg_size: usize = msg.len()+1;
    let mut buf : [u8; 5] = [0; 5];
    BigEndian::write_u32_into(&[msg_size as u32], &mut buf[0..4]);
    buf[4] = msg_code;
    writer.write(&mut buf)?;
    writer.write(&mut msg)?;
    Ok(())
}

impl ApbReadObjects {
    pub fn encode(&self, writer : &mut dyn Write) -> Result<(), Error> {
        return encode_msg(self, 116, writer);
    }
}
impl ApbUpdateObjects {
    pub fn encode(&self, writer : &mut dyn Write) -> Result<(), Error> {
        return encode_msg(self, 118, writer);
    }
}
impl ApbStartTransaction {
    pub fn encode(&self, writer : &mut dyn Write) -> Result<(), Error> {
        return encode_msg(self, 119, writer);
    }
}
impl ApbAbortTransaction {
    pub fn encode(&self, writer : &mut dyn Write) -> Result<(), Error> {
        return encode_msg(self, 120, writer);
    }
}
impl ApbCommitTransaction {
    pub fn encode(&self, writer : &mut dyn Write) -> Result<(), Error> {
        return encode_msg(self, 121, writer);
    }
}
impl ApbStaticUpdateObjects {
    pub fn encode(&self, writer : &mut dyn Write) -> Result<(), Error> {
        return encode_msg(self, 122, writer);
    }
}
impl ApbStaticReadObjects {
    pub fn encode(&self, writer : &mut dyn Write) -> Result<(), Error> {
        return encode_msg(self, 123, writer);
    }
}
impl ApbCreateDC {
    pub fn encode(&self, writer : &mut dyn Write) -> Result<(), Error> {
        return encode_msg(self, 129, writer);
    }
}
impl ApbConnectToDCs {
    pub fn encode(&self, writer : &mut dyn Write) -> Result<(), Error> {
        return encode_msg(self, 131, writer);
    }
}
impl ApbGetConnectionDescriptor {
    pub fn encode(&self, writer : &mut dyn Write) -> Result<(), Error> {
        return encode_msg(self, 133, writer);
    }
}

pub fn decode_operation_resp(reader: &mut dyn Read) -> Result<ApbOperationResp, Error> {
    let data :Vec<u8> = read_msg_raw(reader)?;
    match data[0] {
        // transaction response
        111 => {
            let mut resp = ApbOperationResp::new();
            resp.merge_from_bytes(&data[1..]).unwrap(); // Unmarshal from go?
            return Ok(resp);
        }
        _ => {
            Err(Error::new(ErrorKind::Other, format!("Invalid message code: {}. Expected 111.", data[0])))
        }
    }
}

pub fn decode_start_transaction_resp(reader: &mut dyn Read) -> Result<ApbStartTransactionResp, Error> {
    let data :Vec<u8> = read_msg_raw(reader)?;
    match data[0] {
        // transaction response
        124 => {
            let mut resp = ApbStartTransactionResp::new();
            resp.merge_from_bytes(&data[1..]).unwrap(); // Unmarshal from go?
            return Ok(resp);
        }
        _ => {
            Err(Error::new(ErrorKind::Other, format!("Invalid message code: {}. Expected 124.", data[0])))
        }
    }
}

pub fn decode_read_objects_resp(reader: &mut dyn Read) -> Result<ApbReadObjectsResp, Error> {
    let data :Vec<u8> = read_msg_raw(reader)?;
    match data[0] {
        // transaction response
        126 => {
            let mut resp = ApbReadObjectsResp::new();
            resp.merge_from_bytes(&data[1..]).unwrap(); // Unmarshal from go?
            return Ok(resp);
        }
        _ => {
            Err(Error::new(ErrorKind::Other, format!("Invalid message code: {}. Expected 126.", data[0])))
        }
    }
}

pub fn decode_commit_resp(reader: &mut dyn Read) -> Result<ApbCommitResp, Error> {
    let data :Vec<u8> = read_msg_raw(reader)?;
    match data[0] {
        // transaction response
        127 => {
            let mut resp = ApbCommitResp::new();
            resp.merge_from_bytes(&data[1..]).unwrap(); // Unmarshal from go?
            return Ok(resp);
        }
        _ => {
            Err(Error::new(ErrorKind::Other, format!("Invalid message code: {}. Expected 127.", data[0])))
        }
    }
}

pub fn decode_static_read_objects_resp(reader: &mut dyn Read) -> Result<ApbStaticReadObjectsResp, Error> {
    let data :Vec<u8> = read_msg_raw(reader)?;
    match data[0] {
        // transaction response
        128 => {
            let mut resp = ApbStaticReadObjectsResp::new();
            resp.merge_from_bytes(&data[1..]).unwrap(); // Unmarshal from go?
            return Ok(resp);
        }
        _ => {
            Err(Error::new(ErrorKind::Other, format!("Invalid message code: {}. Expected 128.", data[0])))
        }
    }
}

pub fn decode_apb_create_dc_resp(reader: &mut dyn Read) -> Result<ApbCreateDCResp, Error> {
    let data :Vec<u8> = read_msg_raw(reader)?;
    match data[0] {
        // transaction response
        130 => {
            let mut resp = ApbCreateDCResp::new();
            resp.merge_from_bytes(&data[1..]).unwrap(); // Unmarshal from go?
            return Ok(resp);
        }
        _ => {
            Err(Error::new(ErrorKind::Other, format!("Invalid message code: {}. Expected 130.", data[0])))
        }
    }
}

pub fn decode_apb_connect_to_dcs_resp(reader: &mut dyn Read) -> Result<ApbConnectToDCsResp, Error> {
    let data :Vec<u8> = read_msg_raw(reader)?;
    match data[0] {
        // transaction response
        132 => {
            let mut resp = ApbConnectToDCsResp::new();
            resp.merge_from_bytes(&data[1..]).unwrap(); // Unmarshal from go?
            return Ok(resp);
        }
        _ => {
            Err(Error::new(ErrorKind::Other, format!("Invalid message code: {}. Expected 132.", data[0])))
        }
    }
}

pub fn decode_apb_get_connection_descriptor_resp(reader: &mut dyn Read) -> Result<ApbGetConnectionDescriptorResp, Error> {
    let data :Vec<u8> = read_msg_raw(reader)?;
    match data[0] {
        // transaction response
        134 => {
            let mut resp = ApbGetConnectionDescriptorResp::new();
            resp.merge_from_bytes(&data[1..]).unwrap(); // Unmarshal from go?
            return Ok(resp);
        }
        _ => {
            Err(Error::new(ErrorKind::Other, format!("Invalid message code: {}. Expected 134.", data[0])))
        }
    }
}