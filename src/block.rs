#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;
use nom::AsBytes;
pub const SIZEOF_U16: usize = 2;
/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        // let mut tmp = self.data.clone();
        // for e in self.offsets.clone() {
        //     tmp.put_u16(e);
        // }
        // tmp.put_u16(self.offsets.len() as u16);
        // Bytes::copy_from_slice(tmp.as_bytes())

        let mut buf = self.data.clone();
        let offsets_len = self.offsets.len();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        // Adds number of elements at the end of the block
        buf.put_u16(offsets_len as u16);
        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_of_elements = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let entry_end = data.len() - SIZEOF_U16 - num_of_elements * SIZEOF_U16;
        let offset_raw = &data[entry_end..data.len() - SIZEOF_U16];
        let offsets = offset_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        let entrys = data[0..entry_end].to_vec();
        Self {
            data: entrys,
            offsets,
        }
    }
}
