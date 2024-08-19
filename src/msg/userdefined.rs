use crate::{Frame, Parse};

use bytes::Bytes;

/// Message used for transmitting user defined message.
#[derive(Debug, Clone)]
pub struct UserDefined {
    pub name: String,
    pub data: Vec<u8>,
}

impl UserDefined {
    pub fn new(name: &str, data: &[u8]) -> UserDefined {
        UserDefined {
            name: name.to_string(),
            data: data.to_vec(),
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<UserDefined> {
        let name = parse.next_string()?;
        let data = parse.next_bytes()?.to_vec();

        Ok(UserDefined { name, data })
    }

    pub fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("userdefined".as_bytes()));
        frame.push_string(self.name);
        frame.push_bulk(self.data.into());
        frame
    }

    /// The serialized message of user's own type.
    /// NOTE: Consider if this need to consume the owner and don't clone
    pub fn get_data(&self) -> Vec<u8> {
        self.data.clone()
    }

    /// The message name of user's own type.
    pub fn get_name(&self) -> String {
        self.name.clone()
    }
}
