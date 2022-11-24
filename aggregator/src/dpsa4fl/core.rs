


//////////////////////////////////////////////////
// data structures:

use std::{fmt::Display, io::Cursor};

use prio::codec::{Encode, Decode, CodecError};
use serde::{Serialize, Deserialize};

/// DPSA protocol message representing an identifier for a Training Session.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TrainingSessionId(u16);

impl Display for TrainingSessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Encode for TrainingSessionId {
    fn encode(&self, bytes: &mut Vec<u8>) {
        self.0.encode(bytes);
    }
}

impl Decode for TrainingSessionId {
    fn decode(bytes: &mut Cursor<&[u8]>) -> Result<Self, CodecError> {
        Ok(Self(u16::decode(bytes)?))
    }
}

impl From<u16> for TrainingSessionId {
    fn from(value: u16) -> TrainingSessionId {
        TrainingSessionId(value)
    }
}

impl From<TrainingSessionId> for u16 {
    fn from(id: TrainingSessionId) -> u16 {
        id.0
    }
}
