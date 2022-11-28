


//////////////////////////////////////////////////
// data structures:

use std::{fmt::Display, io::Cursor, collections::HashMap};

use janus_core::hpke::{HpkePrivateKey, generate_hpke_config_and_private_key};
use janus_messages::{HpkeConfigId, HpkeConfig, HpkeKemId, HpkeKdfId, HpkeAeadId};
use prio::codec::{Encode, Decode, CodecError};
use rand::random;
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



/// This registry lazily generates up to 256 HPKE key pairs, one with each possible
/// [`HpkeConfigId`].
#[derive(Default)]
pub struct HpkeConfigRegistry {
    keypairs: HashMap<HpkeConfigId, (HpkeConfig, HpkePrivateKey)>,
}

impl HpkeConfigRegistry {
    pub fn new() -> HpkeConfigRegistry {
        Default::default()
    }

    /// Get the keypair associated with a given ID.
    pub fn fetch_keypair(&mut self, id: HpkeConfigId) -> (HpkeConfig, HpkePrivateKey) {
        self.keypairs
            .entry(id)
            .or_insert_with(|| {
                generate_hpke_config_and_private_key(
                    id,
                    // These algorithms should be broadly compatible with other DAP implementations, since they
                    // are required by section 6 of draft-ietf-ppm-dap-02.
                    HpkeKemId::X25519HkdfSha256,
                    HpkeKdfId::HkdfSha256,
                    HpkeAeadId::Aes128Gcm,
                )
            })
            .clone()
    }

    /// Choose a random [`HpkeConfigId`], and then get the keypair associated with that ID.
    pub fn get_random_keypair(&mut self) -> (HpkeConfig, HpkePrivateKey) {
        self.fetch_keypair(random::<u8>().into())
    }
}

