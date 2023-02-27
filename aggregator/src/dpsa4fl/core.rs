//////////////////////////////////////////////////
// data structures:

use std::{collections::HashMap, fmt::Display, io::Cursor};

use janus_core::hpke::{generate_hpke_config_and_private_key, HpkeKeypair};
use janus_messages::{HpkeAeadId, HpkeConfig, HpkeConfigId, HpkeKdfId, HpkeKemId, Role};
use prio::codec::{CodecError, Decode, Encode};
use prio::flp::types::fixedpoint_l2::NoiseParameterType;
use rand::random;
use serde::{Deserialize, Serialize};
use url::Url;

/////////////////////////////
// Locations

#[derive(Clone)]
pub struct Locations {
    pub internal_leader: Url, // TODO: This internal URL should probably be configured somewhere else, actually
    pub internal_helper: Url, // TODO: Same.
    pub external_leader_tasks: Url,
    pub external_helper_tasks: Url,
    pub external_leader_main: Url,
    pub external_helper_main: Url,
    // controller: Url, // the server that controls the learning process
}

impl Locations {
    pub fn get_external_aggregator_endpoints(&self) -> Vec<Url> {
        vec![
            self.external_leader_main.clone(),
            self.external_helper_main.clone(),
        ]
    }
}

/////////////////////////////
// data

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
    keypairs: HashMap<HpkeConfigId, HpkeKeypair>,
}

impl HpkeConfigRegistry {
    pub fn new() -> HpkeConfigRegistry {
        Default::default()
    }

    /// Get the keypair associated with a given ID.
    pub fn fetch_keypair(&mut self, id: HpkeConfigId) -> HpkeKeypair {
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
    pub fn get_random_keypair(&mut self) -> HpkeKeypair {
        self.fetch_keypair(random::<u8>().into())
    }
}

//////////////////////////////////////////////////
// api:
//
//--- create training session ---

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTrainingSessionRequest {
    // id if known
    pub training_session_id: Option<TrainingSessionId>,

    // endpoints
    pub leader_endpoint: Url,
    pub helper_endpoint: Url,

    //
    pub role: Role,
    pub num_gradient_entries: usize,

    // needs to be the same for both aggregators (section 4.2 of ppm-draft)
    pub verify_key_encoded: String, // in unpadded base64url

    pub collector_hpke_config: HpkeConfig,

    // auth tokens
    pub collector_auth_token_encoded: String, // in unpadded base64url
    pub leader_auth_token_encoded: String,    // in unpadded base64url

    // noise params
    pub noise_parameter: NoiseParameterType,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTrainingSessionResponse {
    pub training_session_id: TrainingSessionId,
}

//--- start training round ---

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartRoundRequest {
    pub training_session_id: TrainingSessionId,
    pub task_id_encoded: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartRoundResponse {
    // pub training_session_id: TrainingSessionId
}
