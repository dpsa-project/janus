
use base64::URL_SAFE_NO_PAD;
use http::StatusCode;
use janus_core::hpke::{HpkePrivateKey, generate_hpke_config_and_private_key};
use janus_messages::{Role, HpkeConfig, HpkeKemId, HpkeKdfId, HpkeAeadId};
use prio::codec::{Encode, Decode, CodecError};
use anyhow::{anyhow, Context, Result, Error};
use rand::random;
use url::Url;

use crate::task::PRIO3_AES128_VERIFY_KEY_LENGTH;

use super::core::{TrainingSessionId, CreateTrainingSessionRequest, CreateTrainingSessionResponse};


pub struct JanusTasksClient
{
    http_client: reqwest::Client,
    leader_endpoint: Url,
    helper_endpoint: Url,
    num_gradient_entries: usize,
    hpke_config: HpkeConfig,
    hpke_private_key: HpkePrivateKey,
}


impl JanusTasksClient
{
    pub fn new(leader_endpoint: Url, helper_endpoint: Url, num_gradient_entries: usize) -> Self
    {
        let hpke_id = random::<u8>().into();
        let (hpke_config, hpke_private_key) = generate_hpke_config_and_private_key(
                hpke_id,
                // These algorithms should be broadly compatible with other DAP implementations, since they
                // are required by section 6 of draft-ietf-ppm-dap-02.
                HpkeKemId::X25519HkdfSha256,
                HpkeKdfId::HkdfSha256,
                HpkeAeadId::Aes128Gcm,
            );
        JanusTasksClient {
            http_client: reqwest::Client::new(),
            leader_endpoint,
            helper_endpoint,
            num_gradient_entries,
            hpke_config,
            hpke_private_key,
        }
    }

    pub async fn create_session(&self) -> Result<()>
    {
        let leader_auth_token_encoded = base64::encode_config(rand::random::<[u8; 16]>(), URL_SAFE_NO_PAD);
        let collector_auth_token_encoded = base64::encode_config(rand::random::<[u8; 16]>(), URL_SAFE_NO_PAD);
        let verify_key = rand::random::<[u8; PRIO3_AES128_VERIFY_KEY_LENGTH]>();
        let verify_key_encoded = base64::encode_config(&verify_key, URL_SAFE_NO_PAD);

        let make_request = |role| CreateTrainingSessionRequest {
            leader_endpoint: self.leader_endpoint.clone(),
            helper_endpoint: self.helper_endpoint.clone(),
            role,
            num_gradient_entries: self.num_gradient_entries,
            verify_key_encoded: verify_key_encoded.clone(),
            collector_hpke_config: self.hpke_config.clone(),
            collector_auth_token_encoded: collector_auth_token_encoded.clone(),
            leader_auth_token_encoded: leader_auth_token_encoded.clone(),
        };

        let leader_response = self.http_client
            .post(self.leader_endpoint.clone())
            .json(&make_request(Role::Leader))
            .send()
            .await?;

        let helper_response = self.http_client
            .post(self.leader_endpoint.clone())
            .json(&make_request(Role::Helper))
            .send()
            .await?;

        match (leader_response.status(), helper_response.status())
        {
            (StatusCode::OK, StatusCode::OK) =>
            {
                println!("successfully created session with: {leader_response:?}\nand\n {helper_response:?}");
                return Ok(());
            }
            (res1, res2) =>
            {
                return Err(anyhow!("Got errors results: \n {res1} \n {res2}"));
            }
        }
        // let res = self.http_client.get(self.leader_endpoint.clone())
        //     .query(&[("training_session_id", id)])
        //     .send()
        //     .await?;

        // let status = res.status();
        // if !status.is_success()
        // {
        //     return Err(anyhow!("'create session' returned error, {:?}", res.status()));
        // }

    }

}

