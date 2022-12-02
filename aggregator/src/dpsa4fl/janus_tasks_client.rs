
use base64::URL_SAFE_NO_PAD;
use http::StatusCode;
use janus_core::hpke::{HpkePrivateKey, generate_hpke_config_and_private_key};
use janus_messages::{Role, HpkeConfig, HpkeKemId, HpkeKdfId, HpkeAeadId, TaskId};
use prio::codec::{Encode, Decode, CodecError};
use anyhow::{anyhow, Context, Result, Error};
use rand::random;
use url::Url;

use crate::task::PRIO3_AES128_VERIFY_KEY_LENGTH;

use super::core::{TrainingSessionId, CreateTrainingSessionRequest, CreateTrainingSessionResponse, StartRoundRequest};


pub struct JanusTasksClient
{
    http_client: reqwest::Client,
    external_leader_endpoint: Url,
    external_helper_endpoint: Url,
    leader_endpoint: Url,
    helper_endpoint: Url,
    num_gradient_entries: usize,
    hpke_config: HpkeConfig,
    hpke_private_key: HpkePrivateKey,
}


impl JanusTasksClient
{
    pub fn new(
        external_leader_endpoint: Url,
        external_helper_endpoint: Url,
        leader_endpoint: Url,
        helper_endpoint: Url,
        num_gradient_entries: usize
    ) -> Self
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
            external_leader_endpoint,
            external_helper_endpoint,
            leader_endpoint,
            helper_endpoint,
            num_gradient_entries,
            hpke_config,
            hpke_private_key,
        }
    }

    pub async fn create_session(&self) -> Result<TrainingSessionId>
    {
        let leader_auth_token_encoded = base64::encode_config(rand::random::<[u8; 16]>(), URL_SAFE_NO_PAD);
        let collector_auth_token_encoded = base64::encode_config(rand::random::<[u8; 16]>(), URL_SAFE_NO_PAD);
        let verify_key = rand::random::<[u8; PRIO3_AES128_VERIFY_KEY_LENGTH]>();
        let verify_key_encoded = base64::encode_config(&verify_key, URL_SAFE_NO_PAD);

        let make_request = |role, id| CreateTrainingSessionRequest {
            training_session_id: id,
            leader_endpoint: self.leader_endpoint.clone(),
            helper_endpoint: self.helper_endpoint.clone(),
            role,
            num_gradient_entries: self.num_gradient_entries,
            verify_key_encoded: verify_key_encoded.clone(),
            collector_hpke_config: self.hpke_config.clone(),
            collector_auth_token_encoded: collector_auth_token_encoded.clone(),
            leader_auth_token_encoded: leader_auth_token_encoded.clone(),
        };

        // send request to leader first
        // and get response
        let leader_response = self.http_client
            .post(self.external_leader_endpoint.join("/create_session").unwrap())
            .json(&make_request(Role::Leader, None))
            .send()
            .await?;
        let leader_response = match leader_response.status()
        {
            StatusCode::OK =>
            {
                let response: CreateTrainingSessionResponse = leader_response.json().await?;
                response
            }
            res =>
            {
                return Err(anyhow!("Got error from leader: {res}"));
            }
        };

        let helper_response = self.http_client
            .post(self.external_helper_endpoint.join("/create_session").unwrap())
            .json(&make_request(Role::Helper, Some(leader_response.training_session_id)))
            .send()
            .await?;

        let helper_response = match helper_response.status()
        {
            StatusCode::OK =>
            {
                let response: CreateTrainingSessionResponse = helper_response.json().await?;
                response
            }
            res =>
            {
                return Err(anyhow!("Got error from helper: {res}"));
            }
        };

        assert!(helper_response.training_session_id == leader_response.training_session_id, "leader and helper have different training session id!");

        Ok(leader_response.training_session_id)

    }

    /// Send requests to the aggregators to start a new round.
    ///
    /// We return the task id with which the task can be collected.
    pub async fn start_round(&self, training_session_id: TrainingSessionId) -> Result<TaskId>
    {
        let task_id: TaskId = random();
        let task_id_encoded = base64::encode_config(&task_id.get_encoded(), URL_SAFE_NO_PAD);
        let request: StartRoundRequest = StartRoundRequest {
            training_session_id,
            task_id_encoded,
        };
        let leader_response = self.http_client
            .post(self.external_leader_endpoint.join("/start_round").unwrap())
            .json(&request)
            .send()
            .await?;

        let helper_response = self.http_client
            .post(self.external_helper_endpoint.join("/start_round").unwrap())
            .json(&request)
            .send()
            .await?;

        match (leader_response.status(), helper_response.status())
        {
            (StatusCode::OK, StatusCode::OK) =>
            {
                Ok(task_id)
            }
            (res1, res2) =>
            {
                Err(anyhow!("Starting round not successful, results are: \n{res1}\n\n{res2}"))
            }
        }
    }

}

