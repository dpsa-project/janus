
use std::time::UNIX_EPOCH;

use base64::URL_SAFE_NO_PAD;
use http::StatusCode;
use janus_core::{hpke::{HpkePrivateKey, generate_hpke_config_and_private_key}, task::AuthenticationToken};
use janus_messages::{Role, HpkeConfig, HpkeKemId, HpkeKdfId, HpkeAeadId, TaskId, Interval, Time, Duration};
use janus_collector::{Collector, CollectorParameters, Collection};
use prio::{codec::{Encode, Decode, CodecError}, vdaf::prio3::{Prio3Aes128CountVec, Prio3Aes128FixedPointBoundedL2VecSum}};
use anyhow::{anyhow, Context, Result, Error};
use rand::random;
use fixed::types::extra::{U15, U31, U63};
use fixed::{FixedI16, FixedI32, FixedI64};
use url::Url;

use crate::task::PRIO3_AES128_VERIFY_KEY_LENGTH;

use super::core::{TrainingSessionId, CreateTrainingSessionRequest, CreateTrainingSessionResponse, StartRoundRequest, Locations};

type Fx = FixedI32<U31>;


pub struct JanusTasksClient
{
    http_client: reqwest::Client,
    location: Locations,
    num_gradient_entries: usize,
    hpke_config: HpkeConfig,
    hpke_private_key: HpkePrivateKey,
    leader_auth_token: AuthenticationToken,
    collector_auth_token: AuthenticationToken,
}


impl JanusTasksClient
{
    pub fn new(
        location: Locations,
        num_gradient_entries: usize,
    ) -> Self
    {
        let leader_auth_token = rand::random::<[u8; 16]>().to_vec().into();
        let collector_auth_token = rand::random::<[u8; 16]>().to_vec().into();

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
            location,
            num_gradient_entries,
            hpke_config,
            hpke_private_key,
            leader_auth_token,
            collector_auth_token,
        }
    }

    pub async fn create_session(&self) -> Result<TrainingSessionId>
    {
        let leader_auth_token_encoded = base64::encode_config(self.leader_auth_token.as_bytes(), URL_SAFE_NO_PAD);
        let collector_auth_token_encoded = base64::encode_config(self.collector_auth_token.as_bytes(), URL_SAFE_NO_PAD);
        let verify_key = rand::random::<[u8; PRIO3_AES128_VERIFY_KEY_LENGTH]>();
        let verify_key_encoded = base64::encode_config(&verify_key, URL_SAFE_NO_PAD);

        let make_request = |role, id| CreateTrainingSessionRequest {
            training_session_id: id,
            leader_endpoint: self.location.internal_leader.clone(),
            helper_endpoint: self.location.internal_helper.clone(),
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
            .post(self.location.external_leader_tasks.join("/create_session").unwrap())
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
            .post(self.location.external_helper_tasks.join("/create_session").unwrap())
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
            .post(self.location.external_leader_tasks.join("/start_round").unwrap())
            .json(&request)
            .send()
            .await?;

        let helper_response = self.http_client
            .post(self.location.external_helper_tasks.join("/start_round").unwrap())
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

    /// Collect results
    pub async fn collect(&self, task_id: TaskId) -> Result<Collection<Vec<f64>>>
    {
        let params = CollectorParameters::new(
            task_id,
            self.location.external_leader_main.clone(),
            self.collector_auth_token.clone(),
            self.hpke_config.clone(),
            self.hpke_private_key.clone(),
        );

        let vdaf_collector = Prio3Aes128FixedPointBoundedL2VecSum::<Fx>::new_aes128_fixedpoint_boundedl2_vec_sum(2, self.num_gradient_entries)?;

        let collector_client = Collector::new(params, vdaf_collector, self.http_client.clone());

        let deadline = UNIX_EPOCH.elapsed()?.as_secs() + 7200;
        let start = Time::from_seconds_since_epoch(0);
        let duration = Duration::from_seconds(deadline);

        let aggregation_parameter = ();

        let result = collector_client.collect(Interval::new(start, duration)?, &aggregation_parameter).await?;

        Ok(result)
    }

}

