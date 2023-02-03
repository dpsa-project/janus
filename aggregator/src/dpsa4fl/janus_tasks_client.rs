
use std::time::UNIX_EPOCH;

use base64::{engine::general_purpose, Engine};
// use base64::URL_SAFE_NO_PAD;
use http::StatusCode;
use janus_core::{hpke::{HpkePrivateKey, generate_hpke_config_and_private_key, HpkeKeypair}, task::AuthenticationToken};
use janus_messages::{Role, HpkeConfig, HpkeKemId, HpkeKdfId, HpkeAeadId, TaskId, Interval, Time, Duration, query_type::TimeInterval, Query};
use janus_collector::{Collector, CollectorParameters, Collection};
use prio::{codec::{Encode, Decode, CodecError}, vdaf::prio3::{Prio3Aes128CountVec, Prio3Aes128FixedPointBoundedL2VecSum}};
use anyhow::{anyhow, Context, Result, Error};
use rand::random;
use fixed::types::extra::{U15, U31, U63};
use fixed::{FixedI16, FixedI32, FixedI64};
use url::Url;

use crate::task::PRIO3_AES128_VERIFY_KEY_LENGTH;

use super::core::{TrainingSessionId, CreateTrainingSessionRequest, CreateTrainingSessionResponse, StartRoundRequest, Locations};

pub type Fx = FixedI32<U31>;
pub const TIME_PRECISION: u64 = 3600;


pub struct JanusTasksClient
{
    http_client: reqwest::Client,
    location: Locations,
    num_gradient_entries: usize,
    hpke_keypair: HpkeKeypair,
    // hpke_config: HpkeConfig,
    // hpke_private_key: HpkePrivateKey,
    leader_auth_token: AuthenticationToken,
    collector_auth_token: AuthenticationToken,
    noise_parameter: u8,
}


impl JanusTasksClient
{
    pub fn new(
        location: Locations,
        num_gradient_entries: usize,
        noise_parameter: u8,
    ) -> Self
    {
        let leader_auth_token = rand::random::<[u8; 16]>().to_vec().into();
        let collector_auth_token = rand::random::<[u8; 16]>().to_vec().into();

        let hpke_id = random::<u8>().into();
        let hpke_keypair = generate_hpke_config_and_private_key(
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
            hpke_keypair,
            // hpke_config,
            // hpke_private_key,
            leader_auth_token,
            collector_auth_token,
            noise_parameter,
        }
    }

    pub async fn create_session(&self) -> Result<TrainingSessionId>
    {
        let leader_auth_token_encoded = general_purpose::URL_SAFE_NO_PAD.encode(self.leader_auth_token.as_bytes());
        let collector_auth_token_encoded = general_purpose::URL_SAFE_NO_PAD.encode(self.collector_auth_token.as_bytes());
            // base64::encode_config(self.collector_auth_token.as_bytes(), URL_SAFE_NO_PAD);
        let verify_key = rand::random::<[u8; PRIO3_AES128_VERIFY_KEY_LENGTH]>();
        let verify_key_encoded = general_purpose::URL_SAFE_NO_PAD.encode(&verify_key);
            // base64::encode_config(&verify_key, URL_SAFE_NO_PAD);

        let make_request = |role, id| CreateTrainingSessionRequest {
            training_session_id: id,
            leader_endpoint: self.location.internal_leader.clone(),
            helper_endpoint: self.location.internal_helper.clone(),
            role,
            num_gradient_entries: self.num_gradient_entries,
            verify_key_encoded: verify_key_encoded.clone(),
            collector_hpke_config: self.hpke_keypair.config().clone(),
            collector_auth_token_encoded: collector_auth_token_encoded.clone(),
            leader_auth_token_encoded: leader_auth_token_encoded.clone(),
            noise_parameter: self.noise_parameter,
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
        let task_id_encoded = general_purpose::URL_SAFE_NO_PAD.encode(&task_id.get_encoded());
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
    pub async fn collect(&self, task_id: TaskId) -> Result<Collection<Vec<f64>, TimeInterval>>
    {
        let params = CollectorParameters::new(
            task_id,
            self.location.external_leader_main.clone(),
            self.collector_auth_token.clone(),
            self.hpke_keypair.config().clone(),
            self.hpke_keypair.private_key().clone(),
            // self.hpke_config.clone(),
            // self.hpke_private_key.clone(),
        );

        let vdaf_collector = Prio3Aes128FixedPointBoundedL2VecSum::<Fx>::new_aes128_fixedpoint_boundedl2_vec_sum(2, self.num_gradient_entries, 0)?;


        // we need to redirect urls returned by janus because janus gives us its
        // internal domain, but we need the external one.
        // let custom = redirect::Policy::custom(|attempt| {
            // if attempt.url().host_str() == Some(self.location.internal_leader)
            // {
            //     attempt.url().
            // }
            // else
            // {
            //     attempt.follow()
            // }
            // if attempt.previous().len() > 5 {
            //     attempt.error("too many redirects")
            // } else if attempt.url().host_str() == Some("example.domain") {
            //     // prevent redirects to 'example.domain'
            //     attempt.stop()
            // } else {
            //     attempt.follow()
            // }
        // });

        let collector_http_client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()?;

        let collector_client = Collector::new(params, vdaf_collector, collector_http_client);

        let start = UNIX_EPOCH.elapsed()?.as_secs();
        let rounded_start = (start / TIME_PRECISION) * TIME_PRECISION;
        let real_start = Time::from_seconds_since_epoch(rounded_start - TIME_PRECISION * 5);
        let duration = Duration::from_seconds(TIME_PRECISION * 15);

        // let deadline = UNIX_EPOCH.elapsed()?.as_secs() + 60*5;
        // let rounded_deadline = (deadline / TIME_PRECISION) * TIME_PRECISION;
        // let start = Time::from_seconds_since_epoch(0);
        // let duration = Duration::from_seconds(rounded_deadline);

        let aggregation_parameter = ();

        let host = self.location.external_leader_main.host().ok_or(anyhow!("Couldnt get hostname"))?;
        let port = self.location.external_leader_main.port().ok_or(anyhow!("Couldnt get port"))?;

        println!("patched host and port are: {host} -:- {port}");

        println!("collecting result now");

        let result = collector_client.collect_with_rewritten_url(Query::new(Interval::new(real_start, duration)?), &aggregation_parameter, &host.to_string(), port).await?;

        // let result = collector_client.collect(Query::new(Interval::new(start, duration)?), &aggregation_parameter).await?;

        Ok(result)
    }

}
