
use prio::codec::{Encode, Decode, CodecError};
use anyhow::{anyhow, Context, Result, Error};
use url::Url;

use super::core::TrainingSessionId;


pub struct JanusTasksClient
{
    http_client: reqwest::Client,
    endpoint: Url,
}


impl JanusTasksClient
{
    pub fn new(endpoint: Url) -> Self
    {
        JanusTasksClient {
            http_client: reqwest::Client::new(),
            endpoint,
        }
    }

    pub async fn create_session(self, id: TrainingSessionId) -> Result<()>
    {
        let res = self.http_client.get(self.endpoint)
            .query(&[("training_session_id", id)])
            .send()
            .await?;

        let status = res.status();
        if !status.is_success()
        {
            return Err(anyhow!("'create session' return error, {:?}", res.status()));
        }

        Ok(())
    }
}

