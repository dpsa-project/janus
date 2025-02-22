use crate::TaskParameters;
use anyhow::anyhow;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use janus_client::Client;
use janus_core::vdaf::VdafInstance;
use janus_interop_binaries::{get_rust_log_level, ContainerLogsDropGuard};
use janus_messages::{Duration, TaskId};
use prio::{
    codec::Encode,
    vdaf::{
        self,
        prio3::{Prio3Count, Prio3Histogram, Prio3Sum, Prio3SumVecMultithreaded},
    },
};
use rand::random;
use serde_json::{json, Value};
use std::env;
use testcontainers::{clients::Cli, core::WaitFor, Image, RunnableImage};
use url::Url;

/// Extension trait to encode measurements for VDAFs as JSON objects, according to
/// draft-dcook-ppm-dap-interop-test-design.
pub trait InteropClientEncoding: vdaf::Client<16> {
    fn json_encode_measurement(&self, measurement: &Self::Measurement) -> Value;
}

impl InteropClientEncoding for Prio3Count {
    fn json_encode_measurement(&self, measurement: &Self::Measurement) -> Value {
        Value::String(format!("{measurement}"))
    }
}

impl InteropClientEncoding for Prio3Sum {
    fn json_encode_measurement(&self, measurement: &Self::Measurement) -> Value {
        Value::String(format!("{measurement}"))
    }
}

impl InteropClientEncoding for Prio3Histogram {
    fn json_encode_measurement(&self, measurement: &Self::Measurement) -> Value {
        Value::String(format!("{measurement}"))
    }
}

impl InteropClientEncoding for Prio3SumVecMultithreaded {
    fn json_encode_measurement(&self, measurement: &Self::Measurement) -> Value {
        Value::Array(
            measurement
                .iter()
                .map(|value| Value::String(format!("{value}")))
                .collect(),
        )
    }
}

fn json_encode_vdaf(vdaf: &VdafInstance) -> Value {
    match vdaf {
        VdafInstance::Prio3Count => json!({
            "type": "Prio3Count"
        }),
        VdafInstance::Prio3Sum { bits } => json!({
            "type": "Prio3Sum",
            "bits": format!("{bits}"),
        }),
        VdafInstance::Prio3SumVec {
            bits,
            length,
            chunk_length,
        } => json!({
            "type": "Prio3SumVec",
            "bits": format!("{bits}"),
            "length": format!("{length}"),
            "chunk_length": format!("{chunk_length}"),
        }),
        VdafInstance::Prio3Histogram {
            length,
            chunk_length,
        } => {
            json!({
                "type": "Prio3Histogram",
                "length": format!("{length}"),
                "chunk_length": format!("{chunk_length}"),
            })
        }
        _ => panic!("VDAF {vdaf:?} is not yet supported"),
    }
}

/// This represents a container image that implements the client role of
/// draft-dcook-ppm-dap-interop-test-design, for use with [`testcontainers`].
#[derive(Clone)]
pub struct InteropClient {
    name: String,
    tag: String,
}

impl InteropClient {
    /// By default, this creates an object referencing the latest divviup-ts interoperation test
    /// container image (for the correct DAP version). If the environment variable
    /// `DIVVIUP_TS_INTEROP_CONTAINER is set to a name and tag, then that image will be used
    /// instead.
    pub fn divviup_ts() -> InteropClient {
        if let Ok(value) = env::var("DIVVIUP_TS_INTEROP_CONTAINER") {
            if let Some((name, tag)) = value.rsplit_once(':') {
                InteropClient {
                    name: name.to_string(),
                    tag: tag.to_string(),
                }
            } else {
                InteropClient {
                    name: value.to_string(),
                    tag: "latest".to_string(),
                }
            }
        } else {
            InteropClient {
                name: "us-west2-docker.pkg.dev/divviup-artifacts-public/divviup-ts/\
                       divviup_ts_interop_client"
                    .to_string(),
                tag: "dap-draft-04@sha256:\
                      43ccdf68e319c677f12f0cb730c63e73b872477cf0e1310b727f449b74a14ac2"
                    .to_string(),
            }
        }
    }
}

impl Image for InteropClient {
    type Args = ();

    fn name(&self) -> String {
        self.name.clone()
    }

    fn tag(&self) -> String {
        self.tag.clone()
    }

    fn ready_conditions(&self) -> Vec<testcontainers::core::WaitFor> {
        Vec::from([WaitFor::Healthcheck])
    }
}

/// This selects which DAP client implementation will be used in an integration test.
pub enum ClientBackend<'a> {
    /// Uploads reports using `janus-client` as a library.
    InProcess,
    /// Uploads reports by starting a containerized client implementation, and sending it requests
    /// using draft-dcook-ppm-dap-interop-test-design.
    Container {
        container_client: &'a Cli,
        container_image: InteropClient,
        network: &'a str,
    },
}

impl<'a> ClientBackend<'a> {
    pub async fn build<V>(
        &self,
        test_name: &str,
        task_parameters: &TaskParameters,
        (leader_port, helper_port): (u16, u16),
        vdaf: V,
    ) -> anyhow::Result<ClientImplementation<'a, V>>
    where
        V: vdaf::Client<16> + InteropClientEncoding,
    {
        match self {
            ClientBackend::InProcess => ClientImplementation::new_in_process(
                task_parameters,
                (leader_port, helper_port),
                vdaf,
            )
            .await
            .map_err(Into::into),
            ClientBackend::Container {
                container_client,
                container_image,
                network,
            } => Ok(ClientImplementation::new_container(
                test_name,
                container_client,
                container_image.clone(),
                network,
                task_parameters,
                vdaf,
            )),
        }
    }
}

pub struct ContainerClientImplementation<'d, V>
where
    V: vdaf::Client<16>,
{
    _container: ContainerLogsDropGuard<'d, InteropClient>,
    leader: Url,
    helper: Url,
    task_id: TaskId,
    time_precision: Duration,
    vdaf: V,
    vdaf_instance: VdafInstance,
    host_port: u16,
    http_client: reqwest::Client,
}

/// A DAP client implementation, specialized to work with a particular VDAF. See also
/// [`ClientBackend`].
pub enum ClientImplementation<'d, V>
where
    V: vdaf::Client<16>,
{
    InProcess { client: Client<V> },
    Container(Box<ContainerClientImplementation<'d, V>>),
}

impl<'d, V> ClientImplementation<'d, V>
where
    V: vdaf::Client<16> + InteropClientEncoding,
{
    pub async fn new_in_process(
        task_parameters: &TaskParameters,
        (leader_port, helper_port): (u16, u16),
        vdaf: V,
    ) -> Result<ClientImplementation<'static, V>, janus_client::Error> {
        let (leader_aggregator_endpoint, helper_aggregator_endpoint) = task_parameters
            .endpoint_fragments
            .endpoints_for_host_client(leader_port, helper_port);
        let client = Client::new(
            task_parameters.task_id,
            leader_aggregator_endpoint,
            helper_aggregator_endpoint,
            task_parameters.time_precision,
            vdaf,
        )
        .await?;
        Ok(ClientImplementation::InProcess { client })
    }

    pub fn new_container(
        test_name: &str,
        container_client: &'d Cli,
        container_image: InteropClient,
        network: &str,
        task_parameters: &TaskParameters,
        vdaf: V,
    ) -> Self {
        let random_part = hex::encode(random::<[u8; 4]>());
        let client_container_name = format!("client-{random_part}");
        let container = ContainerLogsDropGuard::new_janus(
            test_name,
            container_client.run(
                RunnableImage::from(container_image)
                    .with_network(network)
                    .with_env_var(get_rust_log_level())
                    .with_container_name(client_container_name),
            ),
        );
        let host_port = container.get_host_port_ipv4(8080);
        let http_client = reqwest::Client::new();
        let (leader_aggregator_endpoint, helper_aggregator_endpoint) = task_parameters
            .endpoint_fragments
            .endpoints_for_virtual_network_client();
        ClientImplementation::Container(Box::new(ContainerClientImplementation {
            _container: container,
            leader: leader_aggregator_endpoint,
            helper: helper_aggregator_endpoint,
            task_id: task_parameters.task_id,
            time_precision: task_parameters.time_precision,
            vdaf,
            vdaf_instance: task_parameters.vdaf.clone(),
            host_port,
            http_client,
        }))
    }

    pub async fn upload(&self, measurement: &V::Measurement) -> anyhow::Result<()> {
        match self {
            ClientImplementation::InProcess { client } => {
                client.upload(measurement).await.map_err(Into::into)
            }
            ClientImplementation::Container(inner) => {
                let task_id_encoded = URL_SAFE_NO_PAD.encode(inner.task_id.get_encoded());
                let upload_response = inner
                    .http_client
                    .post(format!(
                        "http://127.0.0.1:{}/internal/test/upload",
                        inner.host_port
                    ))
                    .json(&json!({
                        "task_id": task_id_encoded,
                        "leader": inner.leader,
                        "helper": inner.helper,
                        "vdaf": json_encode_vdaf(&inner.vdaf_instance),
                        "measurement": inner.vdaf.json_encode_measurement(measurement),
                        "time_precision": inner.time_precision.as_seconds(),
                    }))
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<Value>()
                    .await?;
                match upload_response.get("status") {
                    Some(status) if status == "success" => Ok(()),
                    Some(status) => Err(anyhow!(
                        "upload request got {status} status, error is {:?}",
                        upload_response.get("error")
                    )),
                    None => Err(anyhow!("upload response is missing \"status\"")),
                }
            }
        }
    }
}
