use anyhow::{anyhow, Context};
use base64::{
    alphabet::URL_SAFE,
    engine::fast_portable::{FastPortable, NO_PAD},
};
use clap::{value_parser, Arg, Command};
use fixed::types::extra::U31;
use fixed::FixedI32;
use janus_client::ClientParameters;
use janus_core::{
    task::VdafInstance,
    time::{MockClock, RealClock},
};
use janus_interop_binaries::{
    install_tracing_subscriber,
    status::{ERROR, SUCCESS},
    NumberAsString, VdafObject,
};
use janus_messages::{Duration, Role, TaskId, Time};
use prio::vdaf::prio3::Prio3Aes128FixedPointBoundedL2VecSum;
use prio::{
    codec::Decode,
    vdaf::{prio3::Prio3, Vdaf},
};
use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, SocketAddr};
use url::Url;
use warp::{hyper::StatusCode, reply::Response, Filter, Reply};

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Measurement {
    Number(NumberAsString<u128>),
    NumberVec(Vec<NumberAsString<u128>>),
    FixedVec(Vec<NumberAsString<FixedI32<U31>>>),
}

impl Measurement {
    fn as_primitive<T: TryFrom<u128>>(&self) -> anyhow::Result<T> {
        match self {
            Measurement::Number(m) => {
                T::try_from(m.0).map_err(|_| anyhow!("could not convert primitive"))
            }
            m => Err(anyhow!(
                "cannot represent measurement {m:?} as a primitive value"
            )),
        }
    }

    fn as_primitive_vec<T: TryFrom<u128>>(&self) -> anyhow::Result<Vec<T>> {
        match self {
            Measurement::NumberVec(vec) => vec
                .iter()
                .map(|item| T::try_from(item.0).map_err(|_| anyhow!("could not convert primitive")))
                .collect::<anyhow::Result<Vec<_>>>(),
            m => Err(anyhow!(
                "cannot represent measurement {m:?} as a vector of primitives"
            )),
        }
    }

    fn as_fixed_vec(&self) -> anyhow::Result<Vec<FixedI32<U31>>> {
        match self {
            Measurement::FixedVec(vec) => Ok(vec.iter().map(|item| item.0).collect()),
            m => Err(anyhow!(
                "cannot represent measurement {m:?} as a vector of fixed point numbers"
            )),
        }
    }
}

#[derive(Debug, Deserialize)]
struct UploadRequest {
    task_id: String,
    leader: Url,
    helper: Url,
    vdaf: VdafObject,
    measurement: Measurement,
    #[serde(default)]
    time: Option<u64>,
    time_precision: u64,
}

#[derive(Debug, Serialize)]
struct UploadResponse {
    status: &'static str,
    #[serde(default)]
    error: Option<String>,
}

async fn handle_upload_generic<V: prio::vdaf::Client>(
    http_client: &reqwest::Client,
    vdaf_client: V,
    request: UploadRequest,
    measurement: V::Measurement,
) -> anyhow::Result<()>
where
    for<'a> Vec<u8>: From<&'a <V as Vdaf>::AggregateShare>,
{
    let task_id_bytes = base64::decode_engine(request.task_id, &URL_SAFE_NO_PAD)
        .context("invalid base64url content in \"task_id\"")?;
    let task_id = TaskId::get_decoded(&task_id_bytes).context("invalid length of TaskId")?;
    let time_precision = Duration::from_seconds(request.time_precision);
    let client_parameters = ClientParameters::new(
        task_id,
        Vec::<Url>::from([request.leader, request.helper]),
        time_precision,
    );

    let leader_hpke_config = janus_client::aggregator_hpke_config(
        &client_parameters,
        &Role::Leader,
        &task_id,
        http_client,
    )
    .await
    .context("failed to fetch leader's HPKE configuration")?;
    let helper_hpke_config = janus_client::aggregator_hpke_config(
        &client_parameters,
        &Role::Helper,
        &task_id,
        http_client,
    )
    .await
    .context("failed to fetch helper's HPKE configuration")?;

    match request.time {
        Some(timestamp) => {
            let clock = MockClock::new(Time::from_seconds_since_epoch(timestamp));
            let client = janus_client::Client::new(
                client_parameters,
                vdaf_client,
                clock,
                http_client,
                leader_hpke_config,
                helper_hpke_config,
            );
            client
                .upload(&measurement)
                .await
                .context("report generation and upload failed")
        }
        None => {
            let client = janus_client::Client::new(
                client_parameters,
                vdaf_client,
                RealClock::default(),
                http_client,
                leader_hpke_config,
                helper_hpke_config,
            );
            client
                .upload(&measurement)
                .await
                .context("report generation and upload failed")
        }
    }
}

async fn handle_upload(
    http_client: &reqwest::Client,
    request: UploadRequest,
) -> anyhow::Result<()> {
    let vdaf_instance = request.vdaf.clone().into();
    match vdaf_instance {
        VdafInstance::Prio3Aes128Count {} => {
            let measurement = request.measurement.as_primitive()?;
            let vdaf_client =
                Prio3::new_aes128_count(2).context("failed to construct Prio3Aes128Count VDAF")?;
            handle_upload_generic(http_client, vdaf_client, request, measurement).await?;
        }

        VdafInstance::Prio3Aes128CountVec { length } => {
            let measurement = request.measurement.as_primitive_vec()?;
            let vdaf_client = Prio3::new_aes128_count_vec_multithreaded(2, length)
                .context("failed to construct Prio3Aes128CountVec VDAF")?;
            handle_upload_generic(http_client, vdaf_client, request, measurement).await?;
        }

        VdafInstance::Prio3Aes128Sum { bits } => {
            let measurement = request.measurement.as_primitive()?;
            let vdaf_client = Prio3::new_aes128_sum(2, bits)
                .context("failed to construct Prio3Aes128Sum VDAF")?;
            handle_upload_generic(http_client, vdaf_client, request, measurement).await?;
        }

        VdafInstance::Prio3Aes128Histogram { ref buckets } => {
            let measurement = request.measurement.as_primitive()?;
            let vdaf_client = Prio3::new_aes128_histogram(2, buckets)
                .context("failed to construct Prio3Aes128Histogram VDAF")?;
            handle_upload_generic(http_client, vdaf_client, request, measurement).await?;
        }
        VdafInstance::Prio3Aes128FixedPointBoundedL2VecSum { entries } => {
            let measurement = request.measurement.as_fixed_vec()?;
            let vdaf_client: Prio3Aes128FixedPointBoundedL2VecSum<FixedI32<U31>> =
                Prio3::new_aes128_fixedpoint_boundedl2_vec_sum(2, entries)
                    .context("failed to construct Prio3Aes128CountVec VDAF")?;
            handle_upload_generic(http_client, vdaf_client, request, measurement).await?;
        }
        _ => panic!("Unsupported VDAF: {:?}", vdaf_instance),
    }
    Ok(())
}

fn make_filter() -> anyhow::Result<impl Filter<Extract = (Response,)> + Clone> {
    let http_client = janus_client::default_http_client()?;

    let ready_filter = warp::path!("ready").map(|| {
        warp::reply::with_status(warp::reply::json(&serde_json::json!({})), StatusCode::OK)
            .into_response()
    });
    let upload_filter =
        warp::path!("upload")
            .and(warp::body::json())
            .then(move |request: UploadRequest| {
                let http_client = http_client.clone();
                async move {
                    let response = match handle_upload(&http_client, request).await {
                        Ok(()) => UploadResponse {
                            status: SUCCESS,
                            error: None,
                        },
                        Err(e) => UploadResponse {
                            status: ERROR,
                            error: Some(format!("{:?}", e)),
                        },
                    };
                    warp::reply::with_status(warp::reply::json(&response), StatusCode::OK)
                        .into_response()
                }
            });

    Ok(warp::path!("internal" / "test" / ..)
        .and(warp::post())
        .and(ready_filter.or(upload_filter).unify()))
}

fn app() -> clap::Command {
    Command::new("Janus interoperation test client").arg(
        Arg::new("port")
            .long("port")
            .short('p')
            .default_value("8080")
            .value_parser(value_parser!(u16))
            .help("Port number to listen on."),
    )
}

const URL_SAFE_NO_PAD: FastPortable = FastPortable::from(&URL_SAFE, NO_PAD);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    install_tracing_subscriber()?;
    let matches = app().get_matches();
    let port = matches
        .try_get_one::<u16>("port")?
        .ok_or_else(|| anyhow!("port argument missing"))?;
    let filter = make_filter()?;
    let server = warp::serve(filter);
    server
        .bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, *port)))
        .await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::app;

    #[test]
    fn verify_clap_app() {
        app().debug_assert();
    }
}
