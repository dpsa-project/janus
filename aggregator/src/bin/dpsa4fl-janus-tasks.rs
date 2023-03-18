use std::{
    net::SocketAddr,
    time::{Instant, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Error, Result};
use base64::{engine::general_purpose, Engine};
// use base64::URL_SAFE_NO_PAD;
use http::{HeaderMap, StatusCode};
use janus_aggregator::{
    binary_utils::{janus_main, setup_signal_handler, BinaryOptions, CommonBinaryOptions},
    config::{BinaryConfig, CommonConfig},
    datastore::{self, Datastore},
    dpsa4fl::{
        core::{
            CreateTrainingSessionRequest, CreateTrainingSessionResponse, HpkeConfigRegistry,
            StartRoundRequest, StartRoundResponse, TrainingSessionId,
        },
        janus_tasks_client::{Fx, TIME_PRECISION},
    },
    task::{QueryType, Task},
    SecretBytes,
};
use janus_core::{
    hpke::HpkeKeypair,
    task::{AuthenticationToken, VdafInstance},
    time::{Clock, RealClock},
};
use janus_messages::{Duration, HpkeConfig, Role, TaskId, Time};
use opentelemetry::metrics::{Histogram, Meter, Unit};
use prio::codec::Decode;
use rand::random;
use serde_json::json;

use clap::Parser;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::{collections::HashMap, convert::Infallible, future::Future};
use tokio::sync::Mutex;
use tracing::warn;
use url::Url;
use warp::{cors::Cors, filters::BoxedFilter, reply::Response, trace, Filter, Rejection, Reply};

//////////////////////////////////////////////////
// main:

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    const CLIENT_USER_AGENT: &str = concat!(
        env!("CARGO_PKG_NAME"),
        "/",
        env!("CARGO_PKG_VERSION"),
        "/dpsafl-janus-tasks"
    );

    janus_main::<_, Options, Config, _, _>(RealClock::default(), |ctx| async move {
        let _meter = opentelemetry::global::meter("collect_job_driver");
        // let datastore = Arc::new(ctx.datastore);

        let shutdown_signal =
            setup_signal_handler().context("failed to register SIGTERM signal handler")?;

        let (_bound_address, server) = taskprovision_server(
            Arc::new(ctx.datastore),
            ctx.clock,
            ctx.config.listen_address,
            HeaderMap::new(),
            // ctx.config
            //     .response_header_map()
            //     .context("failed to parse response headers")?,
            shutdown_signal,
        )
        .context("failed to create aggregator server")?;
        // info!(?bound_address, "Running aggregator");
        println!("Running taskprovision server");

        server.await;

        println!("taskprovision server stopped");
        Ok(())

        // let collect_job_driver = Arc::new(CollectJobDriver::new(
        //     reqwest::Client::builder()
        //         .user_agent(CLIENT_USER_AGENT)
        //         .build()
        //         .context("couldn't create HTTP client")?,
        //     &meter,
        // ));
        // let lease_duration =
        //     Duration::from_seconds(ctx.config.job_driver_config.worker_lease_duration_secs);
        // let shutdown_signal =
        //     setup_signal_handler().context("failed to register SIGTERM signal handler")?;

        // // Start running.
        // let job_driver = Arc::new(JobDriver::new(
        //     ctx.clock,
        //     TokioRuntime,
        //     meter,
        //     Duration::from_seconds(ctx.config.job_driver_config.min_job_discovery_delay_secs),
        //     Duration::from_seconds(ctx.config.job_driver_config.max_job_discovery_delay_secs),
        //     ctx.config.job_driver_config.max_concurrent_job_workers,
        //     Duration::from_seconds(
        //         ctx.config
        //             .job_driver_config
        //             .worker_lease_clock_skew_allowance_secs,
        //     ),
        //     collect_job_driver
        //         .make_incomplete_job_acquirer_callback(Arc::clone(&datastore), lease_duration),
        //     collect_job_driver.make_job_stepper_callback(
        //         Arc::clone(&datastore),
        //         ctx.config.job_driver_config.maximum_attempts_before_failure,
        //     ),
        // ));
        // select! {
        //     _ = job_driver.run() => {}
        //     _ = shutdown_signal => {}
        // };
        // Ok(())
    })
    .await
}

/// Construct a DAP aggregator server, listening on the provided [`SocketAddr`].
/// If the `SocketAddr`'s `port` is 0, an ephemeral port is used. Returns a
/// `SocketAddr` representing the address and port the server are listening on
/// and a future that can be `await`ed to begin serving requests.
pub fn taskprovision_server<C: Clock>(
    datastore: Arc<Datastore<C>>,
    clock: C,
    listen_address: SocketAddr,
    response_headers: HeaderMap,
    shutdown_signal: impl Future<Output = ()> + Send + 'static,
) -> Result<(SocketAddr, impl Future<Output = ()> + 'static), Error> {
    let filter = taskprovision_filter(datastore, clock)?;
    let wrapped_filter = filter.with(warp::filters::reply::headers(response_headers));
    let server = warp::serve(wrapped_filter);
    Ok(server.bind_with_graceful_shutdown(listen_address, shutdown_signal))
}

pub fn taskprovision_filter<C: Clock>(
    datastore: Arc<Datastore<C>>,
    clock: C,
) -> Result<BoxedFilter<(impl Reply,)>, Error> {
    let meter = opentelemetry::global::meter("janus_aggregator");
    let response_time_histogram = meter
        .f64_histogram("janus_aggregator_response_time")
        .with_description("Elapsed time handling incoming requests, by endpoint & status.")
        .with_unit(Unit::new("seconds"))
        .init();

    let aggregator = Arc::new(TaskProvisioner::new(datastore, clock, meter));

    //-------------------------------------------------------
    // create new training session
    let create_session_routing = warp::path("create_session");
    let create_session_responding = warp::post()
        .and(with_cloned_value(Arc::clone(&aggregator)))
        // .and(warp::query::<HashMap<String, String>>())
        .and(warp::body::json())
        .then(
            |aggregator: Arc<TaskProvisioner<C>>, request: CreateTrainingSessionRequest<Fx>| async move {
                let result = aggregator.handle_create_session(request).await;
                match result
                {
                    Ok(training_session_id) =>
                    {
                        let response = CreateTrainingSessionResponse { training_session_id };
                        let response = warp::reply::with_status(warp::reply::json(&response), StatusCode::OK).into_response();
                        Ok(response)
                    },
                    Err(err) =>
                    {
                        let response = warp::reply::with_status(warp::reply::json(&err.to_string()), StatusCode::BAD_REQUEST).into_response();
                        Ok(response)
                    },
                }
            },
        );
    let create_session_endpoint = compose_common_wrappers(
        create_session_routing,
        create_session_responding,
        warp::cors()
            .allow_any_origin()
            .allow_method("POST")
            .max_age(CORS_PREFLIGHT_CACHE_AGE)
            .build(),
        response_time_histogram.clone(),
        "create_session",
    );

    //-------------------------------------------------------
    // start a training round
    let start_round_routing = warp::path("start_round");
    let start_round_responding = warp::post()
        .and(with_cloned_value(Arc::clone(&aggregator)))
        // .and(warp::query::<HashMap<String, String>>())
        .and(warp::body::json())
        .then(
            |aggregator: Arc<TaskProvisioner<C>>, request: StartRoundRequest| async move {
                let result = aggregator.handle_start_round(request).await;
                match result {
                    Ok(()) => {
                        let response = StartRoundResponse {};
                        let response =
                            warp::reply::with_status(warp::reply::json(&response), StatusCode::OK)
                                .into_response();
                        Ok(response)
                    }
                    Err(err) => {
                        let response = warp::reply::with_status(
                            warp::reply::json(&err.to_string()),
                            StatusCode::BAD_REQUEST,
                        )
                        .into_response();
                        Ok(response)
                    }
                }
            },
        );
    let start_round_endpoint = compose_common_wrappers(
        start_round_routing,
        start_round_responding,
        warp::cors()
            .allow_any_origin()
            .allow_method("POST")
            .max_age(CORS_PREFLIGHT_CACHE_AGE)
            .build(),
        response_time_histogram.clone(),
        "start_round",
    );

    Ok(start_round_endpoint
        .or(create_session_endpoint)
        // .or(upload_endpoint)
        // .or(aggregate_endpoint)
        // .or(collect_endpoint)
        // .or(collect_jobs_get_endpoint)
        // .or(collect_jobs_delete_endpoint)
        // .or(aggregate_share_endpoint)
        .boxed())
}

//////////////////////////////////////////////////
// options:

#[derive(Debug, Parser)]
#[clap(
    name = "janus-dpsa4fl-janus-tasks",
    about = "Janus task provision for dpsa4fl testing environments",
    rename_all = "kebab-case",
    version = env!("CARGO_PKG_VERSION"),
)]
struct Options {
    #[clap(flatten)]
    common: CommonBinaryOptions,
}

impl BinaryOptions for Options {
    fn common_options(&self) -> &CommonBinaryOptions {
        &self.common
    }
}

//////////////////////////////////////////////////
// config:

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Config {
    #[serde(flatten)]
    common_config: CommonConfig,
    // #[serde(flatten)]
    // job_driver_config: JobDriverConfig,
    /// Address on which this server should listen for connections and serve its
    /// API endpoints.
    // TODO(#232): options for terminating TLS, unless that gets handled in a load balancer?
    listen_address: SocketAddr,
}

impl BinaryConfig for Config {
    fn common_config(&self) -> &CommonConfig {
        &self.common_config
    }

    fn common_config_mut(&mut self) -> &mut CommonConfig {
        &mut self.common_config
    }
}

//////////////////////////////////////////////////
// self:

struct TrainingSession {
    // endpoints
    leader_endpoint: Url,
    helper_endpoint: Url,

    //
    role: Role,
    num_gradient_entries: usize,

    // needs to be the same for both aggregators (section 4.2 of ppm-draft)
    verify_key: SecretBytes,

    collector_hpke_config: HpkeConfig,

    // auth tokens
    collector_auth_token: AuthenticationToken,
    leader_auth_token: AuthenticationToken,

    // my hpke config & key
    hpke_config_and_key: HpkeKeypair,

    // noise param
    noise_parameter: Fx,
}

pub struct TaskProvisioner<C: Clock> {
    /// Datastore used for durable storage.
    datastore: Arc<Datastore<C>>,
    /// Clock used to sample time.
    clock: C,
    // Cache of task aggregators.
    // task_aggregators: Mutex<HashMap<TaskId, Arc<TaskAggregator>>>,
    /// Currently active training runs.
    training_sessions: Mutex<HashMap<TrainingSessionId, Arc<TrainingSession>>>,

    /// hpke config registry
    keyring: Mutex<HpkeConfigRegistry>,
}

impl<C: Clock> TaskProvisioner<C> {
    fn new(datastore: Arc<Datastore<C>>, clock: C, _meter: Meter) -> Self {
        // let upload_decrypt_failure_counter = meter
        //     .u64_counter("janus_upload_decrypt_failures")
        //     .with_description("Number of decryption failures in the /upload endpoint.")
        //     .init();
        // upload_decrypt_failure_counter.add(&Context::current(), 0, &[]);

        // let aggregate_step_failure_counter = aggregate_step_failure_counter(&meter);

        Self {
            datastore,
            clock,
            training_sessions: Mutex::new(HashMap::new()),
            keyring: Mutex::new(HpkeConfigRegistry::new()),
            // task_aggregators: Mutex::new(HashMap::new()),
            // upload_decrypt_failure_counter,
            // aggregate_step_failure_counter,
        }
    }

    async fn handle_start_round(&self, request: StartRoundRequest) -> Result<(), Error> {
        //---------------------- decode parameters --------------------------
        // session id
        // let training_session_id = training_session_id.ok_or(anyhow!("training_session_id parameter not given."))?;
        let training_session_id = request.training_session_id; // TrainingSessionId::get_decoded(&request.training_session_id)?;

        // get training session with this id
        let training_sessions_lock = self.training_sessions.lock().await;
        let training_session = training_sessions_lock
            .get(&training_session_id)
            .ok_or(anyhow!(
                "There is no training session with id {}",
                &training_session_id
            ))?;

        // task id
        // let task_id_base64 = task_id_base64.ok_or(anyhow!("task_id parameter not given"))?;

        let task_id_bytes = general_purpose::URL_SAFE_NO_PAD.decode(request.task_id_encoded)?;
        // base64::decode_config(request.task_id_encoded, base64::URL_SAFE_NO_PAD)?;
        let task_id = TaskId::get_decoded(&task_id_bytes)?;

        // -------------------- create new task -----------------------------
        let deadline = UNIX_EPOCH.elapsed()?.as_secs() + 10 * 60;

        let collector_auth_tokens = if training_session.role == Role::Leader {
            vec![training_session.collector_auth_token.clone()]
        } else {
            Vec::new()
        };

        let task = Task::new(
            task_id,
            vec![
                training_session.leader_endpoint.clone(),
                training_session.helper_endpoint.clone(),
            ],
            QueryType::TimeInterval,
            // QueryType::FixedSize { max_batch_size: u64::MAX },
            VdafInstance::Prio3Aes128FixedPoint32BitBoundedL2VecSum {
                length: training_session.num_gradient_entries,
                noise_param: training_session.noise_parameter,
            },
            training_session.role,
            vec![training_session.verify_key.clone()],
            10,                                       // max_batch_query_count
            Time::from_seconds_since_epoch(deadline), // task_expiration
            None,                                     // report_expiry_age
            2,                                        // min_batch_size
            Duration::from_seconds(TIME_PRECISION),   // time_precision
            Duration::from_seconds(1000),             // tolerable_clock_skew,
            training_session.collector_hpke_config.clone(),
            vec![training_session.leader_auth_token.clone()], // leader auth tokens
            collector_auth_tokens,                            // collector auth tokens
            [training_session.hpke_config_and_key.clone()],
        )?;

        println!("provisioning task now with id {}", task_id);
        provision_tasks(&self.datastore, vec![task]).await?;
        Ok(())
    }

    async fn handle_create_session(
        &self,
        request: CreateTrainingSessionRequest<Fx>,
    ) -> Result<TrainingSessionId> {
        // decode fields
        let CreateTrainingSessionRequest {
            training_session_id,
            leader_endpoint,
            helper_endpoint,
            role,
            num_gradient_entries,
            verify_key_encoded,
            collector_hpke_config,
            collector_auth_token_encoded,
            leader_auth_token_encoded,
            noise_parameter,
        } = request;

        // prepare id
        // (take requested id if exists, else generate new one)
        let training_session_id = if let Some(id) = training_session_id {
            if self.training_sessions.lock().await.contains_key(&id) {
                return Err(anyhow!(
                    "There already exists a training session with id {id}."
                ));
            }
            id
        } else {
            let id: u16 = random();
            id.into()
        };

        let collector_auth_token =
            AuthenticationToken::from(collector_auth_token_encoded.into_bytes());
        let leader_auth_token = AuthenticationToken::from(leader_auth_token_encoded.into_bytes());
        let verify_key = SecretBytes::new(
            general_purpose::URL_SAFE_NO_PAD
                .decode(verify_key_encoded)
                // base64::decode_config(verify_key_encoded, URL_SAFE_NO_PAD)
                .context("invalid base64url content in \"verifyKey\"")?,
        );

        // generate new hpke config and private key
        let hpke_config_and_key = self.keyring.lock().await.get_random_keypair();

        // create session
        let training_session = TrainingSession {
            leader_endpoint,
            helper_endpoint,
            role,
            num_gradient_entries,
            verify_key,
            collector_hpke_config,
            collector_auth_token,
            leader_auth_token,
            hpke_config_and_key,
            noise_parameter,
        };

        // insert into list
        println!("creating training session with id {}", training_session_id);
        let mut sessions = self.training_sessions.lock().await;
        sessions.insert(training_session_id, Arc::new(training_session));

        // respond with id
        Ok(training_session_id)
    }
}

//////////////////////////////////////////////////
// code:

async fn provision_tasks<C: Clock>(datastore: &Datastore<C>, tasks: Vec<Task>) -> Result<()> {
    // Write all tasks requested.
    let tasks = Arc::new(tasks);
    // info!(task_count = %tasks.len(), "Writing tasks");
    datastore
        .run_tx(|tx| {
            let tasks = Arc::clone(&tasks);
            Box::pin(async move {
                for task in tasks.iter() {
                    // We attempt to delete the task, but ignore "task not found" errors since
                    // the task not existing is an OK outcome too.
                    match tx.delete_task(task.id()).await {
                        Ok(_) | Err(datastore::Error::MutationTargetNotFound) => (),
                        err => err?,
                    }

                    tx.put_task(task).await?;
                }
                Ok(())
            })
        })
        .await
        .context("couldn't write tasks")
}

//////////////////////////////////////////////////////
// helpers:

/// The media type for problem details formatted as a JSON document, per RFC 7807.
static PROBLEM_DETAILS_JSON_MEDIA_TYPE: &str = "application/problem+json";

/// The number of seconds we send in the Access-Control-Max-Age header. This determines for how
/// long clients will cache the results of CORS preflight requests. Of popular browsers, Mozilla
/// Firefox has the highest Max-Age cap, at 24 hours, so we use that. Our CORS preflight handlers
/// are tightly scoped to relevant endpoints, and our CORS settings are unlikely to change.
/// See: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Access-Control-Max-Age
const CORS_PREFLIGHT_CACHE_AGE: u32 = 24 * 60 * 60;

/// Injects a clone of the provided value into the warp filter, making it
/// available to the filter's map() or and_then() handler.
fn with_cloned_value<T>(value: T) -> impl Filter<Extract = (T,), Error = Infallible> + Clone
where
    T: Clone + Sync + Send,
{
    warp::any().map(move || value.clone())
}

/// Convenience function to perform common composition of Warp filters for a single endpoint. A
/// combined filter is returned, with a CORS handler, instrumented to measure both request
/// processing time and successes or failures for metrics, and with per-route named tracing spans.
///
/// `route_filter` should be a filter that determines whether the incoming request matches a
/// given route or not. It should inspect the ambient request, and either extract the empty tuple
/// or reject.
///
/// `response_filter` should be a filter that performs all response handling for this route, after
/// the above `route_filter` has already determined the request is applicable to this route. It
/// should only reject in response to malformed requests, not requests that may yet be served by a
/// different route. This will ensure that a single request doesn't pass through multiple wrapping
/// filters, skewing the low end of unrelated requests' timing histograms. The filter's return type
/// should be `Result<impl Reply, Error>`, and errors will be transformed into responses with
/// problem details documents as appropriate.
///
/// `cors` is a configuration object describing CORS policies for this route.
///
/// `response_time_histogram` is a `Histogram` that will be used to record request handling timings.
///
/// `name` is a unique name for this route. This will be used as a metrics label, and will be added
/// to the tracing span's values as its message.
fn compose_common_wrappers<F1, F2, T>(
    route_filter: F1,
    response_filter: F2,
    cors: Cors,
    response_time_histogram: Histogram<f64>,
    name: &'static str,
) -> BoxedFilter<(impl Reply,)>
where
    F1: Filter<Extract = (), Error = Rejection> + Send + Sync + 'static,
    F2: Filter<Extract = (Result<T, Error>,), Error = Rejection> + Clone + Send + Sync + 'static,
    T: Reply + 'static,
{
    route_filter
        .and(
            response_filter
                .with(warp::wrap_fn(error_handler(response_time_histogram, name)))
                .with(cors)
                .with(trace::named(name)),
        )
        .boxed()
}

/// Produces a closure that will transform applicable errors into a problem details JSON object
/// (see RFC 7807) and update a metrics counter tracking the error status of the result as well as
/// timing information. The returned closure is meant to be used in a warp `with` filter.
fn error_handler<F, T>(
    response_time_histogram: Histogram<f64>,
    name: &'static str,
) -> impl Fn(F) -> BoxedFilter<(Response,)>
where
    F: Filter<Extract = (Result<T, Error>,), Error = Rejection> + Clone + Send + Sync + 'static,
    T: Reply,
{
    move |filter| {
        let _response_time_histogram = response_time_histogram.clone();
        warp::any()
            .map(Instant::now)
            .and(filter)
            .map(move |_start: Instant, result: Result<T, Error>| {
                let error_code = if let Err(error) = &result {
                    warn!(?error, endpoint = name, "Error handling endpoint");
                    error.to_string()
                } else {
                    "".to_owned()
                };

                // response_time_histogram.record(
                //     &Context::current(),
                //     start.elapsed().as_secs_f64(),
                //     &[
                //         KeyValue::new("endpoint", name),
                //         KeyValue::new("error_code", error_code),
                //     ],
                // );

                match result {
                    Ok(reply) => reply.into_response(),
                    Err(_e) => build_problem_details_response(error_code, None),
                }
                //     Err(Error::InvalidConfiguration(_)) => {
                //         StatusCode::INTERNAL_SERVER_ERROR.into_response()
                //     }
                //     Err(Error::MessageDecode(_)) => StatusCode::BAD_REQUEST.into_response(),
                //     Err(Error::ReportTooLate(task_id, _, _)) => {
                //         build_problem_details_response(DapProblemType::ReportTooLate, Some(task_id))
                //     }
                //     Err(Error::UnrecognizedMessage(task_id, _)) => {
                //         build_problem_details_response(DapProblemType::UnrecognizedMessage, task_id)
                //     }
                //     Err(Error::UnrecognizedTask(task_id)) => {
                //         // TODO(#237): ensure that a helper returns HTTP 404 or 403 when this happens.
                //         build_problem_details_response(
                //             DapProblemType::UnrecognizedTask,
                //             Some(task_id),
                //         )
                //     }
                //     Err(Error::MissingTaskId) => {
                //         build_problem_details_response(DapProblemType::MissingTaskId, None)
                //     }
                //     Err(Error::UnrecognizedAggregationJob(task_id, _)) => {
                //         build_problem_details_response(
                //             DapProblemType::UnrecognizedAggregationJob,
                //             Some(task_id),
                //         )
                //     }
                //     Err(Error::DeletedCollectJob(_)) => StatusCode::NO_CONTENT.into_response(),
                //     Err(Error::UnrecognizedCollectJob(_)) => StatusCode::NOT_FOUND.into_response(),
                //     Err(Error::OutdatedHpkeConfig(task_id, _)) => build_problem_details_response(
                //         DapProblemType::OutdatedConfig,
                //         Some(task_id),
                //     ),
                //     Err(Error::ReportTooEarly(task_id, _, _)) => build_problem_details_response(
                //         DapProblemType::ReportTooEarly,
                //         Some(task_id),
                //     ),
                //     Err(Error::UnauthorizedRequest(task_id)) => build_problem_details_response(
                //         DapProblemType::UnauthorizedRequest,
                //         Some(task_id),
                //     ),
                //     Err(Error::InvalidBatchSize(task_id, _)) => build_problem_details_response(
                //         DapProblemType::InvalidBatchSize,
                //         Some(task_id),
                //     ),
                //     Err(Error::BatchInvalid(task_id, _)) => {
                //         build_problem_details_response(DapProblemType::BatchInvalid, Some(task_id))
                //     }
                //     Err(Error::BatchOverlap(task_id, _)) => {
                //         build_problem_details_response(DapProblemType::BatchOverlap, Some(task_id))
                //     }
                //     Err(Error::BatchMismatch { task_id, .. }) => {
                //         build_problem_details_response(DapProblemType::BatchMismatch, Some(task_id))
                //     }
                //     Err(Error::BatchQueriedTooManyTimes(task_id, ..)) => {
                //         build_problem_details_response(
                //             DapProblemType::BatchQueriedTooManyTimes,
                //             Some(task_id),
                //         )
                //     }
                //     Err(Error::Hpke(_)) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                //     Err(Error::Datastore(_)) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                //     Err(Error::Vdaf(_)) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                //     Err(Error::Internal(_)) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                //     Err(Error::Url(_)) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                //     Err(Error::Message(_)) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                //     Err(Error::HttpClient(_)) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                //     Err(Error::Http { .. }) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                //     Err(Error::TaskParameters(_)) => {
                //         StatusCode::INTERNAL_SERVER_ERROR.into_response()
                //     }
                // }
            })
            .boxed()
    }
}

/// Construct an error response in accordance with §3.2.
// TODO(https://github.com/ietf-wg-ppm/draft-ietf-ppm-dap/issues/209): The handling of the instance,
// title, detail, and taskid fields are subject to change.
fn build_problem_details_response(error_type: String, task_id: Option<TaskId>) -> Response {
    // let status = error_type.http_status();
    let status = StatusCode::SEE_OTHER;

    warp::reply::with_status(
        warp::reply::with_header(
            warp::reply::json(&json!({
                // "type": error_type.type_uri(),
                // "title": error_type.description(),
                // "status": status.as_u16(),
                "detail": error_type,
                // The base URI is either "[leader]/upload", "[aggregator]/aggregate",
                // "[helper]/aggregate_share", or "[leader]/collect". Relative URLs are allowed in
                // the instance member, thus ".." will always refer to the aggregator's endpoint,
                // as required by §3.2.
                "instance": "..",
                "taskid": task_id.map(|tid| format!("{}", tid)),
            })),
            http::header::CONTENT_TYPE,
            PROBLEM_DETAILS_JSON_MEDIA_TYPE,
        ),
        status,
    )
    .into_response()
}
