use base64::{
    alphabet::URL_SAFE,
    engine::fast_portable::{FastPortable, NO_PAD},
};
use clap::{
    builder::{NonEmptyStringValueParser, StringValueParser, TypedValueParser},
    error::ErrorKind,
    ArgAction, CommandFactory, FromArgMatches, Parser, ValueEnum,
};
use derivative::Derivative;
use janus_collector::{default_http_client, Collector, CollectorParameters};
use janus_core::{hpke::HpkePrivateKey, task::AuthenticationToken};
use janus_messages::{
    query_type::{FixedSize, QueryType, TimeInterval},
    BatchId, Duration, FixedSizeQuery, HpkeConfig, Interval, PartialBatchSelector, Query, TaskId,
    Time,
};
use prio::{
    codec::Decode,
    vdaf::{self, prio3::Prio3},
};
use std::fmt::Debug;
use tracing_log::LogTracer;
use tracing_subscriber::{prelude::*, EnvFilter, Registry};
use url::Url;

/// Enum to propagate errors through this program. Clap errors are handled separately from all
/// others because [`clap::Error::exit`] takes care of its own error formatting, command-line help,
/// and exit code.
#[derive(Debug)]
enum Error {
    Anyhow(anyhow::Error),
    Clap(clap::Error),
}

impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        Error::Anyhow(error)
    }
}

impl From<clap::Error> for Error {
    fn from(error: clap::Error) -> Self {
        Error::Clap(error)
    }
}

// Parsers for command-line arguments:

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
#[clap(rename_all = "lower")]
enum VdafType {
    /// Prio3Aes128Count
    Count,
    /// Prio3Aes128CountVec
    CountVec,
    /// Prio3Aes128Sum
    Sum,
    /// Prio3Aes128Histogram
    Histogram,
    /// Prio3FixedPoint16BitBoundedL2VecSum
    FixedPoint16BitBoundedL2VecSum,
    /// Prio3FixedPoint32BitBoundedL2VecSum
    FixedPoint32BitBoundedL2VecSum,
    /// Prio3FixedPoint64BitBoundedL2VecSum
    FixedPoint64BitBoundedL2VecSum,
}

#[derive(Clone)]
struct TaskIdValueParser {
    inner: NonEmptyStringValueParser,
}

impl TaskIdValueParser {
    fn new() -> TaskIdValueParser {
        TaskIdValueParser {
            inner: NonEmptyStringValueParser::new(),
        }
    }
}

impl TypedValueParser for TaskIdValueParser {
    type Value = TaskId;

    fn parse_ref(
        &self,
        cmd: &clap::Command,
        arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, clap::Error> {
        let input = self.inner.parse_ref(cmd, arg, value)?;
        let task_id_bytes: [u8; TaskId::LEN] = base64::decode_engine(input, &URL_SAFE_NO_PAD)
            .map_err(|err| clap::Error::raw(ErrorKind::ValueValidation, err))?
            .try_into()
            .map_err(|_| {
                clap::Error::raw(ErrorKind::ValueValidation, "task ID length incorrect")
            })?;
        Ok(TaskId::from(task_id_bytes))
    }
}

#[derive(Clone)]
struct BatchIdValueParser {
    inner: NonEmptyStringValueParser,
}

impl BatchIdValueParser {
    fn new() -> BatchIdValueParser {
        BatchIdValueParser {
            inner: NonEmptyStringValueParser::new(),
        }
    }
}

impl TypedValueParser for BatchIdValueParser {
    type Value = BatchId;

    fn parse_ref(
        &self,
        cmd: &clap::Command,
        arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, clap::Error> {
        let input = self.inner.parse_ref(cmd, arg, value)?;
        let batch_id_bytes: [u8; BatchId::LEN] = base64::decode_engine(input, &URL_SAFE_NO_PAD)
            .map_err(|err| clap::Error::raw(ErrorKind::ValueValidation, err))?
            .try_into()
            .map_err(|_| {
                clap::Error::raw(ErrorKind::ValueValidation, "batch ID length incorrect")
            })?;
        Ok(BatchId::from(batch_id_bytes))
    }
}

fn parse_authentication_token(value: String) -> AuthenticationToken {
    AuthenticationToken::from(value.into_bytes())
}

#[derive(Clone)]
struct HpkeConfigValueParser {
    inner: NonEmptyStringValueParser,
}

impl HpkeConfigValueParser {
    fn new() -> HpkeConfigValueParser {
        HpkeConfigValueParser {
            inner: NonEmptyStringValueParser::new(),
        }
    }
}

impl TypedValueParser for HpkeConfigValueParser {
    type Value = HpkeConfig;

    fn parse_ref(
        &self,
        cmd: &clap::Command,
        arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, clap::Error> {
        let input = self.inner.parse_ref(cmd, arg, value)?;
        let bytes = base64::decode_engine(input, &URL_SAFE_NO_PAD)
            .map_err(|err| clap::Error::raw(ErrorKind::ValueValidation, err))?;
        HpkeConfig::get_decoded(&bytes)
            .map_err(|err| clap::Error::raw(ErrorKind::ValueValidation, err))
    }
}

#[derive(Clone)]
struct PrivateKeyValueParser {
    inner: NonEmptyStringValueParser,
}

impl PrivateKeyValueParser {
    fn new() -> PrivateKeyValueParser {
        PrivateKeyValueParser {
            inner: NonEmptyStringValueParser::new(),
        }
    }
}

impl TypedValueParser for PrivateKeyValueParser {
    type Value = HpkePrivateKey;

    fn parse_ref(
        &self,
        cmd: &clap::Command,
        arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, clap::Error> {
        let input = self.inner.parse_ref(cmd, arg, value)?;
        let bytes = base64::decode_engine(input, &URL_SAFE_NO_PAD)
            .map_err(|err| clap::Error::raw(ErrorKind::ValueValidation, err))?;
        Ok(HpkePrivateKey::new(bytes))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Buckets(Vec<u64>);

#[derive(Clone)]
struct BucketsValueParser {
    inner: NonEmptyStringValueParser,
}

impl BucketsValueParser {
    fn new() -> BucketsValueParser {
        BucketsValueParser {
            inner: NonEmptyStringValueParser::new(),
        }
    }
}

impl TypedValueParser for BucketsValueParser {
    type Value = Buckets;

    fn parse_ref(
        &self,
        cmd: &clap::Command,
        arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, clap::Error> {
        let input = self.inner.parse_ref(cmd, arg, value)?;
        input
            .split(',')
            .map(|chunk| chunk.trim().parse())
            .collect::<Result<Vec<_>, _>>()
            .map(Buckets)
            .map_err(|err| clap::Error::raw(ErrorKind::ValueValidation, err))
    }
}

#[derive(Derivative, Parser, PartialEq, Eq)]
#[derivative(Debug)]
#[clap(
    name = "collect",
    version,
    about = "Command-line DAP-PPM collector from ISRG's Divvi Up",
    long_about = None,
)]
struct Options {
    /// DAP task identifier, encoded with base64url
    #[clap(
        long,
        value_parser = TaskIdValueParser::new(),
        help_heading = "DAP Task Parameters",
        display_order = 0
    )]
    task_id: TaskId,
    /// The leader aggregator's endpoint URL
    #[clap(long, help_heading = "DAP Task Parameters", display_order = 1)]
    leader: Url,
    /// Authentication token for the DAP-Auth-Token HTTP header
    #[clap(
        long,
        value_parser = StringValueParser::new().map(parse_authentication_token),
        env,
        help_heading = "DAP Task Parameters",
        display_order = 2
    )]
    #[derivative(Debug = "ignore")]
    auth_token: AuthenticationToken,
    /// DAP message for the collector's HPKE configuration, encoded with base64url
    #[clap(
        long,
        value_parser = HpkeConfigValueParser::new(),
        help_heading = "DAP Task Parameters",
        display_order = 3
    )]
    hpke_config: HpkeConfig,
    /// The collector's HPKE private key, encoded with base64url
    #[clap(
        long,
        value_parser = PrivateKeyValueParser::new(),
        env,
        help_heading = "DAP Task Parameters",
        display_order = 4
    )]
    #[derivative(Debug = "ignore")]
    hpke_private_key: HpkePrivateKey,

    /// VDAF algorithm
    #[clap(
        long,
        value_enum,
        help_heading = "VDAF Algorithm and Parameters",
        display_order = 0
    )]
    vdaf: VdafType,
    /// Number of vector elements, for use with --vdaf=countvec
    #[clap(long, help_heading = "VDAF Algorithm and Parameters")]
    length: Option<usize>,
    /// Bit length of measurements, for use with --vdaf=sum
    #[clap(long, help_heading = "VDAF Algorithm and Parameters")]
    bits: Option<u32>,
    /// Comma-separated list of bucket boundaries, for use with --vdaf=histogram
    #[clap(
        long,
        required = false,
        num_args = 1,
        action = ArgAction::Set,
        value_parser = BucketsValueParser::new(),
        help_heading = "VDAF Algorithm and Parameters"
    )]
    buckets: Option<Buckets>,

    /// Start of the collection batch interval, as the number of seconds since the Unix epoch
    #[clap(
        long,
        requires = "batch_interval_duration",
        help_heading = "Collect Request Parameters (Time Interval)"
    )]
    batch_interval_start: Option<u64>,
    /// Duration of the collection batch interval, in seconds
    #[clap(
        long,
        requires = "batch_interval_start",
        help_heading = "Collect Request Parameters (Time Interval)"
    )]
    batch_interval_duration: Option<u64>,

    /// Batch identifier, encoded with base64url
    #[clap(
        long,
        value_parser = BatchIdValueParser::new(),
        conflicts_with_all = ["batch_interval_start", "batch_interval_duration", "current_batch"],
        help_heading = "Collect Request Parameters (Fixed Size)",
    )]
    batch_id: Option<BatchId>,
    /// Have the aggregator select a batch that has not yet been collected
    #[clap(
        long,
        action = ArgAction::SetTrue,
        conflicts_with_all = ["batch_interval_start", "batch_interval_duration", "batch_id"],
        help_heading = "Collect Request Parameters (Fixed Size)",
    )]
    current_batch: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    install_tracing_subscriber()?;

    let mut command = Options::command();
    let mut matches = match command.try_get_matches_from_mut(std::env::args_os()) {
        Ok(matches) => matches,
        Err(err) => err.format(&mut command).exit(),
    };
    let options = match Options::from_arg_matches_mut(&mut matches) {
        Ok(options) => options,
        Err(err) => err.format(&mut command).exit(),
    };

    match run(options).await {
        Ok(()) => Ok(()),
        Err(Error::Anyhow(err)) => Err(err),
        Err(Error::Clap(err)) => err.format(&mut command).exit(),
    }
}

// This function is broken out from `main()` for the sake of testing its argument handling.
async fn run(options: Options) -> Result<(), Error> {
    match (
        &options.batch_interval_start,
        &options.batch_interval_duration,
        &options.batch_id,
        options.current_batch,
    ) {
        (Some(batch_interval_start), Some(batch_interval_duration), None, false) => {
            let batch_interval = Interval::new(
                Time::from_seconds_since_epoch(*batch_interval_start),
                Duration::from_seconds(*batch_interval_duration),
            )
            .map_err(|err| Error::Anyhow(err.into()))?;
            run_with_query(options, Query::new_time_interval(batch_interval)).await
        }
        (None, None, Some(batch_id), false) => {
            let batch_id = *batch_id;
            run_with_query(
                options,
                Query::new_fixed_size(FixedSizeQuery::ByBatchId { batch_id }),
            )
            .await
        }
        (None, None, None, true) => {
            run_with_query(options, Query::new_fixed_size(FixedSizeQuery::CurrentBatch)).await
        }
        _ => unreachable!(),
    }
}

async fn run_with_query<Q: QueryType>(options: Options, query: Query<Q>) -> Result<(), Error>
where
    Q: QueryTypeExt,
{
    let parameters = CollectorParameters::new(
        options.task_id,
        options.leader,
        options.auth_token,
        options.hpke_config.clone(),
        options.hpke_private_key.clone(),
    );
    let http_client = default_http_client().map_err(|err| Error::Anyhow(err.into()))?;
    match (options.vdaf, options.length, options.bits, options.buckets) {
        (VdafType::Count, None, None, None) => {
            let vdaf = Prio3::new_aes128_count(2).map_err(|err| Error::Anyhow(err.into()))?;
            run_collection_generic(parameters, vdaf, http_client, query, &())
                .await
                .map_err(|err| Error::Anyhow(err.into()))
        }
        (VdafType::CountVec, Some(length), None, None) => {
            let vdaf =
                Prio3::new_aes128_count_vec(2, length).map_err(|err| Error::Anyhow(err.into()))?;
            run_collection_generic(parameters, vdaf, http_client, query, &())
                .await
                .map_err(|err| Error::Anyhow(err.into()))
        }
        (VdafType::Sum, None, Some(bits), None) => {
            let vdaf = Prio3::new_aes128_sum(2, bits).map_err(|err| Error::Anyhow(err.into()))?;
            run_collection_generic(parameters, vdaf, http_client, query, &())
                .await
                .map_err(|err| Error::Anyhow(err.into()))
        }
        (VdafType::Histogram, None, None, Some(ref buckets)) => {
            let vdaf = Prio3::new_aes128_histogram(2, &buckets.0)
                .map_err(|err| Error::Anyhow(err.into()))?;
            run_collection_generic(parameters, vdaf, http_client, query, &())
                .await
                .map_err(|err| Error::Anyhow(err.into()))
        }
        _ => Err(clap::Error::raw(
            ErrorKind::ArgumentConflict,
            format!(
                "incorrect VDAF parameter arguments were supplied for {}",
                options
                    .vdaf
                    .to_possible_value()
                    .unwrap()
                    .get_help()
                    .unwrap(),
            ),
        )
        .into()),
    }
}

async fn run_collection_generic<V: vdaf::Collector, Q: QueryTypeExt>(
    parameters: CollectorParameters,
    vdaf: V,
    http_client: reqwest::Client,
    query: Query<Q>,
    agg_param: &V::AggregationParam,
) -> Result<(), janus_collector::Error>
where
    for<'a> Vec<u8>: From<&'a V::AggregateShare>,
    V::AggregateResult: Debug,
{
    let collector = Collector::new(parameters, vdaf, http_client);
    let collection = collector.collect(query, agg_param).await?;
    if !Q::IS_PARTIAL_BATCH_SELECTOR_TRIVIAL {
        println!(
            "Batch: {}",
            Q::format_partial_batch_selector(collection.partial_batch_selector())
        );
    }
    println!("Number of reports: {}", collection.report_count());
    println!("Aggregation result: {:?}", collection.aggregate_result());
    Ok(())
}

fn install_tracing_subscriber() -> anyhow::Result<()> {
    let stdout_filter = EnvFilter::from_default_env();
    let layer = tracing_subscriber::fmt::layer()
        .with_level(true)
        .with_target(true)
        .pretty();
    let subscriber = Registry::default().with(stdout_filter.and_then(layer));
    tracing::subscriber::set_global_default(subscriber)?;

    LogTracer::init()?;

    Ok(())
}

const URL_SAFE_NO_PAD: FastPortable = FastPortable::from(&URL_SAFE, NO_PAD);

trait QueryTypeExt: QueryType {
    const IS_PARTIAL_BATCH_SELECTOR_TRIVIAL: bool;

    fn format_partial_batch_selector(partial_batch_selector: &PartialBatchSelector<Self>)
        -> String;
}

impl QueryTypeExt for TimeInterval {
    const IS_PARTIAL_BATCH_SELECTOR_TRIVIAL: bool = true;

    fn format_partial_batch_selector(_: &PartialBatchSelector<Self>) -> String {
        "()".to_string()
    }
}

impl QueryTypeExt for FixedSize {
    const IS_PARTIAL_BATCH_SELECTOR_TRIVIAL: bool = false;

    fn format_partial_batch_selector(
        partial_batch_selector: &PartialBatchSelector<Self>,
    ) -> String {
        base64::encode_engine(partial_batch_selector.batch_id().as_ref(), &URL_SAFE_NO_PAD)
    }
}

#[cfg(test)]
mod tests {
    use crate::{run, Error, Options, VdafType, URL_SAFE_NO_PAD};
    use assert_matches::assert_matches;
    use clap::{error::ErrorKind, CommandFactory, Parser};
    use janus_core::{
        hpke::test_util::generate_test_hpke_config_and_private_key, task::AuthenticationToken,
    };
    use janus_messages::BatchId;
    use prio::codec::Encode;
    use rand::random;
    use reqwest::Url;

    #[test]
    fn verify_app() {
        Options::command().debug_assert();
    }

    #[tokio::test]
    async fn argument_handling() {
        let hpke_keypair = generate_test_hpke_config_and_private_key();
        let encoded_hpke_config =
            base64::encode_engine(hpke_keypair.config().get_encoded(), &URL_SAFE_NO_PAD);
        let encoded_private_key =
            base64::encode_engine(hpke_keypair.private_key().as_ref(), &URL_SAFE_NO_PAD);

        let task_id = random();
        let leader = Url::parse("https://example.com/dap/").unwrap();
        let auth_token = AuthenticationToken::from(b"collector-authentication-token".to_vec());

        let expected = Options {
            task_id,
            leader: leader.clone(),
            auth_token: auth_token.clone(),
            hpke_config: hpke_keypair.config().clone(),
            hpke_private_key: hpke_keypair.private_key().clone(),
            vdaf: VdafType::Count,
            length: None,
            bits: None,
            buckets: None,
            batch_interval_start: Some(1_000_000),
            batch_interval_duration: Some(1_000),
            batch_id: None,
            current_batch: false,
        };
        let task_id_encoded = base64::encode_engine(task_id.get_encoded(), &URL_SAFE_NO_PAD);
        let correct_arguments = [
            "collect",
            &format!("--task-id={task_id_encoded}"),
            "--leader",
            leader.as_str(),
            "--auth-token",
            "collector-authentication-token",
            &format!("--hpke-config={encoded_hpke_config}"),
            &format!("--hpke-private-key={encoded_private_key}"),
            "--vdaf",
            "count",
            "--batch-interval-start",
            "1000000",
            "--batch-interval-duration",
            "1000",
        ];
        match Options::try_parse_from(correct_arguments) {
            Ok(got) => assert_eq!(got, expected),
            Err(e) => panic!("{}\narguments were {:?}", e, correct_arguments),
        }

        assert_eq!(
            Options::try_parse_from(["collect"]).unwrap_err().kind(),
            ErrorKind::MissingRequiredArgument,
        );

        let mut bad_arguments = correct_arguments.clone();
        let bad_argument = format!("--task-id=not valid base64");
        bad_arguments[1] = &bad_argument;
        assert_eq!(
            Options::try_parse_from(bad_arguments).unwrap_err().kind(),
            ErrorKind::ValueValidation,
        );

        let mut bad_arguments = correct_arguments.clone();
        let short_encoded = base64::encode_engine("too short", &URL_SAFE_NO_PAD);
        let bad_argument = format!("--task-id={short_encoded}");
        bad_arguments[1] = &bad_argument;
        assert_eq!(
            Options::try_parse_from(bad_arguments).unwrap_err().kind(),
            ErrorKind::ValueValidation,
        );

        let mut bad_arguments = correct_arguments.clone();
        bad_arguments[3] = "http:bad:url:///dap@";
        assert_eq!(
            Options::try_parse_from(bad_arguments).unwrap_err().kind(),
            ErrorKind::ValueValidation,
        );

        let mut bad_arguments = correct_arguments.clone();
        let bad_argument = format!("--hpke-config=not valid base64");
        bad_arguments[6] = &bad_argument;
        assert_eq!(
            Options::try_parse_from(bad_arguments).unwrap_err().kind(),
            ErrorKind::ValueValidation,
        );

        let mut bad_arguments = correct_arguments.clone();
        let bad_argument = format!("--hpke-private-key=not valid base64");
        bad_arguments[7] = &bad_argument;
        assert_eq!(
            Options::try_parse_from(bad_arguments).unwrap_err().kind(),
            ErrorKind::ValueValidation,
        );

        let base_arguments = Vec::from([
            "collect".to_string(),
            format!("--task-id={task_id_encoded}"),
            "--leader".to_string(),
            leader.to_string(),
            "--auth-token".to_string(),
            "collector-authentication-token".to_string(),
            format!("--hpke-config={encoded_hpke_config}"),
            format!("--hpke-private-key={encoded_private_key}"),
            "--batch-interval-start".to_string(),
            "1000000".to_string(),
            "--batch-interval-duration".to_string(),
            "1000".to_string(),
        ]);

        let mut bad_arguments = base_arguments.clone();
        bad_arguments.extend(["--vdaf=count".to_string(), "--buckets=1,2,3,4".to_string()]);
        let bad_options = Options::try_parse_from(bad_arguments).unwrap();
        assert_matches!(
            run(bad_options).await.unwrap_err(),
            Error::Clap(err) => assert_eq!(err.kind(), ErrorKind::ArgumentConflict)
        );

        let mut bad_arguments = base_arguments.clone();
        bad_arguments.extend(["--vdaf=sum".to_string(), "--buckets=1,2,3,4".to_string()]);
        let bad_options = Options::try_parse_from(bad_arguments).unwrap();
        assert_matches!(
            run(bad_options).await.unwrap_err(),
            Error::Clap(err) => assert_eq!(err.kind(), ErrorKind::ArgumentConflict)
        );

        let mut bad_arguments = base_arguments.clone();
        bad_arguments.extend([
            "--vdaf=countvec".to_string(),
            "--buckets=1,2,3,4".to_string(),
        ]);
        let bad_options = Options::try_parse_from(bad_arguments).unwrap();
        assert_matches!(
            run(bad_options).await.unwrap_err(),
            Error::Clap(err) => assert_eq!(err.kind(), ErrorKind::ArgumentConflict)
        );

        let mut bad_arguments = base_arguments.clone();
        bad_arguments.extend(["--vdaf=countvec".to_string(), "--bits=3".to_string()]);
        let bad_options = Options::try_parse_from(bad_arguments).unwrap();
        assert_matches!(
            run(bad_options).await.unwrap_err(),
            Error::Clap(err) => assert_eq!(err.kind(), ErrorKind::ArgumentConflict)
        );

        let mut bad_arguments = base_arguments.clone();
        bad_arguments.extend([
            "--vdaf=histogram".to_string(),
            "--buckets=1,2,3,4,apple".to_string(),
        ]);
        assert_eq!(
            Options::try_parse_from(bad_arguments).unwrap_err().kind(),
            ErrorKind::ValueValidation
        );

        let mut bad_arguments = base_arguments.clone();
        bad_arguments.extend(["--vdaf=histogram".to_string()]);
        let bad_options = Options::try_parse_from(bad_arguments).unwrap();
        assert_matches!(
            run(bad_options).await.unwrap_err(),
            Error::Clap(err) => assert_eq!(err.kind(), ErrorKind::ArgumentConflict)
        );

        let mut bad_arguments = base_arguments.clone();
        bad_arguments.extend(["--vdaf=sum".to_string()]);
        let bad_options = Options::try_parse_from(bad_arguments).unwrap();
        assert_matches!(
            run(bad_options).await.unwrap_err(),
            Error::Clap(err) => assert_eq!(err.kind(), ErrorKind::ArgumentConflict)
        );

        let mut good_arguments = base_arguments.clone();
        good_arguments.extend(["--vdaf=countvec".to_string(), "--length=10".to_string()]);
        Options::try_parse_from(good_arguments).unwrap();

        let mut good_arguments = base_arguments.clone();
        good_arguments.extend(["--vdaf=sum".to_string(), "--bits=8".to_string()]);
        Options::try_parse_from(good_arguments).unwrap();

        let mut good_arguments = base_arguments.clone();
        good_arguments.extend([
            "--vdaf=histogram".to_string(),
            "--buckets=1,2,3,4".to_string(),
        ]);
        Options::try_parse_from(good_arguments).unwrap();

        // Check parsing arguments for a current batch query.
        let expected = Options {
            task_id,
            leader: leader.clone(),
            auth_token: auth_token.clone(),
            hpke_config: hpke_keypair.config().clone(),
            hpke_private_key: hpke_keypair.private_key().clone(),
            vdaf: VdafType::Count,
            length: None,
            bits: None,
            buckets: None,
            batch_interval_start: None,
            batch_interval_duration: None,
            batch_id: None,
            current_batch: true,
        };
        let correct_arguments = [
            "collect",
            &format!("--task-id={task_id_encoded}"),
            "--leader",
            leader.as_str(),
            "--auth-token",
            "collector-authentication-token",
            &format!("--hpke-config={encoded_hpke_config}"),
            &format!("--hpke-private-key={encoded_private_key}"),
            "--vdaf",
            "count",
            "--current-batch",
        ];
        match Options::try_parse_from(correct_arguments) {
            Ok(got) => assert_eq!(got, expected),
            Err(e) => panic!("{}\narguments were {:?}", e, correct_arguments),
        }

        let batch_id: BatchId = random();
        let batch_id_encoded = base64::encode_engine(batch_id.as_ref(), &URL_SAFE_NO_PAD);
        let expected = Options {
            task_id,
            leader: leader.clone(),
            auth_token,
            hpke_config: hpke_keypair.config().clone(),
            hpke_private_key: hpke_keypair.private_key().clone(),
            vdaf: VdafType::Count,
            length: None,
            bits: None,
            buckets: None,
            batch_interval_start: None,
            batch_interval_duration: None,
            batch_id: Some(batch_id),
            current_batch: false,
        };
        let correct_arguments = [
            "collect",
            &format!("--task-id={task_id_encoded}"),
            "--leader",
            leader.as_str(),
            "--auth-token",
            "collector-authentication-token",
            &format!("--hpke-config={encoded_hpke_config}"),
            &format!("--hpke-private-key={encoded_private_key}"),
            "--vdaf",
            "count",
            &format!("--batch-id={batch_id_encoded}"),
        ];
        match Options::try_parse_from(correct_arguments) {
            Ok(got) => assert_eq!(got, expected),
            Err(e) => panic!("{}\narguments were {:?}", e, correct_arguments),
        }

        // Check that clap enforces all the constraints we need on combinations of query arguments.
        // This allows us to treat a default match branch as `unreachable!()` when unpacking the
        // argument matches.
        let base_arguments = Vec::from([
            "collect".to_string(),
            format!("--task-id={task_id_encoded}"),
            "--leader".to_string(),
            leader.to_string(),
            "--auth-token".to_string(),
            "collector-authentication-token".to_string(),
            format!("--hpke-config={encoded_hpke_config}"),
            format!("--hpke-private-key={encoded_private_key}"),
        ]);
        assert_eq!(
            Options::try_parse_from(base_arguments.clone())
                .unwrap_err()
                .kind(),
            ErrorKind::MissingRequiredArgument
        );
        let mut bad_arguments = base_arguments.clone();
        bad_arguments.push("--batch-interval-start=1".to_string());
        assert_eq!(
            Options::try_parse_from(bad_arguments).unwrap_err().kind(),
            ErrorKind::MissingRequiredArgument
        );
        let mut bad_arguments = base_arguments.clone();
        bad_arguments.push("--batch-interval-duration=1".to_string());
        assert_eq!(
            Options::try_parse_from(bad_arguments).unwrap_err().kind(),
            ErrorKind::MissingRequiredArgument
        );
        let mut bad_arguments = base_arguments.clone();
        bad_arguments.extend([
            "--batch-interval-start=1".to_string(),
            "--batch-interval-duration=1".to_string(),
            "--batch-id=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_string(),
        ]);
        assert_eq!(
            Options::try_parse_from(bad_arguments).unwrap_err().kind(),
            ErrorKind::ArgumentConflict
        );
        let mut bad_arguments = base_arguments.clone();
        bad_arguments.extend([
            "--batch-interval-start=1".to_string(),
            "--batch-id=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_string(),
        ]);
        assert_eq!(
            Options::try_parse_from(bad_arguments).unwrap_err().kind(),
            ErrorKind::ArgumentConflict
        );
        let mut bad_arguments = base_arguments.clone();
        bad_arguments.extend([
            "--batch-interval-duration=1".to_string(),
            "--batch-id=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_string(),
        ]);
        assert_eq!(
            Options::try_parse_from(bad_arguments).unwrap_err().kind(),
            ErrorKind::ArgumentConflict
        );
        let mut bad_arguments = base_arguments.clone();
        bad_arguments.extend([
            "--batch-interval-start=1".to_string(),
            "--current-batch".to_string(),
        ]);
        assert_eq!(
            Options::try_parse_from(bad_arguments).unwrap_err().kind(),
            ErrorKind::ArgumentConflict
        );
        let mut bad_arguments = base_arguments.clone();
        bad_arguments.extend([
            "--batch-interval-duration=1".to_string(),
            "--current-batch".to_string(),
        ]);
        assert_eq!(
            Options::try_parse_from(bad_arguments).unwrap_err().kind(),
            ErrorKind::ArgumentConflict
        );
        let mut bad_arguments = base_arguments.clone();
        bad_arguments.extend([
            "--batch-id=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_string(),
            "--current-batch".to_string(),
        ]);
        assert_eq!(
            Options::try_parse_from(bad_arguments).unwrap_err().kind(),
            ErrorKind::ArgumentConflict
        );
    }
}
