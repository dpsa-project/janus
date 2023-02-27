//! Implements portions of collect sub-protocol for DAP leader and helper.

use crate::{
    aggregator::{
        aggregate_share::compute_aggregate_share, query_type::CollectableQueryType,
        send_request_to_helper, Error,
    },
    datastore::{
        self,
        models::AcquiredCollectionJob,
        models::{CollectionJobState, Lease},
        Datastore,
    },
    task::{self, PRIO3_AES128_VERIFY_KEY_LENGTH},
};
use derivative::Derivative;
#[cfg(feature = "fpvec_bounded_l2")]
use fixed::types::extra::{U15, U31, U63};
#[cfg(feature = "fpvec_bounded_l2")]
use fixed::{FixedI16, FixedI32, FixedI64};
use futures::future::BoxFuture;
#[cfg(feature = "test-util")]
use janus_core::test_util::dummy_vdaf;
use janus_core::{task::VdafInstance, time::Clock};
use janus_messages::{
    query_type::{FixedSize, QueryType, TimeInterval},
    AggregateShare, AggregateShareReq, BatchSelector,
};
use opentelemetry::{
    metrics::{Counter, Histogram, Meter, Unit},
    Context, KeyValue, Value,
};
#[cfg(feature = "fpvec_bounded_l2")]
use prio::vdaf::prio3::Prio3Aes128FixedPointBoundedL2VecSum;
use prio::{
    codec::{Decode, Encode},
    vdaf::{
        self,
        prio3::{
            Prio3Aes128Count, Prio3Aes128CountVecMultithreaded, Prio3Aes128Histogram,
            Prio3Aes128Sum,
        },
    },
};
use reqwest::Method;
use std::{sync::Arc, time::Duration};
use tokio::try_join;
use tracing::{info, warn};

/// Drives a collection job.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct CollectionJobDriver {
    http_client: reqwest::Client,
    #[derivative(Debug = "ignore")]
    metrics: CollectionJobDriverMetrics,
}

impl CollectionJobDriver {
    /// Create a new [`CollectionJobDriver`].
    pub fn new(http_client: reqwest::Client, meter: &Meter) -> Self {
        println!("new collect job driver created");
        Self {
            http_client,
            metrics: CollectionJobDriverMetrics::new(meter),
        }
    }

    /// Step the provided collection job, for which a lease should have been acquired (though this
    /// should be idempotent). If the collection job runs to completion, the leader share, helper
    /// share, report count and report ID checksum will be written to the `collection_jobs` table,
    /// and a subsequent request to the collection job URI will yield the aggregate shares. The collect
    /// job's lease is released, though it won't matter since the job will no longer be eligible to
    /// be run.
    ///
    /// If some error occurs (including a failure getting the helper's aggregate share), neither
    /// aggregate share is written to the datastore. A subsequent request to the collection job URI
    /// will not yield a result. The collection job lease will eventually expire, allowing a later run
    /// of the collection job driver to try again. Both aggregate shares will be recomputed at that
    /// time.
    pub async fn step_collection_job<C: Clock>(
        &self,
        datastore: Arc<Datastore<C>>,
        lease: Arc<Lease<AcquiredCollectionJob>>,
    ) -> Result<(), Error> {
        println!("step_collect_job!");

        match (lease.leased().query_type(), lease.leased().vdaf()) {
            (task::QueryType::TimeInterval, VdafInstance::Prio3Aes128Count) => {
                self.step_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, TimeInterval, Prio3Aes128Count>(
                    datastore,
                    lease
                )
                .await
            }

            (task::QueryType::TimeInterval, VdafInstance::Prio3Aes128CountVec { .. }) => {
                self.step_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, TimeInterval, Prio3Aes128CountVecMultithreaded>(
                    datastore,
                    lease
                )
                .await
            }

            (task::QueryType::TimeInterval, VdafInstance::Prio3Aes128Sum { .. }) => {
                self.step_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, TimeInterval, Prio3Aes128Sum>(
                    datastore,
                    lease
                )
                .await
            }

            (task::QueryType::TimeInterval, VdafInstance::Prio3Aes128Histogram { .. }) => {
                self.step_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, TimeInterval, Prio3Aes128Histogram>(
                    datastore,
                    lease,
                )
                .await
            }

#[cfg(feature = "fpvec_bounded_l2")]
            (task::QueryType::TimeInterval, VdafInstance::Prio3Aes128FixedPoint16BitBoundedL2VecSum { .. }) => {
                self.step_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, TimeInterval, Prio3Aes128FixedPointBoundedL2VecSum<FixedI16<U15>>>(
                    datastore,
                    lease,
                )
                .await
            }

#[cfg(feature = "fpvec_bounded_l2")]
            (task::QueryType::TimeInterval, VdafInstance::Prio3Aes128FixedPoint32BitBoundedL2VecSum { .. }) => {
                self.step_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, TimeInterval, Prio3Aes128FixedPointBoundedL2VecSum<FixedI32<U31>>>(
                    datastore,
                    lease,
                )
                .await
            }

#[cfg(feature = "fpvec_bounded_l2")]
            (task::QueryType::TimeInterval, VdafInstance::Prio3Aes128FixedPoint64BitBoundedL2VecSum { .. }) => {
                self.step_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, TimeInterval, Prio3Aes128FixedPointBoundedL2VecSum<FixedI64<U63>>>(
                    datastore,
                    lease,
                )
                .await
            }

            #[cfg(feature = "test-util")]
            (task::QueryType::TimeInterval, VdafInstance::Fake) => {
                self.step_collection_job_generic::<0, C, TimeInterval, dummy_vdaf::Vdaf>(
                    datastore,
                    lease,
                )
                .await
            }

            (task::QueryType::FixedSize{..}, VdafInstance::Prio3Aes128Count) => {
                self.step_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, FixedSize, Prio3Aes128Count>(
                    datastore,
                    lease
                )
                .await
            }

            (task::QueryType::FixedSize{..}, VdafInstance::Prio3Aes128CountVec { .. }) => {
                self.step_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, FixedSize, Prio3Aes128CountVecMultithreaded>(
                    datastore,
                    lease
                )
                .await
            }

            (task::QueryType::FixedSize{..}, VdafInstance::Prio3Aes128Sum { .. }) => {
                self.step_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, FixedSize, Prio3Aes128Sum>(
                    datastore,
                    lease
                )
                .await
            }

            (task::QueryType::FixedSize{..}, VdafInstance::Prio3Aes128Histogram { .. }) => {
                self.step_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, FixedSize, Prio3Aes128Histogram>(
                    datastore,
                    lease,
                )
                .await
            }

#[cfg(feature = "fpvec_bounded_l2")]
            (task::QueryType::FixedSize{..}, VdafInstance::Prio3Aes128FixedPoint16BitBoundedL2VecSum { .. }) => {
                self.step_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, FixedSize, Prio3Aes128FixedPointBoundedL2VecSum<FixedI16<U15>>>(
                    datastore,
                    lease,
                )
                .await
            }

#[cfg(feature = "fpvec_bounded_l2")]
            (task::QueryType::FixedSize{..}, VdafInstance::Prio3Aes128FixedPoint32BitBoundedL2VecSum { .. }) => {
                self.step_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, FixedSize, Prio3Aes128FixedPointBoundedL2VecSum<FixedI32<U31>>>(
                    datastore,
                    lease,
                )
                .await
            }

#[cfg(feature = "fpvec_bounded_l2")]
            (task::QueryType::FixedSize{..}, VdafInstance::Prio3Aes128FixedPoint64BitBoundedL2VecSum { .. }) => {
                self.step_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, FixedSize, Prio3Aes128FixedPointBoundedL2VecSum<FixedI64<U63>>>(
                    datastore,
                    lease,
                )
                .await
            }

            #[cfg(feature = "test-util")]
            (task::QueryType::FixedSize{..}, VdafInstance::Fake) => {
                self.step_collection_job_generic::<0, C, FixedSize, dummy_vdaf::Vdaf>(
                    datastore,
                    lease,
                )
                .await
            }


            _ => panic!("VDAF {:?} is not yet supported", lease.leased().vdaf()),
        }
    }

    #[tracing::instrument(skip(self, datastore), err)]
    async fn step_collection_job_generic<
        const L: usize,
        C: Clock,
        Q: CollectableQueryType,
        A: vdaf::Aggregator<L>,
    >(
        &self,
        datastore: Arc<Datastore<C>>,
        lease: Arc<Lease<AcquiredCollectionJob>>,
    ) -> Result<(), Error>
    where
        A: 'static,
        A::AggregationParam: Send + Sync,
        A::AggregateShare: 'static + Send + Sync,
        for<'a> &'a A::AggregateShare: Into<Vec<u8>>,
        for<'a> <A::AggregateShare as TryFrom<&'a [u8]>>::Error: std::fmt::Debug,
        Vec<u8>: for<'a> From<&'a A::AggregateShare>,
        A::OutputShare: PartialEq + Eq + Send + Sync + for<'a> TryFrom<&'a [u8]>,
        for<'a> &'a A::OutputShare: Into<Vec<u8>>,
    {
        let (task, collection_job, batch_aggregations) = datastore
            .run_tx_with_name("step_collection_job_1", |tx| {
                let lease = Arc::clone(&lease);
                Box::pin(async move {
                    // TODO(#224): Consider fleshing out `AcquiredCollectionJob` to include a `Task`,
                    // `A::AggregationParam`, etc. so that we don't have to do more DB queries here.
                    let task = tx
                        .get_task(lease.leased().task_id())
                        .await?
                        .ok_or_else(|| {
                            datastore::Error::User(
                                Error::UnrecognizedTask(*lease.leased().task_id()).into(),
                            )
                        })?;
                    println!("defined task successfully");

                    let collection_job = tx
                        .get_collection_job::<L, Q, A>(lease.leased().collection_job_id())
                        .await?
                        .ok_or_else(|| {
                            datastore::Error::User(
                                Error::UnrecognizedCollectionJob(
                                    *lease.leased().collection_job_id(),
                                )
                                .into(),
                            )
                        })?;

                    println!("got collect job successfully");

                    let batch_aggregations = Q::get_batch_aggregations_for_collect_identifier(
                        tx,
                        &task,
                        collection_job.batch_identifier(),
                        collection_job.aggregation_parameter(),
                    )
                    .await?;

                    Ok((task, collection_job, batch_aggregations))
                })
            })
            .await?;

        if matches!(collection_job.state(), CollectionJobState::Finished { .. }) {
            warn!("collection job being stepped already has a computed helper share");
            self.metrics
                .jobs_already_finished_counter
                .add(&Context::current(), 1, &[]);
            return Ok(());
        }

        let (leader_aggregate_share, report_count, checksum) =
            compute_aggregate_share::<L, Q, A>(&task, &batch_aggregations)
                .await
                .map_err(|e| datastore::Error::User(e.into()))?;

        println!("sending aggregate share to helper!");
        // Send an aggregate share request to the helper.
        let req = AggregateShareReq::<Q>::new(
            BatchSelector::new(collection_job.batch_identifier().clone()),
            collection_job.aggregation_parameter().get_encoded(),
            report_count,
            checksum,
        );

        let resp_bytes = send_request_to_helper(
            &self.http_client,
            Method::POST,
            task.aggregate_shares_uri()?,
            AggregateShareReq::<TimeInterval>::MEDIA_TYPE,
            req,
            task.primary_aggregator_auth_token(),
            &self.metrics.http_request_duration_histogram,
        )
        .await?;

        println!("sending share is done!, storing helper aggregate share in data store");

        // Store the helper aggregate share in the datastore so that a later request to a collect
        // job URI can serve it up.
        let collection_job = Arc::new(
            collection_job.with_state(CollectionJobState::Finished {
                report_count,
                encrypted_helper_aggregate_share: AggregateShare::get_decoded(&resp_bytes)?
                    .encrypted_aggregate_share()
                    .clone(),
                leader_aggregate_share,
            }),
        );

        println!("running transaction now?");

        datastore
            .run_tx_with_name("step_collection_job_2", |tx| {
                let (lease, collection_job) = (Arc::clone(&lease), Arc::clone(&collection_job));
                let metrics = self.metrics.clone();

                println!("inside transaction");

                Box::pin(async move {
                    let maybe_updated_collection_job = tx
                        .get_collection_job::<L, Q, A>(collection_job.collection_job_id())
                        .await?
                        .ok_or_else(|| {
                            datastore::Error::User(
                                Error::UnrecognizedCollectionJob(*collection_job.collection_job_id()).into(),
                            )
                        })?;

                    match maybe_updated_collection_job.state() {
                        CollectionJobState::Start => {
                            tx.update_collection_job::<L, Q, A>(&collection_job).await?;
                            tx.release_collection_job(&lease).await?;
                            metrics.jobs_finished_counter.add(&Context::current(), 1, &[]);
                        }

                        CollectionJobState::Deleted => {
                            // If the collection job was deleted between when we acquired it and now, discard
                            // the aggregate shares and leave the job in the deleted state so that
                            // appropriate status can be returned from polling the collection job URI and GC
                            // can run (#313).
                            info!(
                                collection_job_id = %collection_job.collection_job_id(),
                                "collection job was deleted while lease was held. Discarding aggregate results.",
                            );
                            metrics.deleted_jobs_encountered_counter.add(&Context::current(), 1, &[]);
                        }

                        state => {
                            // It shouldn't be possible for a collection job to move to the abandoned
                            // or finished state while this collection job driver held its lease.
                            metrics.unexpected_job_state_counter.add(&Context::current(), 1, &[KeyValue::new("state", Value::from(format!("{state}")))]);
                            panic!(
                                "collection job {} unexpectedly in state {}",
                                collection_job.collection_job_id(), state
                            );
                        }
                    }

                    Ok(())
                })
            })
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self, datastore), err)]
    pub async fn abandon_collection_job<C: Clock>(
        &self,
        datastore: Arc<Datastore<C>>,
        lease: Lease<AcquiredCollectionJob>,
    ) -> Result<(), Error> {
        match (lease.leased().query_type(), lease.leased().vdaf()) {
            (task::QueryType::TimeInterval, VdafInstance::Prio3Aes128Count) => {
                self.abandon_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, TimeInterval, Prio3Aes128Count>(
                    datastore,
                    lease
                )
                .await
            }

            (task::QueryType::TimeInterval, VdafInstance::Prio3Aes128CountVec{..}) => {
                self.abandon_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, TimeInterval, Prio3Aes128CountVecMultithreaded>(
                    datastore,
                    lease
                )
                .await
            }

            (task::QueryType::TimeInterval, VdafInstance::Prio3Aes128Sum{..}) => {
                self.abandon_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, TimeInterval, Prio3Aes128Sum>(
                    datastore,
                    lease
                )
                .await
            }

            (task::QueryType::TimeInterval, VdafInstance::Prio3Aes128Histogram{..}) => {
                self.abandon_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, TimeInterval, Prio3Aes128Histogram>(
                    datastore,
                    lease,
                )
                .await
            }

#[cfg(feature = "fpvec_bounded_l2")]
            (task::QueryType::TimeInterval, VdafInstance::Prio3Aes128FixedPoint16BitBoundedL2VecSum{..}) => {
                self.abandon_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, TimeInterval, Prio3Aes128FixedPointBoundedL2VecSum<FixedI16<U15>>>(
                    datastore,
                    lease,
                )
                .await
            }

#[cfg(feature = "fpvec_bounded_l2")]
            (task::QueryType::TimeInterval, VdafInstance::Prio3Aes128FixedPoint32BitBoundedL2VecSum{..}) => {
                self.abandon_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, TimeInterval, Prio3Aes128FixedPointBoundedL2VecSum<FixedI32<U31>>>(
                    datastore,
                    lease,
                )
                .await
            }

#[cfg(feature = "fpvec_bounded_l2")]
            (task::QueryType::TimeInterval, VdafInstance::Prio3Aes128FixedPoint64BitBoundedL2VecSum{..}) => {
                self.abandon_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, TimeInterval, Prio3Aes128FixedPointBoundedL2VecSum<FixedI64<U63>>>(
                    datastore,
                    lease,
                )
                .await
            }

            #[cfg(feature = "test-util")]
            (task::QueryType::TimeInterval, VdafInstance::Fake) => {
                self.abandon_collection_job_generic::<0, C, TimeInterval, dummy_vdaf::Vdaf>(
                    datastore,
                    lease,
                )
                .await
            }

            (task::QueryType::FixedSize{..}, VdafInstance::Prio3Aes128Count) => {
                self.abandon_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, FixedSize, Prio3Aes128Count>(
                    datastore,
                    lease
                )
                .await
            }

            (task::QueryType::FixedSize{..}, VdafInstance::Prio3Aes128CountVec{..}) => {
                self.abandon_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, FixedSize, Prio3Aes128CountVecMultithreaded>(
                    datastore,
                    lease
                )
                .await
            }

            (task::QueryType::FixedSize{..}, VdafInstance::Prio3Aes128Sum{..}) => {
                self.abandon_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, FixedSize, Prio3Aes128Sum>(
                    datastore,
                    lease
                )
                .await
            }

            (task::QueryType::FixedSize{..}, VdafInstance::Prio3Aes128Histogram{..}) => {
                self.abandon_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, FixedSize, Prio3Aes128Histogram>(
                    datastore,
                    lease,
                )
                .await
            }

#[cfg(feature = "fpvec_bounded_l2")]
            (task::QueryType::FixedSize{..}, VdafInstance::Prio3Aes128FixedPoint16BitBoundedL2VecSum{..}) => {
                self.abandon_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, FixedSize, Prio3Aes128FixedPointBoundedL2VecSum<FixedI16<U15>>>(
                    datastore,
                    lease,
                )
                .await
            }

#[cfg(feature = "fpvec_bounded_l2")]
            (task::QueryType::FixedSize{..}, VdafInstance::Prio3Aes128FixedPoint32BitBoundedL2VecSum{..}) => {
                self.abandon_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, FixedSize, Prio3Aes128FixedPointBoundedL2VecSum<FixedI32<U31>>>(
                    datastore,
                    lease,
                )
                .await
            }

#[cfg(feature = "fpvec_bounded_l2")]
            (task::QueryType::FixedSize{..}, VdafInstance::Prio3Aes128FixedPoint64BitBoundedL2VecSum{..}) => {
                self.abandon_collection_job_generic::<PRIO3_AES128_VERIFY_KEY_LENGTH, C, FixedSize, Prio3Aes128FixedPointBoundedL2VecSum<FixedI64<U63>>>(
                    datastore,
                    lease,
                )
                .await
            }

            #[cfg(feature = "test-util")]
            (task::QueryType::FixedSize{..}, VdafInstance::Fake) => {
                self.abandon_collection_job_generic::<0, C, FixedSize, dummy_vdaf::Vdaf>(
                    datastore,
                    lease,
                )
                .await
            }


            _ => panic!("VDAF {:?} is not yet supported", lease.leased().vdaf()),
        }
    }

    async fn abandon_collection_job_generic<
        const L: usize,
        C: Clock,
        Q: QueryType,
        A: vdaf::Aggregator<L>,
    >(
        &self,
        datastore: Arc<Datastore<C>>,
        lease: Lease<AcquiredCollectionJob>,
    ) -> Result<(), Error>
    where
        A::AggregationParam: Send + Sync,
        A::AggregateShare: Send + Sync,
        for<'a> &'a A::AggregateShare: Into<Vec<u8>>,
        for<'a> <A::AggregateShare as TryFrom<&'a [u8]>>::Error: std::fmt::Debug,
    {
        let lease = Arc::new(lease);
        datastore
            .run_tx_with_name("abandon_collection_job", |tx| {
                let lease = Arc::clone(&lease);
                Box::pin(async move {
                    let collection_job = tx
                        .get_collection_job::<L, Q, A>(lease.leased().collection_job_id())
                        .await?
                        .ok_or_else(|| {
                            datastore::Error::DbState(format!(
                                "collection job {} was leased but no collection job was found",
                                lease.leased().collection_job_id(),
                            ))
                        })?
                        .with_state(CollectionJobState::Abandoned);
                    let update_future = tx.update_collection_job(&collection_job);
                    let release_future = tx.release_collection_job(&lease);
                    try_join!(update_future, release_future)?;
                    Ok(())
                })
            })
            .await?;
        Ok(())
    }

    /// Produce a closure for use as a `[JobDriver::JobAcquirer`].
    pub fn make_incomplete_job_acquirer_callback<C: Clock>(
        &self,
        datastore: Arc<Datastore<C>>,
        lease_duration: Duration,
    ) -> impl Fn(usize) -> BoxFuture<'static, Result<Vec<Lease<AcquiredCollectionJob>>, datastore::Error>>
    {
        move |maximum_acquire_count_per_query_type| {
            let datastore = Arc::clone(&datastore);
            Box::pin(async move {
                datastore
                    .run_tx_with_name("acquire_collection_jobs", |tx| {
                        Box::pin(async move {
                            let (time_interval_jobs, fixed_size_jobs) = try_join!(
                                tx.acquire_incomplete_time_interval_collection_jobs(
                                    &lease_duration,
                                    maximum_acquire_count_per_query_type,
                                ),
                                tx.acquire_incomplete_fixed_size_collection_jobs(
                                    &lease_duration,
                                    maximum_acquire_count_per_query_type
                                ),
                            )?;
                            Ok(time_interval_jobs
                                .into_iter()
                                .chain(fixed_size_jobs.into_iter())
                                .collect())
                        })
                    })
                    .await
            })
        }
    }

    /// Produce a closure for use as a `[JobDriver::JobStepper]`.
    pub fn make_job_stepper_callback<C: Clock>(
        self: Arc<Self>,
        datastore: Arc<Datastore<C>>,
        maximum_attempts_before_failure: usize,
    ) -> impl Fn(Lease<AcquiredCollectionJob>) -> BoxFuture<'static, Result<(), super::Error>> {
        move |collection_job_lease: Lease<AcquiredCollectionJob>| {
            let (this, datastore) = (Arc::clone(&self), Arc::clone(&datastore));
            Box::pin(async move {
                if collection_job_lease.lease_attempts() > maximum_attempts_before_failure {
                    warn!(
                        attempts = %collection_job_lease.lease_attempts(),
                        max_attempts = %maximum_attempts_before_failure,
                        "Abandoning job due to too many failed attempts"
                    );
                    this.metrics
                        .jobs_abandoned_counter
                        .add(&Context::current(), 1, &[]);
                    return this
                        .abandon_collection_job(datastore, collection_job_lease)
                        .await;
                }

                this.step_collection_job(datastore, Arc::new(collection_job_lease))
                    .await
            })
        }
    }
}

/// Holds various metrics instruments for a collection job driver.
#[derive(Clone)]
struct CollectionJobDriverMetrics {
    jobs_finished_counter: Counter<u64>,
    http_request_duration_histogram: Histogram<f64>,
    jobs_abandoned_counter: Counter<u64>,
    jobs_already_finished_counter: Counter<u64>,
    deleted_jobs_encountered_counter: Counter<u64>,
    unexpected_job_state_counter: Counter<u64>,
}

impl CollectionJobDriverMetrics {
    fn new(meter: &Meter) -> Self {
        let jobs_finished_counter = meter
            .u64_counter("janus_collection_jobs_finished")
            .with_description("Count of finished collection jobs.")
            .init();
        jobs_finished_counter.add(&Context::current(), 0, &[]);

        let http_request_duration_histogram = meter
            .f64_histogram("janus_http_request_duration_seconds")
            .with_description(
                "The amount of time elapsed while making an HTTP request to a helper.",
            )
            .with_unit(Unit::new("seconds"))
            .init();

        let jobs_abandoned_counter = meter
            .u64_counter("janus_collection_jobs_abandoned")
            .with_description("Count of abandoned collection jobs.")
            .init();
        jobs_abandoned_counter.add(&Context::current(), 0, &[]);

        let jobs_already_finished_counter = meter
            .u64_counter("janus_collection_jobs_already_finished")
            .with_description(
                "Count of collection jobs for which a lease was acquired but were already finished.",
            )
            .init();
        jobs_already_finished_counter.add(&Context::current(), 0, &[]);

        let deleted_jobs_encountered_counter = meter
            .u64_counter("janus_collect_deleted_jobs_encountered")
            .with_description(
                "Count of collection jobs that were run to completion but found to have been deleted.",
            )
            .init();
        deleted_jobs_encountered_counter.add(&Context::current(), 0, &[]);

        let unexpected_job_state_counter = meter
            .u64_counter("janus_collect_unexpected_job_state")
            .with_description("Count of collection jobs that were run to completion but found in an unexpected state.").init();
        unexpected_job_state_counter.add(&Context::current(), 0, &[]);

        Self {
            jobs_finished_counter,
            http_request_duration_histogram,
            jobs_abandoned_counter,
            jobs_already_finished_counter,
            deleted_jobs_encountered_counter,
            unexpected_job_state_counter,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        aggregator::{collection_job_driver::CollectionJobDriver, DapProblemType, Error},
        binary_utils::job_driver::JobDriver,
        datastore::{
            models::{
                AcquiredCollectionJob, AggregationJob, AggregationJobState, BatchAggregation,
                CollectionJob, CollectionJobState, LeaderStoredReport, Lease, ReportAggregation,
                ReportAggregationState,
            },
            test_util::ephemeral_datastore,
            Datastore,
        },
        messages::TimeExt,
        task::{test_util::TaskBuilder, QueryType, Task},
    };
    use assert_matches::assert_matches;
    use http::{header::CONTENT_TYPE, StatusCode};
    use janus_core::{
        task::VdafInstance,
        test_util::{
            dummy_vdaf::{self, AggregationParam, OutputShare},
            install_test_trace_subscriber,
            runtime::TestRuntimeManager,
        },
        time::{Clock, MockClock, TimeExt as CoreTimeExt},
        Runtime,
    };
    use janus_messages::{
        query_type::TimeInterval, AggregateShare, AggregateShareReq, BatchSelector, Duration,
        HpkeCiphertext, HpkeConfigId, Interval, ReportIdChecksum, Role,
    };
    use opentelemetry::global::meter;
    use prio::codec::{Decode, Encode};
    use rand::random;
    use std::{str, sync::Arc, time::Duration as StdDuration};
    use url::Url;

    async fn setup_collection_job_test_case(
        server: &mut mockito::Server,
        clock: MockClock,
        datastore: Arc<Datastore<MockClock>>,
        acquire_lease: bool,
    ) -> (
        Task,
        Option<Lease<AcquiredCollectionJob>>,
        CollectionJob<0, TimeInterval, dummy_vdaf::Vdaf>,
    ) {
        let time_precision = Duration::from_seconds(500);
        let task = TaskBuilder::new(QueryType::TimeInterval, VdafInstance::Fake, Role::Leader)
            .with_aggregator_endpoints(Vec::from([
                Url::parse("http://irrelevant").unwrap(), // leader URL doesn't matter
                Url::parse(&server.url()).unwrap(),
            ]))
            .with_time_precision(time_precision)
            .with_min_batch_size(10)
            .build();
        let batch_interval = Interval::new(clock.now(), Duration::from_seconds(2000)).unwrap();
        let aggregation_param = AggregationParam(0);

        let collection_job = CollectionJob::<0, TimeInterval, dummy_vdaf::Vdaf>::new(
            *task.id(),
            random(),
            batch_interval,
            aggregation_param,
            CollectionJobState::Start,
        );

        let lease = datastore
            .run_tx(|tx| {
                let (clock, task, collection_job) =
                    (clock.clone(), task.clone(), collection_job.clone());
                Box::pin(async move {
                    tx.put_task(&task).await?;

                    tx.put_collection_job::<0, TimeInterval, dummy_vdaf::Vdaf>(&collection_job)
                        .await?;

                    let aggregation_job_id = random();
                    let report_timestamp = clock
                        .now()
                        .to_batch_interval_start(task.time_precision())
                        .unwrap();
                    tx.put_aggregation_job(
                        &AggregationJob::<0, TimeInterval, dummy_vdaf::Vdaf>::new(
                            *task.id(),
                            aggregation_job_id,
                            aggregation_param,
                            (),
                            Interval::new(report_timestamp, Duration::from_seconds(1)).unwrap(),
                            AggregationJobState::Finished,
                        ),
                    )
                    .await?;

                    let report = LeaderStoredReport::new_dummy(*task.id(), report_timestamp);

                    tx.put_client_report(&dummy_vdaf::Vdaf::new(), &report)
                        .await?;

                    tx.put_report_aggregation(&ReportAggregation::<0, dummy_vdaf::Vdaf>::new(
                        *task.id(),
                        aggregation_job_id,
                        *report.metadata().id(),
                        *report.metadata().time(),
                        0,
                        ReportAggregationState::Finished(OutputShare()),
                    ))
                    .await?;

                    tx.put_batch_aggregation(
                        &BatchAggregation::<0, TimeInterval, dummy_vdaf::Vdaf>::new(
                            *task.id(),
                            Interval::new(clock.now(), time_precision).unwrap(),
                            aggregation_param,
                            dummy_vdaf::AggregateShare(0),
                            5,
                            ReportIdChecksum::get_decoded(&[3; 32]).unwrap(),
                        ),
                    )
                    .await?;
                    tx.put_batch_aggregation(
                        &BatchAggregation::<0, TimeInterval, dummy_vdaf::Vdaf>::new(
                            *task.id(),
                            Interval::new(
                                clock.now().add(&Duration::from_seconds(1000)).unwrap(),
                                time_precision,
                            )
                            .unwrap(),
                            aggregation_param,
                            dummy_vdaf::AggregateShare(0),
                            5,
                            ReportIdChecksum::get_decoded(&[2; 32]).unwrap(),
                        ),
                    )
                    .await?;

                    if acquire_lease {
                        let lease = tx
                            .acquire_incomplete_time_interval_collection_jobs(
                                &StdDuration::from_secs(100),
                                1,
                            )
                            .await?
                            .remove(0);
                        assert_eq!(task.id(), lease.leased().task_id());
                        assert_eq!(
                            collection_job.collection_job_id(),
                            lease.leased().collection_job_id()
                        );
                        Ok(Some(lease))
                    } else {
                        Ok(None)
                    }
                })
            })
            .await
            .unwrap();

        (task, lease, collection_job)
    }

    #[tokio::test]
    async fn drive_collection_job() {
        install_test_trace_subscriber();
        let mut server = mockito::Server::new_async().await;
        let clock = MockClock::default();
        let ephemeral_datastore = ephemeral_datastore().await;
        let ds = Arc::new(ephemeral_datastore.datastore(clock.clone()));

        let time_precision = Duration::from_seconds(500);
        let task = TaskBuilder::new(QueryType::TimeInterval, VdafInstance::Fake, Role::Leader)
            .with_aggregator_endpoints(Vec::from([
                Url::parse("http://irrelevant").unwrap(), // leader URL doesn't matter
                Url::parse(&server.url()).unwrap(),
            ]))
            .with_time_precision(time_precision)
            .with_min_batch_size(10)
            .build();
        let agg_auth_token = task.primary_aggregator_auth_token();
        let batch_interval = Interval::new(clock.now(), Duration::from_seconds(2000)).unwrap();
        let aggregation_param = AggregationParam(0);

        let (collection_job_id, lease) = ds
            .run_tx(|tx| {
                let (clock, task) = (clock.clone(), task.clone());
                Box::pin(async move {
                    tx.put_task(&task).await?;

                    let collection_job_id = random();
                    tx.put_collection_job(
                        &CollectionJob::<0, TimeInterval, dummy_vdaf::Vdaf>::new(
                            *task.id(),
                            collection_job_id,
                            batch_interval,
                            aggregation_param,
                            CollectionJobState::Start,
                        ),
                    )
                    .await?;

                    let aggregation_job_id = random();
                    let report_timestamp = clock
                        .now()
                        .to_batch_interval_start(task.time_precision())
                        .unwrap();
                    tx.put_aggregation_job(
                        &AggregationJob::<0, TimeInterval, dummy_vdaf::Vdaf>::new(
                            *task.id(),
                            aggregation_job_id,
                            aggregation_param,
                            (),
                            Interval::new(report_timestamp, Duration::from_seconds(1)).unwrap(),
                            AggregationJobState::Finished,
                        ),
                    )
                    .await?;

                    let report = LeaderStoredReport::new_dummy(*task.id(), report_timestamp);

                    tx.put_client_report(&dummy_vdaf::Vdaf::new(), &report)
                        .await?;

                    tx.put_report_aggregation(&ReportAggregation::<0, dummy_vdaf::Vdaf>::new(
                        *task.id(),
                        aggregation_job_id,
                        *report.metadata().id(),
                        *report.metadata().time(),
                        0,
                        ReportAggregationState::Finished(OutputShare()),
                    ))
                    .await?;

                    let lease = Arc::new(
                        tx.acquire_incomplete_time_interval_collection_jobs(
                            &StdDuration::from_secs(100),
                            1,
                        )
                        .await?
                        .remove(0),
                    );

                    assert_eq!(task.id(), lease.leased().task_id());
                    assert_eq!(&collection_job_id, lease.leased().collection_job_id());
                    Ok((collection_job_id, lease))
                })
            })
            .await
            .unwrap();

        let collection_job_driver = CollectionJobDriver::new(
            reqwest::Client::builder().build().unwrap(),
            &meter("collection_job_driver"),
        );

        // No batch aggregations inserted yet.
        let error = collection_job_driver
            .step_collection_job(ds.clone(), Arc::clone(&lease))
            .await
            .unwrap_err();
        assert_matches!(error, Error::InvalidBatchSize(error_task_id, 0) => {
            assert_eq!(task.id(), &error_task_id)
        });

        // Put some batch aggregations in the DB.
        ds.run_tx(|tx| {
            let (clock, task) = (clock.clone(), task.clone());
            Box::pin(async move {
                tx.put_batch_aggregation(
                    &BatchAggregation::<0, TimeInterval, dummy_vdaf::Vdaf>::new(
                        *task.id(),
                        Interval::new(clock.now(), time_precision).unwrap(),
                        aggregation_param,
                        dummy_vdaf::AggregateShare(0),
                        5,
                        ReportIdChecksum::get_decoded(&[3; 32]).unwrap(),
                    ),
                )
                .await?;

                tx.put_batch_aggregation(
                    &BatchAggregation::<0, TimeInterval, dummy_vdaf::Vdaf>::new(
                        *task.id(),
                        Interval::new(
                            clock.now().add(&Duration::from_seconds(1000)).unwrap(),
                            time_precision,
                        )
                        .unwrap(),
                        aggregation_param,
                        dummy_vdaf::AggregateShare(0),
                        5,
                        ReportIdChecksum::get_decoded(&[2; 32]).unwrap(),
                    ),
                )
                .await?;

                Ok(())
            })
        })
        .await
        .unwrap();

        let leader_request = AggregateShareReq::new(
            BatchSelector::new_time_interval(batch_interval),
            aggregation_param.get_encoded(),
            10,
            ReportIdChecksum::get_decoded(&[3 ^ 2; 32]).unwrap(),
        );

        // Simulate helper failing to service the aggregate share request.
        let mocked_failed_aggregate_share = server
            .mock("POST", task.aggregate_shares_uri().unwrap().path())
            .match_header(
                "DAP-Auth-Token",
                str::from_utf8(agg_auth_token.as_bytes()).unwrap(),
            )
            .match_header(
                CONTENT_TYPE.as_str(),
                AggregateShareReq::<TimeInterval>::MEDIA_TYPE,
            )
            .match_body(leader_request.get_encoded())
            .with_status(500)
            .with_header("Content-Type", "application/problem+json")
            .with_body("{\"type\": \"urn:ietf:params:ppm:dap:error:batchQueriedTooManyTimes\"}")
            .create_async()
            .await;

        let error = collection_job_driver
            .step_collection_job(ds.clone(), Arc::clone(&lease))
            .await
            .unwrap_err();
        assert_matches!(
            error,
            Error::Http {
                problem_details,
                dap_problem_type: Some(DapProblemType::BatchQueriedTooManyTimes),
            } => {
                assert_eq!(problem_details.status.unwrap(), StatusCode::INTERNAL_SERVER_ERROR);
            }
        );

        mocked_failed_aggregate_share.assert_async().await;

        // collection job in datastore should be unchanged.
        ds.run_tx(|tx| {
            Box::pin(async move {
                let collection_job = tx
                    .get_collection_job::<0, TimeInterval, dummy_vdaf::Vdaf>(&collection_job_id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(collection_job.state(), &CollectionJobState::Start);
                Ok(())
            })
        })
        .await
        .unwrap();

        // Helper aggregate share is opaque to the leader, so no need to construct a real one
        let helper_response = AggregateShare::new(HpkeCiphertext::new(
            HpkeConfigId::from(100),
            Vec::new(),
            Vec::new(),
        ));

        let mocked_aggregate_share = server
            .mock("POST", task.aggregate_shares_uri().unwrap().path())
            .match_header(
                "DAP-Auth-Token",
                str::from_utf8(agg_auth_token.as_bytes()).unwrap(),
            )
            .match_header(
                CONTENT_TYPE.as_str(),
                AggregateShareReq::<TimeInterval>::MEDIA_TYPE,
            )
            .match_body(leader_request.get_encoded())
            .with_status(200)
            .with_header(CONTENT_TYPE.as_str(), AggregateShare::MEDIA_TYPE)
            .with_body(helper_response.get_encoded())
            .create_async()
            .await;

        collection_job_driver
            .step_collection_job(ds.clone(), Arc::clone(&lease))
            .await
            .unwrap();

        mocked_aggregate_share.assert_async().await;

        // Should now have recorded helper encrypted aggregate share, too.
        ds.run_tx(|tx| {
            let helper_aggregate_share = helper_response.encrypted_aggregate_share().clone();
            Box::pin(async move {
                let collection_job = tx
                    .get_collection_job::<0, TimeInterval, dummy_vdaf::Vdaf>(&collection_job_id)
                    .await
                    .unwrap()
                    .unwrap();

                assert_matches!(collection_job.state(), CollectionJobState::Finished{ encrypted_helper_aggregate_share, .. } => {
                    assert_eq!(encrypted_helper_aggregate_share, &helper_aggregate_share);
                });

                Ok(())
            })
        })
        .await
        .unwrap();

        // Drive collection job again. It should succeed without contacting the helper.
        collection_job_driver
            .step_collection_job(ds.clone(), lease)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn abandon_collection_job() {
        // Setup: insert a collection job into the datastore.
        install_test_trace_subscriber();
        let mut server = mockito::Server::new_async().await;
        let clock = MockClock::default();
        let ephemeral_datastore = ephemeral_datastore().await;
        let ds = Arc::new(ephemeral_datastore.datastore(clock.clone()));

        let (_, lease, collection_job) =
            setup_collection_job_test_case(&mut server, clock, Arc::clone(&ds), true).await;

        let collection_job_driver = CollectionJobDriver::new(
            reqwest::Client::builder().build().unwrap(),
            &meter("collection_job_driver"),
        );

        // Run: abandon the collection job.
        collection_job_driver
            .abandon_collection_job(Arc::clone(&ds), lease.unwrap())
            .await
            .unwrap();

        // Verify: check that the collection job was abandoned, and that it can no longer be acquired.
        let (abandoned_collection_job, leases) = ds
            .run_tx(|tx| {
                let collection_job = collection_job.clone();
                Box::pin(async move {
                    let abandoned_collection_job = tx
                        .get_collection_job::<0, TimeInterval, dummy_vdaf::Vdaf>(
                            collection_job.collection_job_id(),
                        )
                        .await?
                        .unwrap();

                    let leases = tx
                        .acquire_incomplete_time_interval_collection_jobs(
                            &StdDuration::from_secs(100),
                            1,
                        )
                        .await?;

                    Ok((abandoned_collection_job, leases))
                })
            })
            .await
            .unwrap();
        assert_eq!(
            abandoned_collection_job,
            collection_job.with_state(CollectionJobState::Abandoned),
        );
        assert!(leases.is_empty());
    }

    #[tokio::test]
    async fn abandon_failing_collection_job() {
        install_test_trace_subscriber();
        let mut server = mockito::Server::new_async().await;
        let clock = MockClock::default();
        let mut runtime_manager = TestRuntimeManager::new();
        let ephemeral_datastore = ephemeral_datastore().await;
        let ds = Arc::new(ephemeral_datastore.datastore(clock.clone()));

        let (task, _, collection_job) =
            setup_collection_job_test_case(&mut server, clock.clone(), Arc::clone(&ds), false)
                .await;

        // Set up the collection job driver
        let meter = meter("collection_job_driver");
        let collection_job_driver =
            Arc::new(CollectionJobDriver::new(reqwest::Client::new(), &meter));
        let job_driver = Arc::new(JobDriver::new(
            clock.clone(),
            runtime_manager.with_label("stepper"),
            meter,
            StdDuration::from_secs(1),
            StdDuration::from_secs(1),
            10,
            StdDuration::from_secs(60),
            collection_job_driver.make_incomplete_job_acquirer_callback(
                Arc::clone(&ds),
                StdDuration::from_secs(600),
            ),
            collection_job_driver.make_job_stepper_callback(Arc::clone(&ds), 3),
        ));

        // Set up three error responses from our mock helper. These will cause errors in the
        // leader, because the response body is empty and cannot be decoded.
        let failure_mock = server
            .mock("POST", task.aggregate_shares_uri().unwrap().path())
            .with_status(500)
            .expect(3)
            .create_async()
            .await;
        // Set up an extra response that should never be used, to make sure the job driver doesn't
        // make more requests than we expect. If there were no remaining mocks, mockito would have
        // respond with a fallback error response instead.
        let no_more_requests_mock = server
            .mock("POST", task.aggregate_shares_uri().unwrap().path())
            .with_status(500)
            .expect(1)
            .create_async()
            .await;

        // Start up the job driver.
        let task_handle = runtime_manager
            .with_label("driver")
            .spawn(async move { job_driver.run().await });

        // Run the job driver until we try to step the collection job four times. The first three
        // attempts make network requests and fail, while the fourth attempt just marks the job
        // as abandoned.
        for i in 1..=4 {
            // Wait for the next task to be spawned and to complete.
            runtime_manager.wait_for_completed_tasks("stepper", i).await;
            // Advance the clock by the lease duration, so that the job driver can pick up the job
            // and try again.
            clock.advance(Duration::from_seconds(600));
        }
        // Shut down the job driver.
        task_handle.abort();

        // Check that the job driver made the HTTP requests we expected.
        failure_mock.assert_async().await;
        assert!(!no_more_requests_mock.matched_async().await);

        // Confirm that the collection job was abandoned.
        let collection_job_after = ds
            .run_tx(|tx| {
                let collection_job = collection_job.clone();
                Box::pin(async move {
                    tx.get_collection_job::<0, TimeInterval, dummy_vdaf::Vdaf>(
                        collection_job.collection_job_id(),
                    )
                    .await
                })
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            collection_job_after,
            collection_job.with_state(CollectionJobState::Abandoned),
        );
    }

    #[tokio::test]
    async fn delete_collection_job() {
        // Setup: insert a collection job into the datastore.
        install_test_trace_subscriber();
        let mut server = mockito::Server::new_async().await;
        let clock = MockClock::default();
        let ephemeral_datastore = ephemeral_datastore().await;
        let ds = Arc::new(ephemeral_datastore.datastore(clock.clone()));

        let (task, lease, collection_job) =
            setup_collection_job_test_case(&mut server, clock, Arc::clone(&ds), true).await;

        // Delete the collection job
        let collection_job = collection_job.with_state(CollectionJobState::Deleted);

        ds.run_tx(|tx| {
            let collection_job = collection_job.clone();
            Box::pin(async move {
                tx.update_collection_job::<0, TimeInterval, dummy_vdaf::Vdaf>(&collection_job)
                    .await
            })
        })
        .await
        .unwrap();

        // Helper aggregate share is opaque to the leader, so no need to construct a real one
        let helper_response = AggregateShare::new(HpkeCiphertext::new(
            HpkeConfigId::from(100),
            Vec::new(),
            Vec::new(),
        ));

        let mocked_aggregate_share = server
            .mock("POST", task.aggregate_shares_uri().unwrap().path())
            .with_status(200)
            .with_header(CONTENT_TYPE.as_str(), AggregateShare::MEDIA_TYPE)
            .with_body(helper_response.get_encoded())
            .create_async()
            .await;

        let collection_job_driver = CollectionJobDriver::new(
            reqwest::Client::builder().build().unwrap(),
            &meter("collection_job_driver"),
        );

        // Step the collection job. The driver should successfully run the job, but then discard the
        // results when it notices the job has been deleted.
        collection_job_driver
            .step_collection_job(ds.clone(), Arc::new(lease.unwrap()))
            .await
            .unwrap();

        mocked_aggregate_share.assert_async().await;

        // Verify: check that the collection job was abandoned, and that it can no longer be acquired.
        ds.run_tx(|tx| {
            let collection_job = collection_job.clone();
            Box::pin(async move {
                let collection_job = tx
                    .get_collection_job::<0, TimeInterval, dummy_vdaf::Vdaf>(
                        collection_job.collection_job_id(),
                    )
                    .await
                    .unwrap()
                    .unwrap();

                assert_eq!(collection_job.state(), &CollectionJobState::Deleted);

                let leases = tx
                    .acquire_incomplete_time_interval_collection_jobs(
                        &StdDuration::from_secs(100),
                        1,
                    )
                    .await
                    .unwrap();

                assert!(leases.is_empty());

                Ok(())
            })
        })
        .await
        .unwrap();
    }
}
