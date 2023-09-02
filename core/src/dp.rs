#[cfg(feature = "test-util")]
use crate::test_util::dummy_vdaf::Vdaf;
use prio::{
    dp::{
        distributions::ZCdpDiscreteGaussian, DifferentialPrivacyBudget,
        DifferentialPrivacyDistribution, DifferentialPrivacyStrategy, DpError,
    },
    field::{Field128, Field64},
    flp::{
        gadgets::{BlindPolyEval, ParallelSumMultithreaded},
        TypeWithNoise,
    }, vdaf::{AggregatorWithNoise, prg::PrgSha3},
};
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////
// identity strategy
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum DpStrategyInstance {
    NoDifferentialPrivacy(NoDifferentialPrivacy),
    ZCdpDiscreteGaussian(ZCdpDiscreteGaussian),
}

impl DpStrategyInstance {}

pub struct NoBudget {}
impl DifferentialPrivacyBudget for NoBudget {}

pub struct NoDistribution {}
impl DifferentialPrivacyDistribution for NoDistribution {}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct NoDifferentialPrivacy {}
impl DifferentialPrivacyStrategy for NoDifferentialPrivacy {
    type Budget = NoBudget;
    type Distribution = NoDistribution;
    type Sensitivity = ();
    fn from_budget(_b: NoBudget) -> Self {
        NoDifferentialPrivacy {}
    }
    fn create_distribution(&self, s: Self::Sensitivity) -> Result<Self::Distribution, DpError> {
        todo!()
    }
}


////////////////////////////////////////////////////////////////
// implementations for vdafs from janus
#[cfg(feature = "test-util")]
impl AggregatorWithNoise<0,16,NoDifferentialPrivacy> for Vdaf {
    fn add_noise_to_agg_share(
        &self,
        dp_strategy: &NoDifferentialPrivacy,
        agg_param: &Self::AggregationParam,
        agg_share: &mut Self::AggregateShare,
        num_measurements: usize,
    ) -> Result<(), prio::vdaf::VdafError> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////
// implementations for vdafs from libprio
impl TypeWithNoise<NoDifferentialPrivacy> for prio::flp::types::Sum<Field128> {}
impl TypeWithNoise<NoDifferentialPrivacy> for prio::flp::types::Count<Field64> {}
impl TypeWithNoise<NoDifferentialPrivacy> for prio::flp::types::Histogram<Field128> {}
impl TypeWithNoise<NoDifferentialPrivacy>
    for prio::flp::types::SumVec<
        Field128,
        ParallelSumMultithreaded<Field128, BlindPolyEval<Field128>>,
    >
{
}

impl AggregatorWithNoise<16,16,NoDifferentialPrivacy> for prio::vdaf::poplar1::Poplar1<PrgSha3, 16> {
    fn add_noise_to_agg_share(
        &self,
        dp_strategy: &NoDifferentialPrivacy,
        agg_param: &Self::AggregationParam,
        agg_share: &mut Self::AggregateShare,
        num_measurements: usize,
    ) -> Result<(), prio::vdaf::VdafError> {
        todo!()
    }
}

