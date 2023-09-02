use prio::{
    dp::{
        distributions::ZCdpDiscreteGaussian, DifferentialPrivacyBudget,
        DifferentialPrivacyDistribution, DifferentialPrivacyStrategy, DpError,
    },
    field::{Field128, Field64},
    flp::{
        gadgets::{BlindPolyEval, ParallelSumMultithreaded},
        TypeWithNoise,
    },
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum DpStrategyInstance {
    NoDp(NoStrategy),
    ZCdpDiscreteGaussian(ZCdpDiscreteGaussian),
}

impl DpStrategyInstance {}

pub struct NoBudget {}
impl DifferentialPrivacyBudget for NoBudget {}

pub struct NoDistribution {}
impl DifferentialPrivacyDistribution for NoDistribution {}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct NoStrategy {}
impl DifferentialPrivacyStrategy for NoStrategy {
    type Budget = NoBudget;
    type Distribution = NoDistribution;
    type Sensitivity = ();
    fn from_budget(_b: NoBudget) -> Self {
        NoStrategy {}
    }
    fn create_distribution(&self, s: Self::Sensitivity) -> Result<Self::Distribution, DpError> {
        todo!()
    }
}

impl TypeWithNoise<NoStrategy> for prio::flp::types::Sum<Field128> {}
impl TypeWithNoise<NoStrategy> for prio::flp::types::Count<Field64> {}
impl TypeWithNoise<NoStrategy> for prio::flp::types::Histogram<Field128> {}
impl TypeWithNoise<NoStrategy>
    for prio::flp::types::SumVec<
        Field128,
        ParallelSumMultithreaded<Field128, BlindPolyEval<Field128>>,
    >
{
}
