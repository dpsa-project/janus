// use crate::task::VdafInstance;
#[cfg(feature = "test-util")]
use crate::test_util::dummy_vdaf::Vdaf;
use anyhow::anyhow;
use derivative::Derivative;
#[cfg(feature = "fpvec_bounded_l2")]
use fixed::traits::Fixed;
use prio::{
    dp::{
        distributions::ZCdpDiscreteGaussian, DifferentialPrivacyBudget,
        DifferentialPrivacyDistribution, DifferentialPrivacyStrategy, DpError,
    },
    field::{Field128, Field64},
    flp::{
        gadgets::{BlindPolyEval, ParallelSumGadget, ParallelSumMultithreaded, PolyEval},
        types::fixedpoint_l2::{compatible_float::CompatibleFloat, FixedPointBoundedL2VecSum},
        TypeWithNoise,
    },
    vdaf::{prg::PrgSha3, AggregatorWithNoise},
};
use serde::{Deserialize, Serialize};




////////////////////////////////////////////////////////////////
// converting into strategy enum

// pub fn vdaf_instance_into_strategy_instance(vdaf: &VdafInstance) -> DpStrategyInstance {
//     match vdaf {
//         VdafInstance::Prio3Count
//         | VdafInstance::Prio3CountVec { .. }
//         | VdafInstance::Prio3Sum { .. }
//         | VdafInstance::Prio3SumVec { .. }
//         | VdafInstance::Prio3Histogram { .. }
//         | VdafInstance::Poplar1 { .. } => DpStrategyInstance::NoDifferentialPrivacy(NoDifferentialPrivacy {}),
//         // VdafInstance::Prio3FixedPoint16BitBoundedL2VecSum { .. }
//         // | VdafInstance::Prio3FixedPoint32BitBoundedL2VecSum { .. }
//         // | VdafInstance::Prio3FixedPoint64BitBoundedL2VecSum { .. } => todo!(),
//         // VdafInstance::Prio3FixedPoint16BitBoundedL2VecSumZCdp { dp_strategy, .. }
//         // | VdafInstance::Prio3FixedPoint32BitBoundedL2VecSumZCdp { dp_strategy, .. }
//         // | VdafInstance::Prio3FixedPoint64BitBoundedL2VecSumZCdp { dp_strategy, .. } => DpStrategyInstance::ZCdpDiscreteGaussian( dp_strategy.clone() ),
//         VdafInstance::Prio3FixedPointBoundedL2VecSum { bitsize, dp_strategy, length } => todo!(),

//         #[cfg(feature = "test-util")]
//         VdafInstance::Fake
//         | VdafInstance::FakeFailsPrepInit
//         | VdafInstance::FakeFailsPrepStep => DpStrategyInstance::NoDifferentialPrivacy(NoDifferentialPrivacy {}),
//     }
// }


////////////////////////////////////////////////////////////////
// converting strategy enum into strategy types

impl TryFrom<DpStrategyInstance> for NoDifferentialPrivacy {
    type Error = anyhow::Error;
    fn try_from(value: DpStrategyInstance) -> Result<Self, Self::Error> {
        match value {
            DpStrategyInstance::NoDifferentialPrivacy(s) => Ok(s),
            DpStrategyInstance::ZCdpDiscreteGaussian(_) => Err(anyhow!(
                "Attempted to use ZCdp instance for NoDp strategy".to_string(),
            )),
        }
    }
}

impl TryFrom<DpStrategyInstance> for ZCdpDiscreteGaussian {
    type Error = anyhow::Error;
    fn try_from(value: DpStrategyInstance) -> Result<Self, Self::Error> {
        match value {
            DpStrategyInstance::ZCdpDiscreteGaussian(s) => Ok(s),
            DpStrategyInstance::NoDifferentialPrivacy(_) => Err(anyhow!(
                "Attempted to use NoDp instance for ZCdp strategy".to_string(),
            )),
        }
    }
}

////////////////////////////////////////////////////////////////
// identity strategy
#[derive(Debug, Derivative, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DpStrategyInstance {
    NoDifferentialPrivacy(NoDifferentialPrivacy),
    ZCdpDiscreteGaussian(ZCdpDiscreteGaussian),
}

impl DpStrategyInstance {}


pub struct NoBudget {}
impl DifferentialPrivacyBudget for NoBudget {}

pub struct NoDistribution {}
impl DifferentialPrivacyDistribution for NoDistribution {}

#[derive(Debug, Derivative, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NoDifferentialPrivacy {}
impl DifferentialPrivacyStrategy for NoDifferentialPrivacy {
    type Budget = NoBudget;
    type Distribution = NoDistribution;
    type Sensitivity = ();
    fn from_budget(_b: NoBudget) -> Self {
        NoDifferentialPrivacy {}
    }
    fn create_distribution(&self, _s: Self::Sensitivity) -> Result<Self::Distribution, DpError> {
        Ok(NoDistribution {})
    }
}

////////////////////////////////////////////////////////////////
// implementations for vdafs from janus
#[cfg(feature = "test-util")]
impl AggregatorWithNoise<0, 16, NoDifferentialPrivacy> for Vdaf {
    fn add_noise_to_agg_share(
        &self,
        _dp_strategy: &NoDifferentialPrivacy,
        _agg_param: &Self::AggregationParam,
        _agg_share: &mut Self::AggregateShare,
        _num_measurements: usize,
    ) -> Result<(), prio::vdaf::VdafError> {
        Ok(())
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

#[cfg(feature = "fpvec_bounded_l2")]
impl<T, SPoly, SBlindPoly> TypeWithNoise<NoDifferentialPrivacy>
    for FixedPointBoundedL2VecSum<T, SPoly, SBlindPoly>
where
    T: Fixed + CompatibleFloat,
    SPoly: ParallelSumGadget<Field128, PolyEval<Field128>> + Eq + Clone + 'static,
    SBlindPoly: ParallelSumGadget<Field128, BlindPolyEval<Field128>> + Eq + Clone + 'static,
{
}

impl AggregatorWithNoise<16, 16, NoDifferentialPrivacy>
    for prio::vdaf::poplar1::Poplar1<PrgSha3, 16>
{
    fn add_noise_to_agg_share(
        &self,
        _dp_strategy: &NoDifferentialPrivacy,
        _agg_param: &Self::AggregationParam,
        _agg_share: &mut Self::AggregateShare,
        _num_measurements: usize,
    ) -> Result<(), prio::vdaf::VdafError> {
        Ok(())
    }
}

#[macro_export]
macro_rules! strategy_alias {
    (false, $DpStrategy:ident, $type:ty) => {};
    (true, $DpStrategy:ident, $type:ty) => {
        type $DpStrategy = $type;
    };
}

#[macro_export]
macro_rules! if_ident_exists {
    ($token:ident, true => $body1:tt, false => $body2:tt) => {
        $body1
    };
    (, true => $body1:tt, false => $body2:tt) => {
        $body2
    };
}

