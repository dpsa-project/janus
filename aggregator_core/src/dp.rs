

use prio::dp::{DifferentialPrivacyStrategy, distributions::ZCdpDiscreteGaussian};
use serde::{Serialize, Deserialize};

#[derive(Clone, Serialize, Deserialize)]
pub enum DpStrategyInstance {
    ZCdpDiscreteGaussian(ZCdpDiscreteGaussian)
}

impl DpStrategyInstance {
    
}


