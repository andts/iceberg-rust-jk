pub mod catalog;
#[cfg(feature = "proto")]
pub mod codec;
pub mod error;
pub mod materialized_view;
mod partition_projection;
mod partition_value;
pub mod planner;
mod pruning_statistics;
mod statistics;
pub mod table;

#[cfg(feature = "proto")]
pub use crate::codec::IcebergPhysicalExtensionCodec;
pub use crate::table::{object_store_url_for_location, DataFusionTable};
