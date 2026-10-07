//! Shipping physical plans to another process the way a distributed executor
//! does: encode with datafusion-proto, then decode on a session that has only
//! the table's object store.
#![cfg(feature = "proto")]
// Each test binary uses a subset of these helpers.
#![allow(dead_code)]

use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_iceberg::{object_store_url_for_location, IcebergPhysicalExtensionCodec};
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
};
use object_store::local::LocalFileSystem;

/// An executor session with only the store of the table at `location`
/// registered. These fixtures keep tables on an unprefixed `LocalFileSystem`.
pub fn executor(location: &str) -> SessionContext {
    let ctx = SessionContext::new();
    ctx.runtime_env().register_object_store(
        object_store_url_for_location(location).as_ref(),
        Arc::new(LocalFileSystem::new()),
    );
    ctx
}

/// Encode `plan` and decode it on `executor`. Call this before executing
/// `plan`: an executed plan carries runtime dynamic-filter state.
pub fn ship(plan: Arc<dyn ExecutionPlan>, executor: &SessionContext) -> Arc<dyn ExecutionPlan> {
    let bytes = physical_plan_to_bytes_with_extension_codec(plan, &IcebergPhysicalExtensionCodec)
        .expect("encode the plan");
    physical_plan_from_bytes_with_extension_codec(
        &bytes,
        &executor.task_ctx(),
        &IcebergPhysicalExtensionCodec,
    )
    .expect("decode the plan on the executor")
}

/// Plan `query` on `ctx`, ship it to an executor for the table at `location`,
/// and execute it there.
pub async fn execute_shipped(
    ctx: &SessionContext,
    query: &str,
    location: &str,
) -> Vec<RecordBatch> {
    let plan = ctx
        .sql(query)
        .await
        .expect("plan the query")
        .create_physical_plan()
        .await
        .expect("create the physical plan");
    let executor = executor(location);
    collect(ship(plan, &executor), executor.task_ctx())
        .await
        .expect("execute the shipped plan")
}
