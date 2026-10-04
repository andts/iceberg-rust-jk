//! `PhysicalExtensionCodec` for plans produced by this crate.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::ScalarValue;
use datafusion::common::{internal_datafusion_err, internal_err, not_impl_err, Result};
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{PhysicalExtensionCodec, PhysicalProtoConverterExtension};
use iceberg_rust::spec::deletion_vector::DeletionVector;
use iceberg_rust::spec::util;

use crate::table::dv_exec::IcebergDvExec;
use crate::table::expr_adapter::IcebergPhysicalExprAdapterFactory;

/// Payload for `IcebergPhysicalExprAdapterFactory`. The factory is stateless, so
/// the versioned tag is the whole encoding; a future stateful version gets a new
/// tag and carries its state after it.
const FIELD_ID_ADAPTER_V1: &[u8] = b"datafusion_iceberg/field-id-adapter/v1";

/// Payload tag for `IcebergDvExec`. Layout after the tag (big-endian):
/// path column name, row-number column name (each `u32` length + UTF-8),
/// strip flag (`u8`), entry count (`u32`), then per entry the normalized
/// data-file path and its `deletion-vector-v1` blob (each `u32` length +
/// bytes), sorted by path.
// The NUL ends the tag, so a later tag that starts with this one (`v10`,
// `v1-lazy`) is foreign rather than a malformed v1 payload.
const DV_EXEC_V1: &[u8] = b"datafusion_iceberg/dv-exec/v1\0";

/// Serializes the parts of `datafusion_iceberg` physical plans that
/// `datafusion-proto` cannot: the field-id expression adapter attached to every
/// Iceberg file scan, and `IcebergDvExec`, which applies row-level deletes (each
/// plan carries only the deletion vectors of the files it scans). Register it
/// with whatever serializes plans (directly, or inside a
/// `ComposedPhysicalExtensionCodec`).
#[derive(Debug, Default, Clone, Copy)]
pub struct IcebergPhysicalExtensionCodec;

impl PhysicalExtensionCodec for IcebergPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Unknown payloads are an internal error: `ComposedPhysicalExtensionCodec`
        // routes a payload back only to the codec that wrote it.
        let Some(payload) = buf.strip_prefix(DV_EXEC_V1) else {
            return internal_err!(
                "unknown datafusion_iceberg plan payload ({} bytes)",
                buf.len()
            );
        };
        let [input] = inputs else {
            return internal_err!("IcebergDvExec takes one input, got {}", inputs.len());
        };
        decode_dv_exec(payload, input)
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        match node.downcast_ref::<IcebergDvExec>() {
            Some(exec) => encode_dv_exec(exec, buf),
            None => not_impl_err!(
                "IcebergPhysicalExtensionCodec does not encode plan node {}",
                node.name()
            ),
        }
    }

    fn try_encode_expr_adapter_factory(
        &self,
        factory: &Arc<dyn PhysicalExprAdapterFactory>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        if factory.is::<IcebergPhysicalExprAdapterFactory>() {
            buf.extend_from_slice(FIELD_ID_ADAPTER_V1);
            Ok(())
        } else {
            not_impl_err!("IcebergPhysicalExtensionCodec does not encode {factory:?}")
        }
    }

    // Unknown payloads are an internal error, not "not implemented":
    // `ComposedPhysicalExtensionCodec` routes a payload back only to the codec
    // that produced it.
    fn try_decode_expr_adapter_factory(
        &self,
        buf: &[u8],
    ) -> Result<Arc<dyn PhysicalExprAdapterFactory>> {
        match buf {
            FIELD_ID_ADAPTER_V1 => Ok(Arc::new(IcebergPhysicalExprAdapterFactory)),
            _ => internal_err!(
                "unknown datafusion_iceberg adapter payload ({} bytes)",
                buf.len()
            ),
        }
    }
}

fn encode_dv_exec(exec: &IcebergDvExec, buf: &mut Vec<u8>) -> Result<()> {
    // Build the payload apart from `buf` so a failed encode writes nothing.
    let mut out = DV_EXEC_V1.to_vec();
    put_bytes(&mut out, exec.path_column_name().as_bytes())?;
    put_bytes(&mut out, exec.row_number_column_name().as_bytes())?;
    out.push(u8::from(exec.strip_path_col()));
    let entries = shipped_entries(exec);
    put_len(&mut out, entries.len())?;
    for (path, dv) in entries {
        put_bytes(&mut out, path.as_bytes())?;
        let blob = dv.to_bytes().map_err(|e| {
            internal_datafusion_err!("IcebergDvExec: encoding the deletion vector of {path}: {e}")
        })?;
        put_bytes(&mut out, &blob)?;
    }
    buf.extend_from_slice(&out);
    Ok(())
}

/// The deletion vectors `exec` ships, sorted by path: those of the data files
/// its child scans. An engine that splits a scan into tasks serializes each
/// task's plan separately, so this keeps the shipped bytes close to the deletes
/// actually applied. If the scanned files can't be determined, all are shipped.
///
/// Looks up the scanned files rather than filtering the whole map, so encoding
/// every task of a split scan costs the number of files, not tasks × deletes.
fn shipped_entries(exec: &IcebergDvExec) -> Vec<(&String, &DeletionVector)> {
    let dvs = exec.dvs();
    let mut entries: Vec<_> = match scanned_data_files(exec.input(), &exec.path_column_name()) {
        Some(files) => files
            .iter()
            .filter_map(|path| dvs.get_key_value(path))
            .collect(),
        None => dvs.iter().collect(),
    };
    entries.sort_by(|a, b| a.0.cmp(b.0));
    entries
}

/// Normalized paths of the data files `plan` scans, read from the
/// `path_column` partition value of each file and normalized exactly as
/// `IcebergDvExec` does at lookup. `None` when that can't be determined.
fn scanned_data_files(plan: &Arc<dyn ExecutionPlan>, path_column: &str) -> Option<HashSet<String>> {
    let mut files = HashSet::new();
    let mut found_scan = false;
    let mut complete = true;
    plan.apply(|node| {
        let Some(config) = node
            .downcast_ref::<DataSourceExec>()
            .and_then(|exec| exec.data_source().downcast_ref::<FileScanConfig>())
        else {
            // Any other leaf (e.g. an engine's stage reader) produces rows of
            // files we can't name, so nothing may be pruned.
            if node.children().is_empty() {
                complete = false;
            }
            return Ok(TreeNodeRecursion::Continue);
        };
        found_scan = true;
        let Some(idx) = config
            .table_partition_cols()
            .iter()
            .position(|field| field.name() == path_column)
        else {
            complete = false;
            return Ok(TreeNodeRecursion::Continue);
        };
        for file in config.file_groups.iter().flat_map(|group| group.iter()) {
            match file.partition_values.get(idx) {
                Some(ScalarValue::Utf8(Some(path))) => {
                    files.insert(util::strip_prefix(path));
                }
                _ => complete = false,
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .ok()?;
    (found_scan && complete).then_some(files)
}

fn decode_dv_exec(
    payload: &[u8],
    input: &Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut reader = PayloadReader(payload);
    let path_column = reader.string()?;
    let row_number_column = reader.string()?;
    let strip_path_col = match reader.u8()? {
        0 => false,
        1 => true,
        flag => return internal_err!("IcebergDvExec payload: invalid strip flag {flag}"),
    };
    let count = reader.len()?;
    let dvs = (0..count)
        .map(|_| {
            let path = reader.string()?;
            let dv = DeletionVector::try_from(reader.bytes()?).map_err(|e| {
                internal_datafusion_err!("IcebergDvExec payload: deletion vector of {path}: {e}")
            })?;
            Ok((path, dv))
        })
        .collect::<Result<HashMap<_, _>>>()?;
    if !reader.0.is_empty() {
        return internal_err!("IcebergDvExec payload: {} trailing bytes", reader.0.len());
    }
    Ok(Arc::new(IcebergDvExec::try_new(
        Arc::clone(input),
        Arc::new(dvs),
        &path_column,
        &row_number_column,
        strip_path_col,
    )?))
}

fn put_len(out: &mut Vec<u8>, len: usize) -> Result<()> {
    let len = u32::try_from(len).map_err(|_| {
        internal_datafusion_err!("IcebergDvExec payload field of {len} exceeds u32")
    })?;
    out.extend_from_slice(&len.to_be_bytes());
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    put_len(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

/// Reads the `IcebergDvExec` payload; every read fails, rather than panics,
/// on a truncated payload.
struct PayloadReader<'a>(&'a [u8]);

impl<'a> PayloadReader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() < n {
            return internal_err!("IcebergDvExec payload is truncated");
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn len(&mut self) -> Result<usize> {
        let bytes: [u8; 4] = self.take(4)?.try_into().expect("took 4 bytes");
        Ok(u32::from_be_bytes(bytes) as usize)
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.len()?;
        self.take(n)
    }

    fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?.to_vec())
            .map_err(|e| internal_datafusion_err!("IcebergDvExec payload: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::Schema;
    use datafusion::common::DataFusionError;
    use datafusion::physical_expr_adapter::{
        DefaultPhysicalExprAdapterFactory, PhysicalExprAdapterFactory,
    };
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_proto::physical_plan::{DefaultPhysicalProtoConverter, PhysicalExtensionCodec};

    use super::IcebergPhysicalExtensionCodec;
    use crate::table::expr_adapter::IcebergPhysicalExprAdapterFactory;
    use std::collections::HashMap;

    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::execution_plan::reset_plan_states;

    use crate::table::dv_exec::IcebergDvExec;
    use crate::table::dv_fixture::{dv_entry, dv_exec, int64_values, DvFixture};
    use datafusion::prelude::SessionContext;
    use datafusion_proto::bytes::{
        physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
    };

    use crate::table::dv_fixture::v_at_least;
    use datafusion::common::ScalarValue;

    const F1: &str = "data/f1.parquet";

    /// Encode `plan` with the codec and decode it over its own children, the
    /// way the proto converter calls the codec (children are decoded first).
    /// The children's execution state is reset, as decoding builds fresh ones:
    /// a `DataSourceExec` hands its files out once per state.
    fn reencode(
        plan: Arc<dyn ExecutionPlan>,
        ctx: &TaskContext,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let mut buf = Vec::new();
        IcebergPhysicalExtensionCodec.try_encode(
            plan.clone(),
            &mut buf,
            &DefaultPhysicalProtoConverter {},
        )?;
        let inputs = plan
            .children()
            .into_iter()
            .map(|child| reset_plan_states(Arc::clone(child)))
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        IcebergPhysicalExtensionCodec.try_decode(
            &buf,
            &inputs,
            ctx,
            &DefaultPhysicalProtoConverter {},
        )
    }

    #[test]
    fn field_id_adapter_round_trips() {
        let codec = IcebergPhysicalExtensionCodec;
        let factory: Arc<dyn PhysicalExprAdapterFactory> =
            Arc::new(IcebergPhysicalExprAdapterFactory);
        let mut buf = Vec::new();
        codec
            .try_encode_expr_adapter_factory(&factory, &mut buf)
            .unwrap();
        let decoded = codec.try_decode_expr_adapter_factory(&buf).unwrap();
        assert!(decoded.is::<IcebergPhysicalExprAdapterFactory>());
    }

    /// Declining with `NotImplemented` is what lets
    /// `ComposedPhysicalExtensionCodec` try the next codec.
    #[test]
    fn other_adapters_are_declined_as_not_implemented() {
        let factory: Arc<dyn PhysicalExprAdapterFactory> =
            Arc::new(DefaultPhysicalExprAdapterFactory);
        let mut buf = Vec::new();
        let err = IcebergPhysicalExtensionCodec
            .try_encode_expr_adapter_factory(&factory, &mut buf)
            .unwrap_err();
        assert!(matches!(err, DataFusionError::NotImplemented(_)), "{err}");
        assert!(buf.is_empty(), "a declined encode must not write");
    }

    #[test]
    fn unknown_payloads_are_rejected() {
        for payload in [
            &b""[..],
            b"datafusion_iceberg/field-id-adapter/v2",
            b"datafusion_iceberg/field-id-adapter/v1\0",
        ] {
            assert!(
                IcebergPhysicalExtensionCodec
                    .try_decode_expr_adapter_factory(payload)
                    .is_err(),
                "payload {payload:?} must not decode"
            );
        }
    }

    #[test]
    fn plan_nodes_are_declined_by_name() {
        let node: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let err = IcebergPhysicalExtensionCodec
            .try_encode(node, &mut Vec::new(), &DefaultPhysicalProtoConverter {})
            .unwrap_err();
        assert!(matches!(err, DataFusionError::NotImplemented(_)), "{err}");
        assert!(err.to_string().contains("EmptyExec"), "{err}");
    }

    #[tokio::test]
    async fn dv_exec_round_trips_and_deletes_by_position() {
        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30, 40, 50, 60, 70])]).await;
        let plan = dv_exec(
            fixture.scan(vec![fixture.file(F1)], None).await,
            HashMap::from([dv_entry(F1, &[1, 5])]),
            true,
        );
        let task_ctx = fixture.ctx.task_ctx();
        let decoded = reencode(plan.clone(), &task_ctx).unwrap();
        assert_eq!(decoded.schema(), plan.schema());

        let here = int64_values(&collect(plan, task_ctx.clone()).await.unwrap(), "v");
        let shipped = int64_values(&collect(decoded, task_ctx).await.unwrap(), "v");
        assert_eq!(here, vec![0, 20, 30, 40, 60, 70]);
        assert_eq!(shipped, here);
    }

    /// A user who opted in to `__data_file_path` keeps it after decoding.
    #[tokio::test]
    async fn dv_exec_keeps_the_path_column_when_not_stripped() {
        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30])]).await;
        let plan = dv_exec(
            fixture.scan(vec![fixture.file(F1)], None).await,
            HashMap::from([dv_entry(F1, &[0])]),
            false,
        );
        let decoded = reencode(plan.clone(), &fixture.ctx.task_ctx()).unwrap();
        assert_eq!(decoded.schema().fields().len(), 2, "v and the path column");
        assert_eq!(decoded.schema(), plan.schema());
    }

    #[tokio::test]
    async fn malformed_dv_exec_payloads_are_rejected() {
        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30])]).await;
        let child = fixture.scan(vec![fixture.file(F1)], None).await;
        let plan = dv_exec(child.clone(), HashMap::from([dv_entry(F1, &[0])]), true);
        let mut buf = Vec::new();
        IcebergPhysicalExtensionCodec
            .try_encode(plan, &mut buf, &DefaultPhysicalProtoConverter {})
            .unwrap();

        let task_ctx = fixture.ctx.task_ctx();
        let decode = |payload: &[u8], inputs: &[Arc<dyn ExecutionPlan>]| {
            IcebergPhysicalExtensionCodec.try_decode(
                payload,
                inputs,
                &task_ctx,
                &DefaultPhysicalProtoConverter {},
            )
        };
        let one_input = std::slice::from_ref(&child);
        assert!(decode(&buf, one_input).is_ok(), "the valid payload");
        assert!(decode(&buf, &[]).is_err(), "no input");
        assert!(
            decode(&buf, &[child.clone(), child.clone()]).is_err(),
            "two inputs"
        );
        assert!(
            decode(&buf[..buf.len() - 1], one_input).is_err(),
            "truncated"
        );
        let mut trailing = buf.clone();
        trailing.push(0);
        assert!(decode(&trailing, one_input).is_err(), "trailing bytes");
        assert!(
            decode(b"datafusion_iceberg/dv-exec/v2", one_input).is_err(),
            "other version"
        );

        // Corruptions that keep the payload's shape and reach its own checks.
        // The strip flag follows the tag and the two length-prefixed column names.
        let flag_at = (0..2).fold(super::DV_EXEC_V1.len(), |at, _| {
            let len = u32::from_be_bytes(buf[at..at + 4].try_into().unwrap()) as usize;
            at + 4 + len
        });
        let mut bad_flag = buf.clone();
        bad_flag[flag_at] = 2;
        let err = decode(&bad_flag, one_input).unwrap_err();
        assert!(err.to_string().contains("invalid strip flag 2"), "{err}");
        // The last byte belongs to the last deletion-vector blob (its CRC):
        // same length, wrong contents.
        let mut bad_blob = buf.clone();
        *bad_blob.last_mut().unwrap() ^= 0xFF;
        let err = decode(&bad_blob, one_input).unwrap_err();
        assert!(err.to_string().contains("deletion vector of"), "{err}");
        assert!(decode(b"", &[child]).is_err(), "empty");
    }

    const F2: &str = "data/f2.parquet";

    /// The data-file paths whose deletion vectors `plan` ships.
    fn shipped_dv_paths(plan: Arc<dyn ExecutionPlan>, ctx: &TaskContext) -> Vec<String> {
        let decoded = reencode(plan, ctx).unwrap();
        let mut paths: Vec<_> = decoded
            .downcast_ref::<IcebergDvExec>()
            .unwrap()
            .dvs()
            .keys()
            .cloned()
            .collect();
        paths.sort();
        paths
    }

    #[tokio::test]
    async fn each_task_ships_only_the_deletes_of_its_files() {
        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30]), (F2, &[40, 50, 60, 70])]).await;
        let dvs = HashMap::from([dv_entry(F1, &[0]), dv_entry(F2, &[1])]);
        let ctx = fixture.ctx.task_ctx();

        let task1 = dv_exec(
            fixture.scan(vec![fixture.file(F1)], None).await,
            dvs.clone(),
            true,
        );
        let task2 = dv_exec(
            fixture.scan(vec![fixture.file(F2)], None).await,
            dvs.clone(),
            true,
        );
        let both = dv_exec(
            fixture
                .scan(vec![fixture.file(F1), fixture.file(F2)], None)
                .await,
            dvs,
            true,
        );
        assert_eq!(shipped_dv_paths(task1, &ctx), vec![F1]);
        assert_eq!(shipped_dv_paths(task2, &ctx), vec![F2]);
        assert_eq!(shipped_dv_paths(both, &ctx), vec![F1, F2]);
    }

    /// Two tasks reading one file by byte range both need its bitmap.
    #[tokio::test]
    async fn both_halves_of_a_split_file_ship_its_deletes() {
        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30, 40, 50, 60, 70]), (F2, &[80])]).await;
        let dvs = HashMap::from([dv_entry(F1, &[1, 5]), dv_entry(F2, &[0])]);
        let ctx = fixture.ctx.task_ctx();
        let (head, tail) = fixture.split_at_second_row_group(F1);
        for half in [head, tail] {
            let task = dv_exec(fixture.scan(vec![half], None).await, dvs.clone(), true);
            assert_eq!(shipped_dv_paths(task, &ctx), vec![F1]);
        }
    }

    /// When the files can't be determined, shipping everything is correct.
    #[tokio::test]
    async fn without_a_file_scan_every_delete_ships() {
        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30])]).await;
        let schema = fixture.scan(vec![fixture.file(F1)], None).await.schema();
        let task = dv_exec(
            Arc::new(EmptyExec::new(schema)),
            HashMap::from([dv_entry(F1, &[0]), dv_entry(F2, &[0])]),
            true,
        );
        assert_eq!(
            shipped_dv_paths(task, &fixture.ctx.task_ctx()),
            vec![F1, F2]
        );
    }
    /// Map keys are normalized (`strip_prefix`); partition values carry the
    /// path as stored in the manifest, scheme included.
    #[tokio::test]
    async fn pruning_normalizes_paths_like_the_lookup() {
        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30])]).await;
        let stored = format!("s3://bucket/{F1}");
        let mut file = fixture.file(F1);
        file.partition_values = vec![ScalarValue::Utf8(Some(stored.clone()))];
        let task = dv_exec(
            fixture.scan(vec![file], None).await,
            HashMap::from([dv_entry(&stored, &[0]), dv_entry(F2, &[0])]),
            true,
        );
        assert_eq!(
            shipped_dv_paths(task, &fixture.ctx.task_ctx()),
            vec![format!("/{F1}")]
        );
    }

    /// Ship `plan` through datafusion-proto to an executor session that has
    /// only the fixture's store, and execute it there.
    async fn execute_shipped(fixture: &DvFixture, plan: Arc<dyn ExecutionPlan>) -> Vec<i64> {
        let bytes =
            physical_plan_to_bytes_with_extension_codec(plan, &IcebergPhysicalExtensionCodec)
                .unwrap();
        let executor = SessionContext::new();
        executor
            .runtime_env()
            .register_object_store(fixture.url.as_ref(), fixture.store.clone());
        let decoded = physical_plan_from_bytes_with_extension_codec(
            &bytes,
            &executor.task_ctx(),
            &IcebergPhysicalExtensionCodec,
        )
        .unwrap();
        int64_values(&collect(decoded, executor.task_ctx()).await.unwrap(), "v")
    }

    /// Stands in for v3 deletion vectors: once loaded, they are the same map.
    #[tokio::test]
    async fn dv_exec_ships_with_predicate_pushdown() {
        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30, 40, 50, 60, 70])]).await;
        let plan = dv_exec(
            fixture
                .scan(vec![fixture.file(F1)], Some(v_at_least(40)))
                .await,
            HashMap::from([dv_entry(F1, &[5])]),
            true,
        );
        assert_eq!(execute_shipped(&fixture, plan).await, vec![40, 60, 70]);
    }

    /// One file split by byte range into two tasks, each shipped on its own:
    /// both apply deletes by absolute position, and together they return what
    /// one process does.
    #[tokio::test]
    async fn split_file_tasks_delete_by_absolute_position() {
        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30, 40, 50, 60, 70])]).await;
        let dvs = HashMap::from([dv_entry(F1, &[1, 5])]);
        let (head, tail) = fixture.split_at_second_row_group(F1);

        let head_rows = execute_shipped(
            &fixture,
            dv_exec(fixture.scan(vec![head], None).await, dvs.clone(), true),
        )
        .await;
        let tail_rows = execute_shipped(
            &fixture,
            dv_exec(fixture.scan(vec![tail], None).await, dvs.clone(), true),
        )
        .await;
        assert_eq!(
            head_rows,
            vec![0, 20, 30],
            "row group 0, position 1 deleted"
        );
        assert_eq!(
            tail_rows,
            vec![40, 60, 70],
            "row group 1, absolute position 5 deleted"
        );

        let whole = dv_exec(fixture.scan(vec![fixture.file(F1)], None).await, dvs, true);
        let here = int64_values(&collect(whole, fixture.ctx.task_ctx()).await.unwrap(), "v");
        assert_eq!([head_rows, tail_rows].concat(), here);
    }

    /// A tag that merely starts with the v1 tag (a later version) is foreign,
    /// not a v1 payload with odd contents.
    #[tokio::test]
    async fn later_dv_exec_versions_are_unknown_not_malformed_v1() {
        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30])]).await;
        let child = fixture.scan(vec![fixture.file(F1)], None).await;
        let err = IcebergPhysicalExtensionCodec
            .try_decode(
                b"datafusion_iceberg/dv-exec/v1-lazy\0payload",
                std::slice::from_ref(&child),
                &fixture.ctx.task_ctx(),
                &DefaultPhysicalProtoConverter {},
            )
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("unknown datafusion_iceberg plan payload"),
            "{err}"
        );
    }

    /// Rows may reach `IcebergDvExec` from a leaf that is not a file scan
    /// (e.g. an engine's stage reader next to a local scan); their files are
    /// unknown, so nothing may be pruned.
    #[tokio::test]
    async fn a_leaf_other_than_a_file_scan_ships_every_delete() {
        use datafusion::physical_plan::union::UnionExec;

        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30])]).await;
        let scan = fixture.scan(vec![fixture.file(F1)], None).await;
        let other_leaf: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(scan.schema()));
        let union = UnionExec::try_new(vec![scan, other_leaf]).unwrap();
        let task = dv_exec(
            union,
            HashMap::from([dv_entry(F1, &[0]), dv_entry(F2, &[0])]),
            true,
        );
        assert_eq!(
            shipped_dv_paths(task, &fixture.ctx.task_ctx()),
            vec![F1, F2]
        );
    }
}
