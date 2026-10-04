//! `PhysicalExtensionCodec` for plans produced by this crate.

use std::sync::Arc;

use datafusion::common::{internal_err, not_impl_err, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{PhysicalExtensionCodec, PhysicalProtoConverterExtension};

use crate::table::expr_adapter::IcebergPhysicalExprAdapterFactory;

/// Payload for `IcebergPhysicalExprAdapterFactory`. The factory is stateless, so
/// the versioned tag is the whole encoding; a future stateful version gets a new
/// tag and carries its state after it.
const FIELD_ID_ADAPTER_V1: &[u8] = b"datafusion_iceberg/field-id-adapter/v1";

/// Serializes the parts of `datafusion_iceberg` physical plans that
/// `datafusion-proto` cannot: currently the field-id expression adapter attached
/// to every Iceberg file scan. Register it with whatever serializes plans
/// (directly, or inside a `ComposedPhysicalExtensionCodec`).
#[derive(Debug, Default, Clone, Copy)]
pub struct IcebergPhysicalExtensionCodec;

impl PhysicalExtensionCodec for IcebergPhysicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("IcebergPhysicalExtensionCodec does not decode plan nodes")
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        // Name the node: this is the error a user sees when a plan contains
        // e.g. `IcebergDvExec`, which nothing can serialize yet.
        not_impl_err!(
            "IcebergPhysicalExtensionCodec does not encode plan node {}",
            node.name()
        )
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
}
