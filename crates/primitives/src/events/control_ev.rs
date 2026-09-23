//! Control event carries the requests from the control plane to pause an orderbook or a service.

use crate::signature::Signature;
use crate::value::TimestampMs;
use alloy::primitives::Address;
use alloy::{
    primitives::{B256, keccak256},
    sol,
    sol_types::SolValue,
};
use serde::{Deserialize, Serialize};

/// Control events from the control plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlEvent {
    /// Pause event pauses a service or a match engine.
    Pause(OpMeta),
    /// Resume event resumes a service or a match engine.
    Resume(OpMeta),
    /// Kill event kills a service.
    Kill(OpMeta),
}

/// Metadata of operations from control plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpMeta {
    /// The service name in the cluster.
    service_name: String,
    /// The service instance id in the cluster.
    instance_id: u32,
    /// The host name, or the IP of the host.
    host: String,
    /// The timestamp filled at the control plane service.
    /// It works as an important fundamental block for the operation security.
    /// The timestamp should fall into a recent time window before activate the
    /// operation at service side.
    timestamp: TimestampMs,
    /// The operator's account address.
    operator: Address,
    /// The signature generated from the operator's key when signing the message's hash.
    signature: Signature,
}

sol! {
    /// ABI encoded payload for OpMeta.
    struct OpMetaHashPayload {
        string service_name;
        uint32 instance_id;
        string host;
        uint64 timestamp;
        address operator;
    }
}

impl OpMeta {
    pub fn new(
        service_name: String,
        instance_id: u32,
        host: String,
        timestamp: TimestampMs,
        operator: Address,
        signature: Signature,
    ) -> Self {
        Self { service_name, instance_id, host, timestamp, operator, signature }
    }

    /// hash computes the Keccak-256 hash of the metadata and returns it as [`B256`].
    ///
    /// The digest is `keccak256(abi.encode(payload))` — the ABI encoding of
    /// [`OpMetaHashPayload`] including the single-argument offset word, i.e. the
    /// same bytes a Solidity `abi.encode` produces. Signature generation and
    /// verification must use exactly this encoding; the signature attests the
    /// digest and is deliberately not part of it.
    pub fn hash(&self) -> B256 {
        let payload = OpMetaHashPayload {
            service_name: self.service_name.clone(),
            instance_id: self.instance_id,
            host: self.host.clone(),
            timestamp: self.timestamp.0,
            operator: self.operator,
        };

        keccak256(payload.abi_encode())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::keccak256;

    // ---------------------------------------------------------------
    // OpMeta
    // ---------------------------------------------------------------

    fn sample_op_meta(seed: u8) -> OpMeta {
        OpMeta::new(
            format!("svd-oms-{seed}"),
            u32::from(seed),
            format!("host-{seed}.dex.local"),
            TimestampMs(u64::from(seed) * 1_000),
            Address::new([seed; 20]),
            Signature([seed; 65]),
        )
    }

    /// Encodes a single [`OpMetaHashPayload`] the way Solidity's `abi.encode`
    /// does: one offset word for the dynamic struct, then the field tuple with
    /// its offsets relative to the tuple start. This verifies [`OpMeta::hash`]
    /// independently of alloy's `sol!` machinery.
    fn abi_encode_meta_payload(
        service_name: &str,
        instance_id: u32,
        host: &str,
        timestamp: u64,
        operator: [u8; 20],
    ) -> Vec<u8> {
        fn word(bytes: &mut Vec<u8>, value: u64) {
            // Right-align the value in a 32-byte word.
            bytes.extend_from_slice(&[0u8; 24]);
            bytes.extend_from_slice(&value.to_be_bytes());
        }

        fn tail(bytes: &mut Vec<u8>, s: &str) {
            // The length word, then the data right-padded to a whole word.
            word(bytes, s.len() as u64);
            let padded_len = s.len().div_ceil(32) * 32;
            let mut data = s.as_bytes().to_vec();
            data.resize(padded_len, 0);
            bytes.extend_from_slice(&data);
        }

        const HEAD_WORDS: usize = 5;
        let service_tail_offset = HEAD_WORDS * 32;
        let host_tail_offset = service_tail_offset + 32 + service_name.len().div_ceil(32) * 32;

        let mut tuple = Vec::with_capacity(host_tail_offset + 32 + host.len().div_ceil(32) * 32);
        // Offsets to the string tails, then the static fields in declaration order.
        word(&mut tuple, service_tail_offset as u64);
        word(&mut tuple, u64::from(instance_id));
        word(&mut tuple, host_tail_offset as u64);
        word(&mut tuple, timestamp);
        // The operator address, left-padded to a word.
        tuple.extend_from_slice(&[0u8; 12]);
        tuple.extend_from_slice(&operator);
        tail(&mut tuple, service_name);
        tail(&mut tuple, host);

        // abi.encode of a single dynamic struct prefixes the offset word.
        let mut bytes = Vec::with_capacity(32 + tuple.len());
        word(&mut bytes, 32);
        bytes.extend_from_slice(&tuple);
        bytes
    }

    #[test]
    fn test_op_meta_hash_matches_manual_abi_encoding() {
        // Fields that fit a single word.
        let meta = sample_op_meta(1);
        let expected = keccak256(abi_encode_meta_payload(
            "svd-oms-1",
            1,
            "host-1.dex.local",
            1_000,
            [1u8; 20],
        ));
        assert_eq!(meta.hash(), expected);

        // A host longer than one word exercises the multi-word tail padding.
        let long_host = "very-long-host-name-that-spans-multiple-abi-words.example.com";
        assert!(long_host.len() > 32);
        let meta = OpMeta::new(
            "svd-settle".to_string(),
            7,
            long_host.to_string(),
            TimestampMs(9_000),
            Address::new([9u8; 20]),
            Signature([8u8; 65]),
        );
        let expected =
            keccak256(abi_encode_meta_payload("svd-settle", 7, long_host, 9_000, [9u8; 20]));
        assert_eq!(meta.hash(), expected);
    }

    #[test]
    fn test_op_meta_hash_sensitive_to_each_payload_field() {
        let base = sample_op_meta(1);
        let base_hash = base.hash();

        let other_service = OpMeta::new(
            "other".to_string(),
            1,
            "host-1.dex.local".to_string(),
            TimestampMs(1_000),
            Address::new([1u8; 20]),
            Signature([1u8; 65]),
        );
        assert_ne!(base_hash, other_service.hash());

        let other_instance = OpMeta::new(
            "svd-oms-1".to_string(),
            2,
            "host-1.dex.local".to_string(),
            TimestampMs(1_000),
            Address::new([1u8; 20]),
            Signature([1u8; 65]),
        );
        assert_ne!(base_hash, other_instance.hash());

        let other_host = OpMeta::new(
            "svd-oms-1".to_string(),
            1,
            "host-2.dex.local".to_string(),
            TimestampMs(1_000),
            Address::new([1u8; 20]),
            Signature([1u8; 65]),
        );
        assert_ne!(base_hash, other_host.hash());

        let other_timestamp = OpMeta::new(
            "svd-oms-1".to_string(),
            1,
            "host-1.dex.local".to_string(),
            TimestampMs(2_000),
            Address::new([1u8; 20]),
            Signature([1u8; 65]),
        );
        assert_ne!(base_hash, other_timestamp.hash());

        let other_operator = OpMeta::new(
            "svd-oms-1".to_string(),
            1,
            "host-1.dex.local".to_string(),
            TimestampMs(1_000),
            Address::new([2u8; 20]),
            Signature([1u8; 65]),
        );
        assert_ne!(base_hash, other_operator.hash());
    }

    #[test]
    fn test_op_meta_hash_excludes_signature() {
        let signed_a = OpMeta::new(
            "svd-oms-1".to_string(),
            1,
            "host-1.dex.local".to_string(),
            TimestampMs(1_000),
            Address::new([1u8; 20]),
            Signature([1u8; 65]),
        );
        let signed_b = OpMeta::new(
            "svd-oms-1".to_string(),
            1,
            "host-1.dex.local".to_string(),
            TimestampMs(1_000),
            Address::new([1u8; 20]),
            Signature([2u8; 65]),
        );
        // The signature attests the payload, it cannot be part of it.
        assert_eq!(signed_a.hash(), signed_b.hash());
    }

    #[test]
    fn test_op_meta_serde_roundtrip() {
        let meta = sample_op_meta(1);
        let bytes = rmp_serde::to_vec(&meta).unwrap();
        // The wire format is deterministic: re-serializing yields identical bytes.
        assert_eq!(rmp_serde::to_vec(&meta).unwrap(), bytes);
        let restored: OpMeta = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(rmp_serde::to_vec(&restored).unwrap(), bytes);
        assert_eq!(restored.hash(), meta.hash());
    }

    #[test]
    fn test_op_meta_serde_roundtrip_boundary_values() {
        let meta = OpMeta::new(
            "s".repeat(300),
            u32::MAX,
            "h".repeat(300),
            TimestampMs(u64::MAX),
            Address::new([0xff; 20]),
            Signature([0xaa; 65]),
        );
        let bytes = rmp_serde::to_vec(&meta).unwrap();
        let restored: OpMeta = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(rmp_serde::to_vec(&restored).unwrap(), bytes);
        assert_eq!(restored.hash(), meta.hash());
    }

    // ---------------------------------------------------------------
    // ControlEvent
    // ---------------------------------------------------------------

    #[test]
    fn test_control_event_serde_roundtrip() {
        for event in [
            ControlEvent::Pause(sample_op_meta(1)),
            ControlEvent::Resume(sample_op_meta(2)),
            ControlEvent::Kill(sample_op_meta(3)),
        ] {
            let bytes = rmp_serde::to_vec(&event).unwrap();
            let restored: ControlEvent = rmp_serde::from_slice(&bytes).unwrap();
            assert_eq!(rmp_serde::to_vec(&restored).unwrap(), bytes);
        }
    }

    #[test]
    fn test_control_event_variant_and_payload_survive_roundtrip() {
        let meta = sample_op_meta(4);
        let event = ControlEvent::Kill(meta.clone());
        let restored: ControlEvent =
            rmp_serde::from_slice(&rmp_serde::to_vec(&event).unwrap()).unwrap();
        let ControlEvent::Kill(restored_meta) = restored else {
            panic!("variant changed over the wire");
        };
        assert_eq!(restored_meta.hash(), meta.hash());
    }
}
