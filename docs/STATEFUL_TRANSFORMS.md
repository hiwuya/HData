# Stateful transform admission rules

The built-in transforms are currently stateless. They may be fused, retried, or parallelized by a
runner without retaining application state. This is intentional: no transform currently promises a
windowed aggregate, deduplication store, join buffer, timer, or other persistent state contract.

Before adding any stateful transform, its provider documentation, configuration, and tests must
explicitly declare all of the following:

1. **Keying:** the row fields used as the state key, their null behavior, and cardinality limits.
2. **Time:** event-time field, timestamp conversion, watermark assumptions, allowed lateness, and
   trigger/pane behavior.
3. **State:** state type, TTL/retention, cleanup timer, storage growth bound, and behavior when
   state is missing after a deployment/recovery.
4. **Recovery:** checkpoint/replay behavior, duplicate/late record semantics, and sink idempotency
   requirements. The declared `DeliveryCapabilities` must remain true through the transform.
5. **Runner support:** a DirectRunner behavior test plus Flink and Spark evidence before the transform
   is advertised for those runners. A test must exercise restore/replay, not merely normal output.

A transform that cannot state these rules stays stateless. Do not model cross-record state with
ordinary mutable fields in a DoFn: Beam can serialize, clone, retry, and distribute it arbitrarily.
