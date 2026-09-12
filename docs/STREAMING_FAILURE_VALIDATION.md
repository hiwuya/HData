# Streaming failure validation

This document records what the automated suite proves for long-running Kafka and CDC paths, and the
failure tests that still require a controlled runner environment. A passing DirectRunner test must not be
read as evidence that a remote worker has recovered from a process or network failure.

## Automated evidence

| Path | Failure or recovery scenario | Test | Evidence and boundary |
|---|---|---|---|
| Kafka source | A replacement worker resumes from a restored SDF position. | `KafkaPipelineTest.a resumed descriptor emits only records after the checkpoint position` | Runs Beam's `ReadFromKafkaDoFn` with Kafka's `MockConsumer`; it proves that the restored descriptor seeks to the next offset without replaying already checkpointed records. It does not persist a DirectRunner checkpoint or replace a remote worker. |
| Kafka sink | The broker may accept a record but its acknowledgement is lost during a network interruption. | `KafkaWriteFnTest.an ambiguous broker acknowledgement exposes a duplicate retry boundary` | Kafka's `MockProducer` records the send then fails its future. The row is routed to the dead letter stream for retry, while the broker history contains it once. Retrying may therefore duplicate the record, as required by the `AT_LEAST_ONCE` contract. |
| Kafka sink | The broker is unreachable before a send is accepted. | `KafkaWriteFnTest.a synchronous throw from send also goes to dead letter instead of killing the whole bundle` | A synchronous producer failure preserves the original row in dead letter output. |
| Debezium source | A worker exits without running teardown and a replacement must take over durable state. | `OffsetLeaseTest.a stale lease can be taken over without any explicit release` and `OffsetLeaseTest.an owner that lost its lease to a takeover discovers it on the next heartbeat` | The file lease expires after missed heartbeats, so a crashed local worker cannot permanently block a replacement. The mechanism is same-host and same-filesystem only. |
| Debezium source | A clean restart resumes from persisted offsets without replay. | `DebeziumRecoveryTest.a restart against the same offset file resumes instead of replaying` | Runs the embedded engine and its file offset store with Debezium's test connector. Two independent pipelines emit IDs 1–2 and then 3–5. |
| Debezium source | A completed real MySQL snapshot restarts into binlog streaming without another snapshot. | `DebeziumMySqlContainerIT.a restart resumes streaming after a completed snapshot, without re-snapshotting` | Optional Testcontainers qualification: the restarted pipeline sees only rows inserted after the initial snapshot. |
| SQS source | A worker disappears after emitting a message without deleting its receipt. | `SQSContainerIT.a non-deleting read is redelivered after a worker restart` | Optional LocalStack qualification runs two independent pipelines with `delete_after_read: false`; both receive the same message. This is the intended at-least-once mode. |
| RabbitMQ source | A broker auto-acknowledges a pulled message. | `RabbitMQReadProvider.deliveryCapabilities` and `RabbitMQReadFn` | Reads use `basicGet(..., true)`, so recovery is deliberately **not** claimed: a crash after the broker acknowledgement can lose the message. |

## Delivery and duplicate rules

`ReadFromKafka` in unbounded mode is resumable from runner checkpoint state. Ordering is per Kafka
partition, and a record delivered after the last completed checkpoint can be replayed after a worker
failure. `commit_offsets_on_checkpoint` mirrors progress to a consumer group but is not the source of
HData's recovery state.

`WriteToKafka` with `sink_delivery_guarantee: at-least-once` uses `acks=all` and requires an idempotency
key. A lost acknowledgement is inherently ambiguous: retrying protects against loss but may write a
duplicate. Use a key-compacted topic, an idempotent consumer, or a downstream deduplication key. The
`none` mode is at-most-once and can lose an unacknowledged record.

`ReadFromDebezium` is at-least-once. A hard crash after events are emitted but before the next
`offset.flush.interval.ms` flush replays that interval on restart; it must not skip the unflushed events.
Use the CDC record key as the sink idempotency or deduplication key. Details, including snapshot behavior,
are in [connectors.md](connectors.md#debezium).

`ReadFromSQS` has two distinct choices. `delete_after_read: true` deletes a receipt immediately after
emitting it and is at-most-once; a worker failure after that deletion can lose a message. Set
`delete_after_read: false` for at-least-once delivery and accept redelivery after the queue visibility
timeout. `ReadFromRabbitMQ` always uses auto-ack and is at-most-once. It is unsuitable where a worker
crash must preserve a message. Pulsar's current connector is a bounded snapshot and has no durable
subscription or continuous-recovery contract.

## Controlled qualification still required

Before promoting either connector beyond experimental, retain evidence from a manually triggered Flink
or Spark job that performs all of the following against a real broker or database:

1. Start an unbounded job, wait for at least one completed checkpoint, terminate a worker, and verify that
   processing resumes without gaps; permit records after the last checkpoint to replay.
2. Interrupt and restore the worker-to-service network while records are in flight; verify that source
   failures are observable and sink failures retain replayable dead-letter rows.
3. For each at-least-once sink, replay an ambiguous acknowledgement and verify the documented
   idempotency or deduplication strategy removes the duplicate business effect.
4. Save the job configuration, runner version, service image or version, checkpoint logs, and test report
   with the qualification run as required by [CONNECTOR_CONTRACT.md](CONNECTOR_CONTRACT.md).

The repository workflows are manual by policy. Use the **Connector Contract Qualification** workflow for
the Testcontainers portion and the runner-profile workflow to compile and verify the runner profile; a
remote failure exercise needs the corresponding controlled runner cluster.
