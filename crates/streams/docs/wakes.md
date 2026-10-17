# Wakes for Workflow readers

A Workflow reader that finds nothing attaches to the stream's notifier as an idle callback, and
asks for one wake after the counter of its read. The notifier sends the wake to the server, and
the server puts it on the reader's next Workflow Task. A wake says that the stream moved, not what
it holds, so the reader reads the stream itself.

## Delivery

The server writes each wake as one entry of `WorkflowTaskScheduledEventAttributes.attachments`, a
`google.protobuf.Any` of `temporal.api.stream.v1.StreamWake` with the type URL
`type.googleapis.com/temporal.api.stream.v1.StreamWake` (`STREAM_WAKE_TYPE_URL`). Core gives lang
the attachments of one task as one `WorkflowTaskAttachments` job, in event order, in the task's
first activation. The job sorts after the task's signals, updates and resolutions, and before
Local Activity resolutions. It comes from History, so replay gives the same job at the same point.
Lang skips attachments of other types.

## Fold

Lang folds the wakes of one job per subscription with `fold_stream_wake`, and hands each reader
one wake, by these rules.

- The highest counter keeps its position and metadata. A lower or equal counter changes neither.
- A recheck, a close or a resubscribe on any of the folded wakes stays.
- The first close keeps its outcome.
- Subscriptions keep the order of their first wake.

`testdata/wake_folds.json` holds the cases. Each lang's fold must give the `folded` list of each
case from its `wakes` list. In the file, `close_result` is the data of the result payload and
`close_failure` is the failure's message. A missing field takes its proto default.

## Payload codec

The producer runs its payload codec and failure converter on `close_result` and `close_failure`
before it closes the stream, so a wake carries encoded values. Core does not look into an `Any`,
so its activation visitor does not decode them. Lang unpacks each attachment in its async decode
pass, before the Workflow sees the job, and decodes `close_result` with its payload codec and
`close_failure` with its failure converter. The fold runs on the decoded wakes inside activation
handling, where no codec call can run.
