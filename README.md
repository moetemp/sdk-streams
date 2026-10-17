# sdk-streams

Internal and experimental.

`temporalio-streams` is the stream runtime for Temporal SDKs. A Workflow, an Activity or a client
publishes records to a stream in a store it configures, and readers follow the stream. The SDKs
link this crate through their bridges. Temporal Core holds no stream code.

The crate and its build and test instructions land through the first pull requests.
