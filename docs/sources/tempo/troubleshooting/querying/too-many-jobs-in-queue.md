---
title: Too many jobs in the queue
description: Troubleshoot too many jobs in the queue
weight: 474
aliases:
- ../operations/troubleshooting/too-many-jobs-in-queue/ # https://grafana.com/docs/tempo/<TEMPO_VERSION>/operations/troubleshooting/too-many-jobs-in-queue/
- ../too-many-jobs-in-queue/ # https://grafana.com/docs/tempo/<TEMPO_VERSION>/troubleshooting/too-many-jobs-in-queue/
---

# Too many jobs in the queue

The error message might also be
- `queue doesn't have room for 100 jobs`
- `failed to add a job to work queue`

You may see this error if the compactor isn’t running and the blocklist size has exploded.
Possible reasons why the compactor may not be running are:

- Insufficient permissions.
- Compactor sitting idle because no block is hashing to it.
- Incorrect configuration settings.

## Diagnose the issue

- Check metric `tempodb_compaction_bytes_written_total`
If this is greater than zero (0), it means the compactor is running and writing to the backend.
- Check metric `tempodb_compaction_errors_total`
If this metric is greater than zero (0), check the logs of the compactor for an error message.

## Solutions

- Verify that the Compactor has the LIST, GET, PUT, and DELETE permissions on the bucket objects.
  - If these permissions are missing, assign them to the compactor container.
  - For detailed information, refer to the [Amazon S3 permissions](https://grafana.com/docs/tempo/<TEMPO_VERSION>/configuration/hosted-storage/s3/).
- If there’s a compactor sitting idle while others are running, port-forward to the compactor’s http endpoint. Then go to `/compactor/ring` and click **Forget** on the inactive compactor.
- Check the following configuration parameters to ensure that there are correct settings:
  - `max_block_bytes` to determine when the ingester cuts blocks. A good number is anywhere from 100MB to 2GB depending on the workload.
  - `max_compaction_objects` to determine the max number of objects in a compacted block. This should relatively high, generally in the millions.
  - `retention_duration` for how long traces should be retained in the backend.
- Check the storage section of the configuration and increase `queue_depth`. Do bear in mind that a deeper queue could mean longer
  waiting times for query responses. Adjust `max_workers` accordingly, which configures the number of parallel workers
  that query backend blocks.

```yaml
storage:
  trace:
    pool:
      max_workers: 100   # worker pool determines the number of parallel requests to the object store backend
      queue_depth: 10000
```
