#### Prisma Connector Prototype
Development
##### Overview

The prisma connector prototype is an instrumentation pipeline collector and processor combined into one. It will transform incoming data from the ingest pubsub topic to a `cloud_event` following the structure of the [fourkeys repository](https://github.com/GoogleCloudPlatform/fourkeys).

It is an application that runs on Cloud Run and listens to incoming pubsub messages.

Like the bq-worker from the fourkeys repository, the prisma connector inserts the transformed payload to one source of truth table in BigQuery (the name of the table has still not yet been decided). But for now we have [this](https://console.cloud.google.com/bigquery?referrer=search&project=off-net-dev&ws=!1m5!1m4!4m3!1soff-net-dev!2sprisma!3sevents_raw3) table as our test subject.

*Note: The link to the test table is only accessible to those part of `off-net-dev` project.*

The derived tables made from the source table follows how [`iof-secops`](https://github.com/telus/iof-secops/blob/main/applications/data-transform-and-load/lib/prisma_image_scan.py) constructed their columns for [`vulnerabilities`](https://console.cloud.google.com/bigquery?referrer=search&project=off-net-dev&ws=!1m5!1m4!4m3!1soff-net-dev!2sprisma!3sprisma-derived-vulnerabilities) and [`compliances`](https://console.cloud.google.com/bigquery?referrer=search&project=off-net-dev&ws=!1m5!1m4!4m3!1soff-net-dev!2sprisma!3sprisma-derived-compliances)

*Note: The link to the derived table is only accessible to those part of `off-net-dev` project.*

##### Testing

As you might have noticed the repository right now contains more than one json file. You need not worry about it since these are just used for testing. The most prominent would be the `pubsub-payload-from-ingest-topic.json` file. This file contains the mock data that the ingest pubsub topic should send a post request to this server.

For now there is no automated tests yet as we are still working on it.