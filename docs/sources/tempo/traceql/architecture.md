---
title: How TraceQL works
menuTitle: How TraceQL works
description: Learn about how TraceQL works
weight: 300
draft: true
aliases:
  - /docs/tempo/latest/traceql/architecture
keywords:
  - Tempo query language
  - Architecture
  - TraceQL
---

<!-- This page is now hidden. It's very out of date. We need to either update or delete it. -->

# How TraceQL works

The TraceQL engine connects the Tempo API handler with the storage layer. The TraceQL engine:

- Parses incoming requests and extract flattened conditions the storage layer can work with
- Pulls spansets from the storage layer and revalidates that the query matches each span
- Returns the search response

The default Tempo search reviews the whole trace. TraceQL provides a method for formulating precise queries so you can zoom in to the data you need. Query results are returned faster because the queries limit what's searched.

For a deeper look at TraceQL, read the [TraceQL: A first-of-its-kind query language to accelerate trace analysis in Tempo 2.0](/blog/2022/11/30/traceql-a-first-of-its-kind-query-language-to-accelerate-trace-analysis-in-tempo-2.0/) blog post.

For examples of query syntax, refer to [Construct a TraceQL query](https://grafana.com/docs/tempo/<TEMPO_VERSION>/traceql/construct-a-traceql-query).

{{< vimeo 773194063 >}}

## Active development and limitations

TraceQL will be implemented in phases. The initial iteration of the TraceQL engine includes spanset selection and pipelines.

For more information about TraceQL’s design, refer to the [TraceQL extensions](https://github.com/grafana/tempo/blob/main/docs/design-proposals/2023-11%20TraceQL%20Extensions.md) abd [TraceQL Concepts](https://github.com/grafana/tempo/blob/main/docs/design-proposals/2022-04%20TraceQL%20Concepts.md) design proposals.

