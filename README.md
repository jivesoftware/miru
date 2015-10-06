![Miru](https://github.com/jivesoftware/miru/wiki/images/miru-logo.png)
=========

#### What It Is

Miru is a multi-tenant stream engine. Choo choo! No, not a steam engine, a stream engine. What is a stream engine? It is a search engine without all the overhead of scoring. Documents are maintained in insertion order, or "time order" in streams parlance. The single ranking for a document (sometimes referred to as prime ordering) is its position in the index. 

Alternative super hero names include Bit Collider and Join-O-Rama.

#### How It Works

Miru tenancy is comprised of a schema and time-ordered partitions. Each partition contains millions of ordered documents, and documents contain tens or thousands of terms in alignment with the schema. Each term is a sequence of bits, so at its heart Miru operates via efficient use of huge parallel bitsets.

Every partition is replicated for HA. Because every discrete partition is backed by a small number of files and a unified delta/append model, Miru supports tens or hundreds of thousands of partitions per node.

#### Use Cases

1. Activity/message streams

   Miru is well suited for streams of messages such as content creation, commenting, and status updates. The filter and full-text search APIs let you write once but very efficiently present in any number of views/facets.

2. Collaborative filtering and linear regression

   Miru can combine and collide meta information about content, people or other relatable features in order to compute recommendations and trends for dynamic queries and filters in realtime or near-realtime. 

3. Metrics and logging

   Similar to activity streams, chronologically ordered data inputs such as health metrics (resource utilization, counts, deltas) and logging (console/application output) fit easily into Miru’s time-ordered facet filters. Entire clusters or networks can aggregate as a single searchable set, presenting an instantaneous view of application health and error trends.

#### Getting Started
Check out Miru over at [the wiki](https://github.com/jivesoftware/miru/wiki).

#### Licensing
Miru is licensed under the Apache License, Version 2.0. See LICENSE for full license text.
