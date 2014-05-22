# Trident KairosDB State

This library implements a Trident state on top of KairosDB. It supports non-transactional,
transactional, and opaque state types.

## Install

This library is not yet hosted in Maven Central. Until such time, you can build and install
it into your local Maven repository for linking against.

You must also install the [Trident plugin in KairosDB](https://github.com/BrightTag/trident-kairosdb-plugin).

## Usage

Only `GroupedStream`s are supported, keyed by `(timeStart, timeEnd, metricName, tags)`.
For example, assuming your spout emits fields `(timeStart, timeEnd, metricName, value, tags)`,
you might setup an aggregation topology as follows. (This is the only currently tested setup).

    String kairosHost = "kairos01";
    TridentTopology topology = new TridentTopology();
    Stream stream = topology.newStream("spout1", spout)
        .groupBy(new Fields("timeStart", "timeEnd", "metricName", "tags"))
        .persistentAggregate(KairosState.opaque(kairosHost), new Fields("value"), new Sum(), new Fields("count"))
    return topology.build();

## Configuration

Several options are exposed via the `KairosState.Options` class. The most important
options are for changing the port, write delay, and cache size.

* `port`: the port on which KairosDB is listening.
* `kairosWriteDelay`: the time in millis to sleep after writing to ensure read-after-write consistency.
* `localCacheSize`: the number of key-value pairs to cache in-memory.

For example:

    KairosState.Options<OpaqueValue<Long>> opts = new KairosState.Options<OpaqueValue<Long>>();
    opts.port = 8080;
    opts.kairosWriteDelay = 2000;
    opts.localCacheSize = 5000;
    StateFactory stateFactory = KairosState.opaque(kairosHost, opts);

