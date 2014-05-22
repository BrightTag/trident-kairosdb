# Trident KairosDB State

This library implements a Trident state on top of KairosDB. It supports non-transactional,
transactional, and opaque state types.

## Install

Because this is still beta software, you'll have to build and install from the `develop` branch
of the [kairosdb-client](https://github.com/BrightTag/kairosdb-client/tree/develop).

1. Clone the BT fork (til the pull request is merged): `git clone https://github.com/BrightTag/kairosdb-client.git`
2. Checkout the develop branch: `cd kairosdb-client && git checkout develop`
3. Build and install to local Maven: `mvn clean install`

Now continue with the normal installation.

1. Build and install trident-kairosdb into your local Maven repository.

    $ cd trident-kairosdb
    $ mvn clean install

2. Add the Maven coordinates to your Storm topology project.

    <dependency>
        <groupId>com.brighttag</groupId>
        <artifactId>trident-kairosdb</artifactId>
        <version>0.1.0</version>
    </dependency>

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

