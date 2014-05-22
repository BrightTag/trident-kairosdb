/*
 * Copyright 2014 BrightTag, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.brighttag.trident.kairos;

import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang.NotImplementedException;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.Metric;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.response.Queries;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Results;

import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;

/**
 * Trident state implementation for KairosDB. It supports non-transactional,
 * transactional, and opaque state types.
 *
 * Requires use of KairosDB built with support for "trident_opaque", "trident_transactional",
 * and "trident_nontransactional" custom types.
 *
 * Only {@code GroupedStream}s are supported, keyed by {@code (timeStart, timeEnd, metricName, tags)}.
 * For example, assuming your spout emits fields {@code (timeStart, timeEnd, metricName, value, tags)},
 * you might setup an aggregation topology as follows. (This is the only currently tested setup).
 *
 * <pre>{@code
 *   TridentTopology topology = new TridentTopology();
 *   Stream stream = topology.newStream("spout1", spout)
 *       .groupBy(new Fields("timeStart", "timeEnd", "metricName", "tags"))
 *       .persistentAggregate(KairosState.opaque(kairosHost), new Fields("value"), new Sum(), new Fields("count"))
 *   return topology.build();
 * }</pre>
 *
 * @author codyaray
 * @since 2/18/2014
 * @param <T> type of the value; one of 'Long', 'TransactionalValue<Long>', or 'OpaqueValue<Long>'
 */
public class KairosState<T> implements IBackingMap<T> {

  @SuppressWarnings("rawtypes")
  private static final Map<StateType, Serializer> DEFAULT_SERIALIZERS =
      ImmutableMap.of(
          StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer(),
          StateType.TRANSACTIONAL, new JSONTransactionalSerializer(),
          StateType.OPAQUE, new JSONOpaqueSerializer());

  public static class Options<T> implements Serializable {
    private static final long serialVersionUID = -4631189563874399265L;
    public String globalKey = "$__GLOBAL_KEY__$";
    public int localCacheSize = 5000;
    public int port = 8080;
    public int kairosWriteDelay = 2 * 1000; // 2 seconds = 2 x kairos default (1s for v0.9.1-0.9.2)
    public Serializer<T> serializer;
  }

  /**
   * Return an opaque {@link KairosState.Factory}.
   */
  public static StateFactory opaque(String host) {
    return opaque(host, new Options<OpaqueValue<Long>>());
  }

  /**
   * Return an opaque {@link KairosState.Factory} with the given {@code options}.
   */
  public static StateFactory opaque(String host, Options<OpaqueValue<Long>> options) {
    return new Factory<OpaqueValue<Long>>(StateType.OPAQUE, host, options);
  }

  /**
   * Return a transactional {@link KairosState.Factory}.
   */
  public static StateFactory transactional(String host) {
    return transactional(host, new Options<TransactionalValue<Long>>());
  }

  /**
   * Return a transactional {@link KairosState.Factory} with the given {@code options}.
   */
  public static StateFactory transactional(String host, Options<TransactionalValue<Long>> options) {
    return new Factory<TransactionalValue<Long>>(StateType.OPAQUE, host, options);
  }

  /**
   * Return a non-transactional {@link KairosState.Factory}.
   */
  public static StateFactory nonTransactional(String host) {
    return nonTransactional(host, new Options<Long>());
  }

  /**
   * Return a non-transactional {@link KairosState.Factory} with the given {@code options}.
   */
  public static StateFactory nonTransactional(String host, Options<Long> options) {
    return new Factory<Long>(StateType.NON_TRANSACTIONAL, host, options);
  }

  /**
   * Factory for creating {@link KairosState}s.
   */
  public static class Factory<T> implements StateFactory {
    private static final long serialVersionUID = -6288582170089084573L;

    private final StateType stateType;
    private final String url;
    private final Options<T> options;
    private final Serializer<T> serializer;

    @SuppressWarnings("unchecked")
    public Factory(StateType stateType, String host, Options<T> options) {
      this.stateType = stateType;
      this.options = options;
      this.url = String.format("%s:%d", host, options.port);
      this.serializer = Objects.firstNonNull(options.serializer, DEFAULT_SERIALIZERS.get(stateType));
    }

    @Override
    @SuppressWarnings("rawtypes")
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
      try {
        KairosState<T> state = new KairosState<T>(new HttpClient(url), serializer, options.kairosWriteDelay);
        CachedMap<T> cachedMap = new CachedMap<T>(state, options.localCacheSize);
        MapState<T> mapState = buildMapState(cachedMap);
        return new SnapshottableMap<T>(mapState, new Values(options.globalKey));
      } catch (MalformedURLException e) {
        throw Throwables.propagate(e);
      }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private MapState<T> buildMapState(CachedMap cachedMap) {
      switch (stateType) {
      case NON_TRANSACTIONAL:
        return NonTransactionalMap.build(cachedMap);
      case OPAQUE:
        return OpaqueMap.build(cachedMap);
      case TRANSACTIONAL:
        return TransactionalMap.build(cachedMap);
      default:
        throw new RuntimeException("Unknown state type: " + stateType);
      }
    }
  }

  private final Serializer<T> serializer;
  private final HttpClient client;
  private final int kairosWriteDelay;
  private final String type;

  public KairosState(HttpClient client, Serializer<T> serializer, int kairosWriteDelay) {
    this.client = client;
    this.serializer = serializer;
    this.kairosWriteDelay = kairosWriteDelay;
    this.type = toType(serializer);
  }

  /**
   * Fetch multiple metrics from KairosDB.
   *
   * @param keys list of metric key fields in this order:
   *   (start_date [long], exclusive_end_date [long], name [string], tags [map<string, string>]...)
   * @return list of values corresponding to the {@code key} with the same index in {@code keys}
   *   or {@code null} for each key without a value
   */
  @Override
  public List<T> multiGet(List<List<Object>> keys) {
    // time range and metric name required to query by
    if (keys.size() < 3) {
      return ImmutableList.of();
    }
    Map<List<Date>, QueryBuilder> queries = groupQueriesByTimeRange(keys);
    return getValuesFromKairos(keys, queries);
  }

  private static Map<List<Date>, QueryBuilder> groupQueriesByTimeRange(List<List<Object>> keys) {
    Map<List<Date>, QueryBuilder> queryMap = Maps.newHashMap();
    for (List<Object> key : keys) {
      Date start = toDate(key.get(0));
      Date end = toDate(key.get(1), true);
      List<Date> timeRange = ImmutableList.of(start, end);
      QueryBuilder builder = queryMap.get(timeRange);
      if (builder == null) {
        builder = QueryBuilder.getInstance().setStart(start).setEnd(end);
        queryMap.put(timeRange, builder);
      }
      builder.addMetric(key.get(2).toString())
          .addTags(toTags(key.subList(3, key.size())));
    }
    return queryMap;
  }

  private List<T> getValuesFromKairos(List<List<Object>> keys, Map<List<Date>, QueryBuilder> queryMap) {
    List<T> values = Lists.newArrayListWithExpectedSize(keys.size());
    try {
      for (QueryBuilder builder : queryMap.values()) {
        waitForWrite(); // TODO: replace writeDelay in KairosDB with write-through cache
        QueryResponse response = client.query(builder);
        for (Queries query : response.getQueries()) {
          for (Results result : query.getResults()) {
            if (!Strings.isNullOrEmpty(result.getName()) && !result.getDataPoints().isEmpty()) {
              values.add(deserialize(result));
            } else {
              values.add(null);
            }
          }
        }
      }
    } catch (URISyntaxException e) {
      throw Throwables.propagate(e);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return values;
  }

  /**
   * Store multiple metrics into KairosDB. This save is idempotent; any data for
   * {@code keys} that already exists is overwritten with the new {@code vals}.
   *
   * @param keys list of metric key fields in this order:
   *   (start_date [long], exclusive_end_date [long], name [string], tags [map<string, string>]...)
   */
  @Override
  public void multiPut(List<List<Object>> keys, List<T> vals) {
    MetricBuilder builder = MetricBuilder.getInstance();
    for (int i = 0; i < keys.size(); i++) {
      List<Object> k = keys.get(i);
      String name = k.get(2).toString();
      Map<String, String> tags = toTags(k.subList(3, k.size()));
      Metric metric = builder.addMetric(name, type).addTags(tags);
      serialize(metric, toDate(k.get(0)), vals.get(i));
    }
    try {
      client.pushMetrics(builder);
      waitForWrite();
    } catch (URISyntaxException e) {
      throw Throwables.propagate(e);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * KairosDB has a delay buffer for writes. We must wait at least
   * this long to avoid a race condition between READ and WRITE
   */
  private void waitForWrite() {
    try {
      Thread.sleep(kairosWriteDelay);
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Adds a datapoint to {@code metric} consisting of the {@code timestamp}, the serialized
   * form of {@code obj}, and the custom data type for the current {@code serializer}.
   */
  private void serialize(Metric metric, Date timestamp, T obj) {
    String value = new String(serializer.serialize(obj), Charsets.UTF_8);
    metric.addDataPoint(timestamp.getTime(), value);
  }

  /**
   * Parses a datapoint value from the KairosDB response, returning the deserialized value.
   */
  private T deserialize(Results r) {
    // Guaranteed to be a single element because we query for a single bucket
    @SuppressWarnings("unchecked")
    String value = (String) Iterables.getOnlyElement(r.getDataPoints()).getValue();
    return serializer.deserialize(value.getBytes(Charsets.UTF_8));
  }

  @SuppressWarnings("rawtypes")
  private static final Map<Class<? extends Serializer>, String> KAIROS_TYPES = ImmutableMap.of(
      JSONOpaqueSerializer.class, "trident_opaque",
      JSONTransactionalSerializer.class, "trident_transactional",
      JSONNonTransactionalSerializer.class, "trident_nontransactional");

  private static String toType(Serializer<?> serializer) {
    if (KAIROS_TYPES.containsKey(serializer.getClass())) {
      return KAIROS_TYPES.get(serializer.getClass());
    }
    throw new NotImplementedException("Kairos custom type for serializer " + serializer.getClass());
  }

  private static Map<String, String> toTags(List<Object> objs) {
    Map<String, String> tags = Maps.newHashMap();
    for (Object o : objs) {
      if (o instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, String> m = (Map<String, String>) o;
        tags.putAll(m);
      }
    }
    return tags;
  }

  /**
   * Parses the string form of {@code obj} as a signed decimal long representing a date.
   * Throws an exception on failure.
   *
   * @return the Date representation of {@code obj}
   */
  private static Date toDate(Object obj) {
    return toDate(obj, false);
  }

  /**
   * Parses the string form of {@code obj} as a signed decimal long representing a date.
   * If {@code obj} is {@code exclusive} (i.e., {@code exclusive is true), subtracts one
   * millisecond to transform from an exclusive date to an inclusive date.
   * Throws an exception on failure.
   *
   * @param exclusive whether {@obj} is exclusive
   * @return the Date representation of {@code obj}
   */
  private static Date toDate(Object obj, boolean exclusive) {
    return new Date(Long.parseLong(obj.toString()) - (exclusive ? 1 : 0));
  }

}