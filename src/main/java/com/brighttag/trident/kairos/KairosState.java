// Copyright 2014 BrightTag, Inc. All rights reserved.
package com.brighttag.trident.kairos;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.CustomDataPoint;
import org.kairosdb.client.builder.DataPoint;
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
 * @author codyaray
 * @since 2/18/2014
 * @param <T> type of the value; one of 'Long', 'TransactionalValue<Long>', or 'OpaqueValue<Long>'
 */
public class KairosState<T> implements IBackingMap<T> {

  @SuppressWarnings("rawtypes")
  private static final Map<StateType, KairosSerializer> DEFAULT_SERIALIZERS =
      ImmutableMap.<StateType, KairosSerializer>of(
        StateType.NON_TRANSACTIONAL, new NonTransactionalKairosSerializer(),
        StateType.TRANSACTIONAL, new TransactionalKairosSerializer(),
        StateType.OPAQUE, new OpaqueKairosSerializer());

  public static class Options<T> implements Serializable {
    private static final long serialVersionUID = -4631189563874399265L;
    public int localCacheSize = 5000;
    public String globalKey = "$__GLOBAL_KEY__$";
    public int port = 8080;
    public KairosSerializer<T> serializer;
    public int kairosWriteDelay = 2 * 1000; // 2 seconds = 2 x kairos default (1s for v0.9.1-0.9.2)
  }

  public static StateFactory opaque(String host) {
    return opaque(host, new Options<OpaqueValue<Long>>());
  }

  public static StateFactory opaque(String host, Options<OpaqueValue<Long>> options) {
    return new Factory<OpaqueValue<Long>>(StateType.OPAQUE, host, options);
  }

  public static StateFactory transactional(String host) {
    return transactional(host, new Options<TransactionalValue<Long>>());
  }

  public static StateFactory transactional(String host, Options<TransactionalValue<Long>> options) {
    return new Factory<TransactionalValue<Long>>(StateType.OPAQUE, host, options);
  }

  public static StateFactory nonTransactional(String host) {
    return nonTransactional(host, new Options<Long>());
  }

  public static StateFactory nonTransactional(String host, Options<Long> options) {
    return new Factory<Long>(StateType.NON_TRANSACTIONAL, host, options);
  }

  public static class Factory<T> implements StateFactory {
    private static final long serialVersionUID = -6288582170089084573L;

    private final StateType stateType;
    private final String host;
    private final Options<T> options;
    private final KairosSerializer<T> serializer;

    @SuppressWarnings("unchecked")
    public Factory(StateType stateType, String host, Options<T> options) {
      this.stateType = stateType;
      this.host = host;
      this.options = options;
      this.serializer = Objects.firstNonNull(options.serializer, DEFAULT_SERIALIZERS.get(stateType));
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
      KairosState<T> state = new KairosState<T>(new HttpClient(host, options.port), serializer, options.kairosWriteDelay);
      CachedMap cachedMap = new CachedMap(state, options.localCacheSize);
      MapState mapState;
      if (stateType == StateType.NON_TRANSACTIONAL) {
        mapState = NonTransactionalMap.build(cachedMap);
      } else if (stateType == StateType.OPAQUE) {
        mapState = OpaqueMap.build(cachedMap);
      } else if (stateType == StateType.TRANSACTIONAL) {
        mapState = TransactionalMap.build(cachedMap);
      } else {
        throw new RuntimeException("Unknown state type: " + stateType);
      }
      return new SnapshottableMap<T>(mapState, new Values(options.globalKey));
    }
  }

  private final KairosSerializer<T> serializer;
  private final HttpClient client;
  private final int kairosWriteDelay;

  public KairosState(HttpClient client, KairosSerializer<T> serializer, int kairosWriteDelay) {
    this.client = client;
    this.serializer = serializer;
    this.kairosWriteDelay = kairosWriteDelay;
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

  @Override
  public List<T> multiGet(List<List<Object>> keys) {
    // no keys means no date etc to query by
    if (keys.isEmpty()) {
      return ImmutableList.of();
    }
    Map<List<Date>, QueryBuilder> queryMap = Maps.newHashMap();
    for (List<Object> key : keys) {
      Date start = toDate(key.get(0));
      Date end = toDate(key.get(1), true);
      List<Date> bucket = ImmutableList.of(start, end);
      QueryBuilder builder = queryMap.get(bucket);
      if (builder == null) {
        builder = QueryBuilder.getInstance().setStart(start).setEnd(end);
        queryMap.put(bucket, builder);
      }
      builder.addMetric(key.get(2).toString())
          .addTags(toTags(key.subList(3, key.size())));
    }
    List<T> values = Lists.newArrayListWithExpectedSize(keys.size());
    try {
      for (QueryBuilder builder : queryMap.values()) {
        waitForWrite();
        QueryResponse response = client.query(builder);
        System.out.println("QUERY " + builder.build() + "\nRESPONSE " + toString(response));
        for (Queries query : response.getQueries()) {
          for (Results result : query.getResults()) {
            if (!Strings.isNullOrEmpty(result.getName()) && !result.getDataPoints().isEmpty()) {
              values.add(serializer.deserialize(result));
            } else {
              values.add(null);
            }
          }
        }
      }
    } catch (URISyntaxException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return values;
  }

  @Override
  public void multiPut(List<List<Object>> keys, List<T> vals) {
    MetricBuilder builder = MetricBuilder.getInstance();
    for (int i = 0; i < keys.size(); i++) {
      List<Object> k = keys.get(i);
      Metric metric = builder.addMetric(k.get(2).toString());
      serializer.serialize(vals.get(i), metric, toDate(k.get(0)), toTags(k.subList(3, k.size())));
    }
    try {
       System.out.println("STORE " + builder.build());
      client.pushMetrics(builder);
      waitForWrite();
    } catch (URISyntaxException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private Map<String, String> toTags(List<Object> objs) {
    Map<String, String> tags = Maps.newHashMap();
    for (Object o : objs) {
      if (o instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, String> m = (Map<String, String>) o;
        tags.putAll(m);
      } else {
        System.out.println("Ignoring non-map key: " + o);
      }
    }
    return tags;
  }

  private void waitForWrite() {
    try {
      // KairosDB has a delay buffer for writes
      // We must wait at least this long to avoid a race condition between READ and WRITE
      Thread.sleep(kairosWriteDelay);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private static interface KairosSerializer<T> extends Serializable {
    void serialize(T obj, Metric m, Date timestamp, Map<String, String> tags);
    T deserialize(Results r);
  }

  private static class OpaqueKairosSerializer implements KairosSerializer<OpaqueValue<Long>> {
    private static final long serialVersionUID = 2284757678780545341L;

    private static final JSONOpaqueSerializer SERIALIZER = new JSONOpaqueSerializer();

    @Override
    public void serialize(OpaqueValue<Long> obj, Metric metric, Date timestamp, Map<String, String> tags) {
      String value = new String(SERIALIZER.serialize(obj), Charsets.UTF_8);
      metric.addTags(tags);
      metric.addDataPoint(timestamp.getTime(), value, "storm_opaque");
    }

    @Override
    @SuppressWarnings("unchecked")
    public OpaqueValue<Long> deserialize(Results r) {
      // Guaranteed to be a single element because we query for a single bucket
      String value = ((CustomDataPoint<String>) Iterables.getOnlyElement(r.getDataPoints())).getValue();
      return SERIALIZER.deserialize(value.getBytes(Charsets.UTF_8));
    }

  }

  private static class TransactionalKairosSerializer implements KairosSerializer<TransactionalValue<Long>> {
    private static final long serialVersionUID = 2284757678780545341L;

    private static final JSONTransactionalSerializer SERIALIZER = new JSONTransactionalSerializer();

    @Override
    public void serialize(TransactionalValue<Long> obj, Metric metric, Date timestamp, Map<String, String> tags) {
      String value = new String(SERIALIZER.serialize(obj), Charsets.UTF_8);
      metric.addTags(tags);
      metric.addDataPoint(timestamp.getTime(), value, "storm_transactional");
    }

    @Override
    @SuppressWarnings("unchecked")
    public TransactionalValue<Long> deserialize(Results r) {
      // TODO comment - why is it guaranteed to only be a single element in the response?
      String value = ((CustomDataPoint<String>) Iterables.getOnlyElement(r.getDataPoints())).getValue();
      return SERIALIZER.deserialize(value.getBytes(Charsets.UTF_8));
    }

  }

  private static class NonTransactionalKairosSerializer implements KairosSerializer<Long> {
    private static final long serialVersionUID = 2284757678780545341L;

    private static final JSONNonTransactionalSerializer SERIALIZER = new JSONNonTransactionalSerializer();

    @Override
    public void serialize(Long obj, Metric metric, Date timestamp, Map<String, String> tags) {
      String value = new String(SERIALIZER.serialize(obj), Charsets.UTF_8);
      metric.addTags(tags);
      metric.addDataPoint(timestamp.getTime(), value, "storm_nontransactional");
    }

    @Override
    @SuppressWarnings("unchecked")
    public Long deserialize(Results r) {
      // TODO comment - why is it guaranteed to only be a single element in the response?
      String value = ((CustomDataPoint<String>) Iterables.getOnlyElement(r.getDataPoints())).getValue();
      return (Long)SERIALIZER.deserialize(value.getBytes(Charsets.UTF_8));
    }

  }

  private static String toString(QueryResponse response) {
    return FluentIterable.from(response.getQueries()).transformAndConcat(new Function<Queries,List<String>>() {
      @Override
      public List<String> apply(Queries input) {
        return KairosState.toString(input);
      }
    }).toList().toString();
  }

  private static List<String> toString(Queries queries) {
    return FluentIterable.from(queries.getResults()).transform(new Function<Results,String>() {
      @Override
      public String apply(Results input) {
        return KairosState.toString(input);
      }
    }).toList();
  }

  private static String toString(Results results) {
    return Objects.toStringHelper(results)
        .add("name", results.getName())
        .add("datapoints", toString(results.getDataPoints()))
        .add("tags", results.getTags())
        .add("groups", results.getGroupResults())
        .toString();
  }

  private static String toString(List<DataPoint> datapoints) {
    return FluentIterable.from(datapoints).transform(new Function<DataPoint,String>() {
      @Override
      public String apply(DataPoint input) {
        CustomDataPoint<?> datapoint = (CustomDataPoint<?>) input;
        return "[" + datapoint.getTimestamp() + "," + datapoint.getValue() + "]";
      }
    }).toList().toString();
  }

}