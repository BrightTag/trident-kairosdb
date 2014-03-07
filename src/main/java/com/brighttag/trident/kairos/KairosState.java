package com.brighttag.trident.kairos;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;

import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.Aggregator;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.builder.LongDataPoint;
import org.kairosdb.client.builder.Metric;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.response.Queries;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Results;

import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;

import static org.kairosdb.client.util.Preconditions.checkNotNullOrEmpty;

public class KairosState<T> implements IBackingMap<T> {

  @SuppressWarnings("rawtypes")
  private static final Map<StateType, KairosSerializer> DEFAULT_SERIALIZERS =
      ImmutableMap.<StateType, KairosSerializer>of(
        StateType.NON_TRANSACTIONAL, new NonTransactionalKairosSerializer(),
//        StateType.TRANSACTIONAL, new TransactionalKairosSerializer(),
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

  private static Date toDate(Object obj) {
    return new Date(Long.parseLong(obj.toString()));
  }

  @Override
  public List<T> multiGet(List<List<Object>> keys) {
//    System.out.println("GET KEYS " + keys);
    // no keys means no date etc to query by
    if (keys.isEmpty()) {
      return ImmutableList.of();
    }
    Map<List<Date>, QueryBuilder> queryMap = Maps.newHashMap();
    for (List<Object> key : keys) {
      Date start = toDate(key.get(0));
      Date end = toDate(key.get(1));
      List<Date> bucket = ImmutableList.of(start, end);
      QueryBuilder builder = queryMap.get(bucket);
      if (builder == null) {
        builder = QueryBuilder.getInstance().setStart(start).setEnd(end);
        queryMap.put(bucket, builder);
      }
      builder.addMetric(key.get(2).toString())
          .addAggregator(new AggregatorImpl("max"))
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
//    System.out.println("PUT KEYS " + keys + " VALS " + vals);
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

  private static class AggregatorImpl implements Aggregator {
    private final String name;

    private AggregatorImpl(String name)
    {
      this.name = checkNotNullOrEmpty(name);
    }

    @Override
    public String getName()
    {
      return name;
    }

    @Override
    public String toJson()
    {
      return "\"name\": \"" + name + "\"";
    }
  }

  private static interface KairosSerializer<T> extends Serializable {
    void serialize(T obj, Metric m, Date timestamp, Map<String, String> tags);
    T deserialize(Results r);
  }

  private static class OpaqueKairosSerializer implements KairosSerializer<OpaqueValue<Long>> {
    private static final long serialVersionUID = 2284757678780545341L;

    @Override
    public void serialize(OpaqueValue<Long> obj, Metric metric, Date timestamp, Map<String, String> tags) {
      metric.addTags(tags);
      metric.addTag("currTxid", String.valueOf(obj.getCurrTxid()));
      metric.addTag("prev", String.valueOf(obj.getPrev()));
      metric.addDataPoint(timestamp.getTime(), obj.getCurr());
    }

    @Override
    public OpaqueValue<Long> deserialize(Results r) {
      Map<String, List<String>> tags = r.getTags();
      long transactionId = Long.parseLong(NULLABLE_STRING_LONG_ORDER.max(tags.get("currTxid")));
      String s = NULLABLE_STRING_LONG_ORDER.max(tags.get("prev"));
      Long prev = s.equals("null") ? null : Long.parseLong(s);
      long curr = ((LongDataPoint) VALUE_ORDER.max(r.getDataPoints())).getValue();
      return new OpaqueValue<Long>(transactionId, curr, prev);
    }

    private static Ordering<DataPoint> VALUE_ORDER = new Ordering<DataPoint>() {
      @Override
      public int compare(DataPoint l, DataPoint r) {
        LongDataPoint left = (LongDataPoint) l;
        LongDataPoint right = (LongDataPoint) r;
        return Longs.compare(left.getValue(), right.getValue());
      }
    };

    private static Ordering<String> NULLABLE_STRING_LONG_ORDER = new Ordering<String>() {
      @Override
      public int compare(@Nullable String l, @Nullable String r) {
        if (l.equals("null")) { return -1; }
        if (r.equals("null")) { return  1; }
        return Longs.compare(Long.parseLong(l), Long.parseLong(r));
      }
    };

  }

  private static class NonTransactionalKairosSerializer implements KairosSerializer<Long> {
    private static final long serialVersionUID = 2284757678780545341L;

    @Override
    public void serialize(Long obj, Metric metric, Date timestamp, Map<String, String> tags) {
      metric.addTags(tags);
      metric.addDataPoint(timestamp.getTime(), obj);
    }

    @Override
    public Long deserialize(Results r) {
      Map<String, List<String>> tags = r.getTags();
      long transactionId = Long.parseLong(NULLABLE_STRING_LONG_ORDER.max(tags.get("currTxid")));
      return transactionId;
    }

    private static Ordering<String> NULLABLE_STRING_LONG_ORDER = new Ordering<String>() {
      @Override
      public int compare(@Nullable String l, @Nullable String r) {
        if (l.equals("null")) { return -1; }
        if (r.equals("null")) { return  1; }
        return Longs.compare(Long.parseLong(l), Long.parseLong(r));
      }
    };

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
        LongDataPoint datapoint = (LongDataPoint) input;
        return "[" + datapoint.getTimestamp() + "," + datapoint.getValue() + "]";
      }
    }).toList().toString();
  }

}