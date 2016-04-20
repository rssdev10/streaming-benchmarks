/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package kafka.benchmark;

import benchmark.common.advertising.RedisAdCampaignCache;
import benchmark.common.advertising.CampaignProcessorCommon;
import benchmark.common.Utils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.HoppingWindows;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.UnlimitedWindows;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

/**
 * To Run:  run target/kafka-benchmarks-0.1.0-AdvertisingTopology.jar  -conf "../conf/benchmarkConf.yaml"
 */
public class AdvertisingTopology {

    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopology.class);
    final static private Serde<String> strSerde = new Serdes.StringSerde();
    final static private StringArraySerializer arrSerde = StringArraySerializer.getInstance();

    public static void main(final String[] args) throws Exception {
        Options opts = new Options();
        opts.addOption("conf", true, "Path to the config file.");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(opts, args);
        String configPath = cmd.getOptionValue("conf");
        Map<?, ?> conf = Utils.findAndReadConfigFile(configPath, true);

        Map<String,?> benchmarkParams = getKafkaConfs(conf);

        LOG.info("conf: {}", conf);
        LOG.info("Parameters used: {}", benchmarkParams);

        KStreamBuilder builder = new KStreamBuilder();
//        builder.addStateStore(
//                Stores.create("config-params")
//                .withStringKeys().withStringValues()
//                .inMemory().maxEntries(10).build(), "redis");

        String topicsArr[] = {(String)benchmarkParams.get("topic")}; 
        KStream<String, ?> source1 = builder.stream(topicsArr);

        Properties props = new Properties();
        props.putAll(benchmarkParams);
//        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-benchmarks");
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
////        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        source1
                // Parse the String as JSON
                .mapValues(input -> {
                    JSONObject obj = new JSONObject(input.toString());
                    //System.out.println(obj.toString());
                    String[] tuple = {
                            obj.getString("user_id"),
                            obj.getString("page_id"),
                            obj.getString("ad_id"),
                            obj.getString("ad_type"),
                            obj.getString("event_type"),
                            obj.getString("event_time"),
                            obj.getString("ip_address") };
                    return tuple;
                })

                // Filter the records if event type is "view"
                .filter(new Predicate<String, String[]>() {
                    @Override
                    public boolean test(String key, String[] value) {
                        return value[4].equals("view");
                    }
                })

                // project the event
                .mapValues(input -> {
                    String[] arr = (String[]) input;
                    return new String[] { arr[2], arr[5] };
                })

                // perform join with redis data
                .transformValues(new RedisJoinBolt(benchmarkParams))
                .filter( (key, value) -> value != null)

                // create key from value
                .map( (key, value) -> {
                    String[] arr = (String[]) value;
                    return new KeyValue<String, String[]>(arr[0], arr);
                  }
                )

                // process campaign
                .aggregateByKey(
                        new Initializer<List<String[]>>() {
                            @Override
                            public List<String[]> apply() {
                                return new ArrayList<String[]>();
                            }
                        },
                        new Aggregator<String, String[], List<String[]>>() {
                            @Override
                            public List<String[]> apply(
                                    String aggKey,
                                    String[] value,
                                    List<String[]> aggregate) {
                                aggregate.add(value);
                                return aggregate;
                            }
                        },
                        UnlimitedWindows.of("kafka-test-unlim"),
                        // HoppingWindows.of("kafka-test-hopping").with(12L).every(5L),
                        strSerde,
                        arrSerde
                 ).toStream()
                .process(new CampaignProcessor(benchmarkParams));

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
    }

    public static final class RedisJoinBolt implements ValueTransformerSupplier<String[], String[]> {
        private final Map<String, ?> benchmarkParams;
        public RedisJoinBolt(Map<String, ?> benchmarkParams) {
            this.benchmarkParams = benchmarkParams;
        }

        public ValueTransformer<String[], String[]> get() {
            return new ValueTransformer<String[], String[]>() {

                RedisAdCampaignCache redisAdCampaignCache;

                @Override
                public void init(ProcessorContext context) {
                    // initialize jedis
                    String jedisServer = (String) benchmarkParams.get("jedis_server");
                    LOG.info("Opening connection with Jedis to {}", jedisServer);

                    this.redisAdCampaignCache = new RedisAdCampaignCache(jedisServer);

                    this.redisAdCampaignCache.prepare();
                }

                @Override
                public String[] transform(String[] input) {
                    String[] tuple = null;
                    String ad_id = input[0];
                    String campaign_id = this.redisAdCampaignCache.execute(ad_id);
                    if (campaign_id != null) {
                        tuple = new String[] { campaign_id, (String) input[0], (String) input[1] };
                    }
                    return tuple;
                }

                @Override
                public String[] punctuate(long timestamp) {
                    return null;
                }

                @Override
                public void close() {}
            };
        }
    }

    public static class CampaignProcessor implements ProcessorSupplier<Windowed<String>, List<String[]>> 
    {
        private final Map<String, ?> benchmarkParams;
        public CampaignProcessor(Map<String, ?> benchmarkParams) {
            this.benchmarkParams = benchmarkParams;
        }

        @Override
        public Processor<Windowed<String>, List<String[]>> get() {
            return new Processor<Windowed<String>, List<String[]>>() {
                CampaignProcessorCommon campaignProcessorCommon;

                @Override
                public void init(ProcessorContext context) {
                    // initialize jedis
                    String jedisServer = (String) benchmarkParams.get("jedis_server");
                    LOG.info("Opening connection with Jedis to {}", jedisServer);

                    this.campaignProcessorCommon = new CampaignProcessorCommon(jedisServer);
                    this.campaignProcessorCommon.prepare();
                }

                @Override
                public void process(Windowed<String> key, List<String[]> tupleList) {
                    tupleList.forEach(x -> {
                        String[] tuple = x;
                        String campaign_id = tuple[0];
                        String event_time = tuple[2];
                        this.campaignProcessorCommon.execute(campaign_id, event_time);
                    });
                }

                @Override
                public void punctuate(long timestamp) {}

                @Override
                public void close() {}
            };
        }
    }

    private static Map<String, String> getKafkaConfs(Map<?, ?> conf) {
        String kafkaBrokers = getKafkaBrokers(conf);
        String zookeeperServers = getZookeeperServers(conf);

        Map<String, String> confs = new HashMap<String, String>();
        confs.put("topic", getKafkaTopic(conf));
        confs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
//        confs.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeperServers);
        confs.put("jedis_server", getRedisHost(conf));
        confs.put(StreamsConfig.APPLICATION_ID_CONFIG, "myGroup");

        return confs;
    }

    @SuppressWarnings("unchecked")
    private static String getZookeeperServers(Map<?, ?> conf) {
        if (!conf.containsKey("zookeeper.servers")) {
            throw new IllegalArgumentException("Not zookeeper servers found!");
        }
        return listOfStringToString((List<String>) conf.get("zookeeper.servers"),
                String.valueOf(conf.get("zookeeper.port")));
    }

    @SuppressWarnings("unchecked")
    private static String getKafkaBrokers(Map<?, ?> conf) {
        if (!conf.containsKey("kafka.brokers")) {
            throw new IllegalArgumentException("No kafka brokers found!");
        }
        if (!conf.containsKey("kafka.port")) {
            throw new IllegalArgumentException("No kafka port found!");
        }
        return listOfStringToString((List<String>) conf.get("kafka.brokers"),
                String.valueOf(conf.get("kafka.port")));
    }

    private static String getKafkaTopic(Map<?, ?> conf) {
        if (!conf.containsKey("kafka.topic")) {
            throw new IllegalArgumentException("No kafka topic found!");
        }
        return (String) conf.get("kafka.topic");
    }

    private static String getRedisHost(Map<?, ?> conf) {
        if (!conf.containsKey("redis.host")) {
            throw new IllegalArgumentException("No redis host found!");
        }
        return (String) conf.get("redis.host");
    }

    public static String listOfStringToString(List<String> list, String port) {
        return list.stream()
                .map(item -> item + ":" + port)
                .collect(Collectors.joining(","));
    }
}
