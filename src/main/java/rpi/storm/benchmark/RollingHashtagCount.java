package rpi.storm.benchmark;

import backtype.storm.generated.StormTopology;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;

import intel.storm.benchmark.lib.bolt.RollingCountBolt;
import intel.storm.benchmark.lib.bolt.RollingBolt;
import intel.storm.benchmark.lib.spout.FileReadSpout;
import yahoo.benchmark.common.Utils;
import rpi.storm.benchmark.common.BenchmarkBase;

import java.util.Map;


public class RollingHashtagCount extends BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(RollingHashtagCount.class);
    public static final String SPOUT_ID = "spout";
    public static final String HASHTAG_ID = "hashtag";
    public static final String COUNT_ID = "rolling_count";

    private int windowLength_;
    private int emitFreq_;

    public RollingHashtagCount(String[] args) throws ParseException {
        super(args);
        windowLength_ = getConfInt(globalConf_, "rolling_hashtag_count.window_length");
        emitFreq_ = getConfInt(globalConf_, "rolling_hashtag_count.emit_freq");
    }

    public static class HashtagBolt extends BaseBasicBolt {
        public static final String FIELDS_HASHTAG = "hashtag";
        public static final int TWEET_TEXT_INDEX = 4;
        public static final int TWEET_NUM_FIELDS = 13;

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String str = input.getString(0);

            if (str.startsWith("["))
                return;

            String[] fields = str.split("\\|");
            // log.info("fields.length: " + fields.length);
            if (fields.length != TWEET_NUM_FIELDS)
                return;

            String[] words = fields[TWEET_TEXT_INDEX].split(" ");
            for (int i = 0; i < words.length; i++) {
                if (words[i].startsWith("#") && 1 < words[i].length()) {
                    // there could be multiple hashtags, so go through all words
                    // log.info("hashtag: " + words[i]);
                    collector.emit(new Values(words[i]));
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS_HASHTAG));
        }
    }

    @Override
    public StormTopology getTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new KafkaSpout(spoutConf_), parallel_);
        builder.setBolt(HASHTAG_ID, new HashtagBolt(), parallel_)
            .localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(COUNT_ID, new RollingCountBolt(windowLength_, emitFreq_), parallel_)
            .fieldsGrouping(HASHTAG_ID, new Fields(HashtagBolt.FIELDS_HASHTAG));

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        RollingHashtagCount app = new RollingHashtagCount(args);
        app.submitTopology(args[0]);
    }
}

