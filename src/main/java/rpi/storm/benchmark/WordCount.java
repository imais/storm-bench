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

import intel.storm.benchmark.lib.operation.WordSplit;
import yahoo.benchmark.common.Utils;
import rpi.storm.benchmark.common.BenchmarkBase;

import java.util.HashMap;
import java.util.Map;


public class WordCount extends BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(WordCount.class);

    public static final String SPOUT_ID = "spout";
    public static final String SPLIT_ID = "split";
    public static final String COUNT_ID = "count";

    public WordCount(String[] args) throws ParseException {
        super(args);
    }
    
    @Override
    public StormTopology getTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new KafkaSpout(spoutConf_), spouts_parallel_);
        builder.setBolt(SPLIT_ID, new SplitSentence(), bolts_parallel_)
            .localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(COUNT_ID, new Count(), bolts_parallel_)
            .fieldsGrouping(SPLIT_ID, new Fields(SplitSentence.FIELDS));

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        WordCount app = new WordCount(args);
        app.submitTopology(args[0]);
    }

    public static class SplitSentence extends BaseBasicBolt {
        public static final String FIELDS = "word";

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            for (String word : WordSplit.splitSentence(input.getString(0))) {
                collector.emit(new Values(word));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }
    }

    public static class Count extends BaseBasicBolt {
        public static final String FIELDS_WORD = "word";
        public static final String FIELDS_COUNT = "count";

        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
            log.debug(word + ": " + count);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS_WORD, FIELDS_COUNT));
        }
    }
}
