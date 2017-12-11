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

import yahoo.benchmark.common.Utils;
import rpi.storm.benchmark.common.BenchmarkBase;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Grep extends BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(Grep.class);

    public static final String SPOUT_ID = "spout";
    public static final String FM_ID = "find";
    public static final String CM_ID = "count";
    
    private String regex_;

    public Grep(String[] args) throws ParseException {
        super(args);
        regex_ = getConfString(globalConf_, "grep.pattern_string");
    }

    @Override
    public StormTopology getTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new KafkaSpout(spoutConf_), spouts_parallel_);
        builder.setBolt(FM_ID, new FindMatchingSentence(regex_), bolts_parallel_)
            .localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(CM_ID, new CountMatchingSentence(), bolts_parallel_)
            .fieldsGrouping(FM_ID, new Fields(FindMatchingSentence.FIELDS));        

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        Grep app = new Grep(args);
        app.submitTopology(args[0]);
    }

    public static class FindMatchingSentence extends BaseBasicBolt {
        public static final String FIELDS = "word";
        private Pattern pattern;
        private Matcher matcher;
        private final String ptnString;

        public FindMatchingSentence(String ptnString) {
            this.ptnString = ptnString;
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            pattern = Pattern.compile(ptnString);
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String sentence = input.getString(0);
            matcher = pattern.matcher(input.getString(0));
            if (matcher.find()) {
                log.debug(String.format("find pattern %s in sentence %s", ptnString, sentence));
                collector.emit(new Values(1));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }
    }

    public static class CountMatchingSentence extends BaseBasicBolt {
        public static final String FIELDS = "count";
        private int count = 0;

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            if (input.getInteger(0).equals(1)) {
                collector.emit(new Values(count++));
                log.debug("count: " + count);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }
    }
}
