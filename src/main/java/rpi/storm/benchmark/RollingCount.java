package rpi.storm.benchmark;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
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


public class RollingCount extends BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(RollingCount.class);

    public static final String SPOUT_ID = "spout";
    public static final String SPLIT_ID = "split";
    public static final String COUNT_ID = "rolling_count";

    private int windowLength_;
    private int emitFreq_;

    public RollingCount(String[] args) throws ParseException {
        super(args);
        windowLength_ = getConfInt(globalConf_, "rolling_count.window_length");
        emitFreq_ = getConfInt(globalConf_, "rolling_count.emit_freq");
    }

    @Override
    public StormTopology getTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new KafkaSpout(spoutConf_), parallel_);
        builder.setBolt(SPLIT_ID, new WordCount.SplitSentence(), parallel_)
            .localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(COUNT_ID, new RollingCountBolt(windowLength_, emitFreq_), parallel_)
            .fieldsGrouping(SPLIT_ID, new Fields(WordCount.SplitSentence.FIELDS));

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        RollingCount app = new RollingCount(args);
        app.submitTopology(args[0]);
    }
}

