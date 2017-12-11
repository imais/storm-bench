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
import intel.storm.benchmark.lib.bolt.ConstBolt;

import java.io.Serializable;
import java.util.Map;


public class SOL extends BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(SOL.class);

    public static final String SPOUT_ID = "spout";
    public static final String BOLT_ID = "bolt";

    private int numLevels_;

    public SOL(String[] args) throws ParseException {
        super(args);
        numLevels_ = getConfInt(globalConf_, "sol.topology_level");
    }

    @Override
    public StormTopology getTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        // numLevel: total number of layers including one spout and bolts
        builder.setSpout(SPOUT_ID, new KafkaSpout(spoutConf_), spouts_parallel_);
        builder.setBolt(BOLT_ID + 1, new ConstBolt(), bolts_parallel_)
            .shuffleGrouping(SPOUT_ID);
        for (int levelNum = 2; levelNum <= numLevels_ - 1; levelNum++) {
            builder.setBolt(BOLT_ID + levelNum, new ConstBolt(), bolts_parallel_)
                .shuffleGrouping(BOLT_ID + (levelNum - 1));
        }

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        SOL app = new SOL(args);
        app.submitTopology(args[0]);
    }
}
