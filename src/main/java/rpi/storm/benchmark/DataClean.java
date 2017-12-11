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

import intel.storm.benchmark.lib.bolt.FilterBolt;
import intel.storm.benchmark.lib.bolt.PageViewBolt;
import static intel.storm.benchmark.lib.spout.pageview.PageView.Item;
import yahoo.benchmark.common.Utils;
import rpi.storm.benchmark.common.BenchmarkBase;

import java.util.HashMap;
import java.util.Map;

public class DataClean extends BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(DataClean.class);

    public static final String SPOUT_ID = "spout";
    public static final String VIEW_ID = "view";
    public static final String FILTER_ID = "filter";

    public DataClean(String[] args) throws ParseException {
        super(args);
    }

    @Override
    public StormTopology getTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new KafkaSpout(spoutConf_), spouts_parallel_);
        builder.setBolt(VIEW_ID, new PageViewBolt(Item.STATUS, Item.ALL), bolts_parallel_)
            .localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(FILTER_ID, new FilterBolt<Integer>(200), bolts_parallel_)
            .fieldsGrouping(VIEW_ID, new Fields(Item.STATUS.toString()));

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        DataClean app = new DataClean(args);
        app.submitTopology(args[0]);
    }
}
