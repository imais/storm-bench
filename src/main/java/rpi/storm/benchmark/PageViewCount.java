package rpi.storm.benchmark;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;

import yahoo.benchmark.common.Utils;
import intel.storm.benchmark.lib.bolt.PageViewBolt;
import static intel.storm.benchmark.lib.spout.pageview.PageView.Item;
import rpi.storm.benchmark.WordCount;
import rpi.storm.benchmark.common.BenchmarkBase;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.List;


public class PageViewCount extends BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(PageViewCount.class);

    public static final String SPOUT_ID = "spout";
    public static final String VIEW_ID = "view";
    public static final String COUNT_ID = "count";

    public PageViewCount(String[] args) throws ParseException {
        super(args);
    }

    @Override
    public StormTopology getTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new KafkaSpout(spoutConf_), spouts_parallel_);
        builder.setBolt(VIEW_ID, new PageViewBolt(Item.URL, Item.ONE), bolts_parallel_)
            .localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(COUNT_ID, new WordCount.Count(), bolts_parallel_)
            .fieldsGrouping(VIEW_ID, new Fields(Item.URL.toString()));

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        PageViewCount app = new PageViewCount(args);
        app.submitTopology(args[0]);
    }
}
