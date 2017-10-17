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

import intel.storm.benchmark.lib.bolt.PageViewBolt;
import intel.storm.benchmark.lib.bolt.UniqueVisitorBolt;
import static intel.storm.benchmark.lib.spout.pageview.PageView.Item;
import yahoo.benchmark.common.Utils;
import rpi.storm.benchmark.common.BenchmarkBase;

import java.util.Map;


public class UniqueVisitor extends BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(UniqueVisitor.class);

    public static final String SPOUT_ID = "spout";
    public static final String VIEW_ID = "view";
    public final static String UNIQUER_ID = "uniquer";

    private int winLen_;
    private int emitFreq_;

    public UniqueVisitor(String[] args) throws ParseException {
        super(args);
        winLen_ = getConfInt(globalConf_, "unique_visitor.window_length");
        emitFreq_ = getConfInt(globalConf_, "unique_visitor.emit_freq");
    }

    @Override
    public StormTopology getTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new KafkaSpout(spoutConf_), parallel_);
        builder.setBolt(VIEW_ID, new PageViewBolt(Item.URL, Item.USER), parallel_)
            .localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(UNIQUER_ID, new UniqueVisitorBolt(winLen_, emitFreq_), parallel_)
            .fieldsGrouping(VIEW_ID, new Fields(Item.URL.toString()));

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        UniqueVisitor app = new UniqueVisitor(args);
        app.submitTopology(args[0]);
    }
}
