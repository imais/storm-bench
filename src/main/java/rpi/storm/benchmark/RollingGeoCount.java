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
import org.json.JSONObject;
import storm.kafka.KafkaSpout;

import intel.storm.benchmark.lib.bolt.RollingCountBolt;
import yahoo.benchmark.common.Utils;
import rpi.storm.benchmark.common.BenchmarkBase;

import java.util.Map;


public class RollingGeoCount extends BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(RollingGeoCount.class);

    public static final String SPOUT_ID = "spout";
    public static final String ZONE_ID = "zone";
    public static final String COUNT_ID = "rolling_count";

    private int windowLength_;
    private int emitFreq_;

    public RollingGeoCount(String[] args) throws ParseException {
        super(args);
        windowLength_ = getConfInt(globalConf_, "rollingcount.window_length");
        emitFreq_ = getConfInt(globalConf_, "rollingcount.emit_freq");
    }

    public static class ZoneIndex extends BaseBasicBolt {
        public static final String FIELDS = "zone";
        public static final String ZONE_INVALID = "invalid";
        public static final String ZONE_OUTSIDE_US = "outside_us";

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String str = input.getString(0);
            String zone = ZONE_INVALID;

            if (str.startsWith("{\"Id\"") && str.endsWith("},")) {
                // remove "," at the end and parse it as an JSON object
                JSONObject obj = new JSONObject(str.substring(0, str.length() - 1));
                if (obj.getString("Lat") != null && obj.getString("Long") != null) {
                    double lat = Double.parseDouble(obj.getString("Lat"));
                    double lng = Double.parseDouble(obj.getString("Long"));

                    if ((24 <= lat && lat < 48) && (lng <= 66 && lng < 126)) {
                        // zone id according to the U.S. national grid
                        char letter = (char)('R' + (int)((lat - 24) / 8));
                        zone  = Integer.toString(19 - (int)((lng - 66) / 6)) + letter;
                    }
                    else
                        zone = ZONE_OUTSIDE_US;
                }
            }
            collector.emit(new Values(zone));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }
    }

    @Override
    public StormTopology getTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new KafkaSpout(spoutConf_), parallel_);
        builder.setBolt(ZONE_ID, new ZoneIndex(), parallel_)
            .localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(COUNT_ID, new RollingCountBolt(windowLength_, emitFreq_), parallel_)
            .fieldsGrouping(ZONE_ID, new Fields(ZoneIndex.FIELDS));

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        RollingGeoCount app = new RollingGeoCount(args);
        app.submitTopology(args[0]);
    }
}

