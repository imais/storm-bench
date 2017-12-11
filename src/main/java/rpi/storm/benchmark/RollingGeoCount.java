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
        public static final String ZONE_NO_LATLNG = "no_latlng";
        public static final String ZONE_UNDEFINED = "undefined";

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String str = input.getString(0);
            String zone = ZONE_NO_LATLNG;

            if (str.startsWith("{\"Id\"") && str.endsWith("},")) {
                // remove "," at the end and parse it as an JSON object
                JSONObject obj = new JSONObject(str.substring(0, str.length() - 1));
                if (obj.has("Lat") && !obj.isNull("Lat") && 
                    obj.has("Long") && !obj.isNull("Long")) {
                    double lat = obj.getDouble("Lat");
                    double lng = obj.getDouble("Long");

                    if ((-80 <= lat && lat < 84) && (-180 <= lng && lng < 180)) {
                        int[] base_lat = {80, 32, -8};
                        char[] base_letters = {'C', 'J', 'P'};
                        int lat_mode = 0;

                        if (lat < -32) lat_mode = 0;      // [-80, -32): C...H
                        else if (lat < 8) lat_mode = 1;   // [-32, 8): J...Q
                        else if (lat < 72) lat_mode = 2;  // [8, 72): P...W
                        else lat_mode = 3;                // [72, 84): X

                        char letter = (lat_mode == 3) ? 'X' : 
                            (char)(base_letters[lat_mode] + (int)((lat + base_lat[lat_mode]) / 8));
                        zone  = Integer.toString(1 + (int)((lng + 180) / 6)) + letter;
                    }
                    else 
                        zone = ZONE_UNDEFINED;
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
        builder.setSpout(SPOUT_ID, new KafkaSpout(spoutConf_), spouts_parallel_);
        builder.setBolt(ZONE_ID, new ZoneIndex(), bolts_parallel_)
            .localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(COUNT_ID, new RollingCountBolt(windowLength_, emitFreq_), 
                        bolts_parallel_)
            .fieldsGrouping(ZONE_ID, new Fields(ZoneIndex.FIELDS));

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        RollingGeoCount app = new RollingGeoCount(args);
        app.submitTopology(args[0]);
    }
}

