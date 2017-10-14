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

import yahoo.benchmark.common.Utils;
import rpi.storm.benchmark.common.BenchmarkBase;
import rpi.storm.benchmark.lib.bolt.RollingLatLongBolt;

import java.util.Map;


public class RollingProximity extends BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(RollingProximity.class);

    public static final String SPOUT_ID = "spout";
    public static final String LATLONG_FILTER_ID = "latlong_filter";
    public static final String ROLLING_LATLONG_ID = "rolling_latlong";
    public static final String DIST_FILTER_ID = "dist_filter";
    public static final String ROLLING_SORT_ID = "rolling_sort";

    private int windowLength_;
    private int emitFreq_;
    private double proximityThreshold_;

    public RollingProximity(String[] args) throws ParseException {
        super(args);
        windowLength_ = getConfInt(globalConf_, "rollingproximity.window_length");
        emitFreq_ = getConfInt(globalConf_, "rollingproximity.emit_freq");
        proximityThreshold_ = getConfInt(globalConf_, "rollingproximity.proximity_threshold");
    }

    public static class LatLongFilter extends BaseBasicBolt {
        public static final String FIELDS_ICAO = "icao";
        public static final String FIELDS_POSTIME = "postime";
        public static final String FIELDS_LAT = "lat";
        public static final String FIELDS_LONG = "long";

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String str = input.getString(0);

            if (str.startsWith("{\"Id\"") && str.endsWith("},")) {
                // remove "," at the end and parse it as an JSON object
                JSONObject obj = new JSONObject(str.substring(0, str.length() - 1));
                if (obj.has("Icao") && !obj.isNull("Icao") && 
                    obj.has("PosTime") && !obj.isNull("PosTime") &&
                    obj.has("Lat") && !obj.isNull("Lat") && 
                    obj.has("Long") && !obj.isNull("Long")) {
                    String icao = obj.getString("Icao");
                    long posTime = obj.getLong("PosTime");
                    double lat = obj.getDouble("Lat");
                    double lng = obj.getDouble("Long");

                    collector.emit(new Values(icao, posTime, lat, lng));
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS_ICAO, FIELDS_POSTIME, FIELDS_LAT, FIELDS_LONG));
        }
    }

    @Override
    public StormTopology getTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new KafkaSpout(spoutConf_), parallel_);
        builder.setBolt(LATLONG_FILTER_ID, new LatLongFilter(), parallel_)
            .shuffleGrouping(SPOUT_ID);
        builder.setBolt(ROLLING_LATLONG_ID, 
                        new RollingLatLongBolt(windowLength_, emitFreq_), parallel_)
            .fieldsGrouping(LATLONG_FILTER_ID, new Fields(LatLongFilter.FIELDS_ICAO));
        // builder.setBolt(COUNT_ID, new RollingCountBolt(windowLength_, emitFreq_), parallel_)
        //     .fieldsGrouping(ZONE_ID, new Fields(ZoneIndex.FIELDS));

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        RollingProximity app = new RollingProximity(args);
        app.submitTopology(args[0]);
    }
}

