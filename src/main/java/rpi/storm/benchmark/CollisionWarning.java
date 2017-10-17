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
import rpi.storm.benchmark.RollingSort;
import rpi.storm.benchmark.common.BenchmarkBase;
import rpi.storm.benchmark.lib.bolt.RollingLatLongBolt;

import java.util.Map;
import java.util.HashMap;


public class CollisionWarning extends BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(CollisionWarning.class);

    public static final String SPOUT_ID = "spout";
    public static final String LATLONG_FILTER_ID = "latlong_filter";
    public static final String ROLLING_LATLONG_ID = "rolling_latlong";
    public static final String DIST_FILTER_ID = "dist_filter";
    public static final String ROLLING_SORT_ID = "rolling_sort";

    private int windowLength_;
    private int emitFreq_;
    private double distThresholdKm_;
    private int sortEmitFreq_;
    private int sortChunkSize_;

    public CollisionWarning(String[] args) throws ParseException {
        super(args);
        windowLength_ = getConfInt(globalConf_, "collision_warning.window_length");
        emitFreq_ = getConfInt(globalConf_, "collision_warning.emit_freq");
        distThresholdKm_ = getConfInt(globalConf_, "collision_warning.dist_threshold_km");
        sortEmitFreq_ = getConfInt(globalConf_, "collision_warning.sort_emit_freq");
        sortChunkSize_ = getConfInt(globalConf_, "collision_warning.sort_chunk_size");
    }

    public static class LatLongFilterBolt extends BaseBasicBolt {
        public static final String FIELDS_ICAO = "icao";
        public static final String FIELDS_POSTIME = "postime";
        public static final String FIELDS_LAT = "lat";
        public static final String FIELDS_LONG = "long";
        public static final String FIELDS_GND = "gnd";

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
                    if (!obj.has("Gnd") || obj.isNull("Gnd") || !obj.getBoolean("Gnd"))
                        // if Gnd is true, do not emit
                        collector.emit(new Values(icao, posTime, lat, lng));
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS_ICAO, FIELDS_POSTIME, FIELDS_LAT, FIELDS_LONG));
        }
    }

    public static class DistFilterBolt extends BaseBasicBolt {
        public static final String FIELDS_DIST = "dist";
        public static final String FIELDS_FLIGHT1 = "flight1";
        public static final String FIELDS_FLIGHT2 = "flight2";

        private static int taskId;
        private static int totalTasks;
        private static Map<String, Values> flightMap;
        private static double EARTH_RADIUS_KM = 6371.0; // mean radius in kilometer

        private double distThresholdKm;
        
        public DistFilterBolt(double distThresholdKm) {
            this.distThresholdKm = distThresholdKm;
        }
        
        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            int orgTaskId = context.getThisTaskId();
            totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
            taskId = orgTaskId % totalTasks;
            System.out.println("original taskId: " + orgTaskId + 
                               ", taskId: " + taskId + 
                               ", totalTasks: " + totalTasks);
            flightMap = new HashMap<String, Values>();
        }

        public double computeDist(double lat1, double lng1, double lat2, double lng2) {
            // The haversine formula:
            //  https://en.wikipedia.org/wiki/Haversine_formula
            //  http://www.movable-type.co.uk/scripts/latlong.html
            double deltaLat = Math.toRadians(lat2 - lat1);
            double deltaLng = Math.toRadians(lng2 - lng1);
            lat1 = Math.toRadians(lat1);
            lat2 = Math.toRadians(lat2);

            double a = Math.sin(deltaLat/2) * Math.sin(deltaLat/2) + 
                Math.cos(lat1) * Math.cos(lat2) *
                Math.sin(deltaLng/2) * Math.sin(deltaLng/2);
            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));

            return EARTH_RADIUS_KM * c;
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String icao = input.getString(0);
            Long posTime = input.getLong(1);
            Double lat = input.getDouble(2);
            Double lng = input.getDouble(3);

            if (icao.hashCode() % totalTasks == taskId) {
                // if the input belong to my task or it is newer than previous one, 
                // put it in the map
                Values vals = flightMap.get(icao);
                if (vals == null || (Long)vals.get(0) < posTime)
                    flightMap.put(icao, new Values(posTime, lat, lng));
            }
            else {
                for (Map.Entry<String, Values> entry : flightMap.entrySet()) {
                    String key = entry.getKey();        // icao
                    Values vals = entry.getValue();     // 0:posTime, 1:lat, 2:lng
                    double distKm = computeDist(lat, lng, 
                                                    (Double)vals.get(1), (Double)vals.get(2));
                    if (distKm <= distThresholdKm) {
                        String flight1 = key + ":" + vals.get(0) + ":" + 
                            vals.get(1) + ":" + vals.get(2);
                        String flight2 = icao + ":" + posTime + ":" + lat + ":" + lng;
                        // System.out.println(distKm + ", " + flight1 + ", " + flight2);
                        collector.emit(new Values(distKm, flight1, flight2));
                    }
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS_DIST, FIELDS_FLIGHT1, FIELDS_FLIGHT2)); 
        }
        
    }

    @Override
    public StormTopology getTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new KafkaSpout(spoutConf_), parallel_);
        builder.setBolt(LATLONG_FILTER_ID, new LatLongFilterBolt(), parallel_)
            .shuffleGrouping(SPOUT_ID);
        builder.setBolt(ROLLING_LATLONG_ID, 
                        new RollingLatLongBolt(windowLength_, emitFreq_), parallel_)
            .fieldsGrouping(LATLONG_FILTER_ID, new Fields(LatLongFilterBolt.FIELDS_ICAO));
        builder.setBolt(DIST_FILTER_ID, 
                        new DistFilterBolt(distThresholdKm_), parallel_)
            .allGrouping(ROLLING_LATLONG_ID);
        builder.setBolt(ROLLING_SORT_ID, 
                        new RollingSort.SortBolt(sortEmitFreq_, sortChunkSize_), 1)
            .globalGrouping(DIST_FILTER_ID);


        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        CollisionWarning app = new CollisionWarning(args);
        app.submitTopology(args[0]);
    }
}

