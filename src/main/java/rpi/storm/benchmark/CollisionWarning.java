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
    public static final String DIST_FILTER_ID = "dist_filter";
    public static final String ROLLING_SORT_ID = "rolling_sort";

    private double distThresholdKm_;
    private int speculativeCompNum_;
    private int speculativeCompTimeStepSec_;
    private int sortEmitFreq_;
    private int sortChunkSize_;
    private boolean logTopDataOnly_;

    public CollisionWarning(String[] args) throws ParseException {
        super(args);
        distThresholdKm_ = getConfInt(globalConf_, "collision_warning.dist_threshold_km");
        speculativeCompNum_ = 
            getConfInt(globalConf_, "collision_warning.speculative_comp_num");
        speculativeCompTimeStepSec_ = 
            getConfInt(globalConf_, "collision_warning.speculative_comp_timestep_sec");
        sortEmitFreq_ = getConfInt(globalConf_, "collision_warning.sort_emit_freq");
        sortChunkSize_ = getConfInt(globalConf_, "collision_warning.sort_chunk_size");
        logTopDataOnly_ = getConfBoolean(globalConf_, "collision_warning.sort_log_top_data_only");
    }

    public static class LatLongFilterBolt extends BaseBasicBolt {
        public static final String FIELDS_ICAO = "icao";
        public static final String FIELDS_POSTIME = "postime";
        public static final String FIELDS_LAT = "lat";
        public static final String FIELDS_LONG = "long";
        public static final String FIELDS_SPD = "spd";
        public static final String FIELDS_TRAK = "trak";
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
                    obj.has("Long") && !obj.isNull("Long") &&
                    obj.has("Spd") && !obj.isNull("Spd") &&
                    obj.has("Trak") && !obj.isNull("Trak")) {
                    String icao = obj.getString("Icao");
                    long posTime = obj.getLong("PosTime");
                    double lat = obj.getDouble("Lat");
                    double lng = obj.getDouble("Long");
                    double spd = obj.getDouble("Spd");
                    double trak = obj.getDouble("Trak");

                    if (!obj.has("Gnd") || obj.isNull("Gnd") || !obj.getBoolean("Gnd")) {
                        // if Gnd is true, do not emit
                        collector.emit(new Values(icao, posTime, lat, lng, spd, trak));
                    }
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS_ICAO, FIELDS_POSTIME, FIELDS_LAT, 
                                        FIELDS_LONG, FIELDS_SPD, FIELDS_TRAK));
        }
    }

    public static class DistFilterBolt extends BaseBasicBolt {
        public static final String FIELDS_DIST = "dist";
        public static final String FIELDS_POSTIME = "posTime";
        public static final String FIELDS_FLIGHT1 = "flight1";
        public static final String FIELDS_FLIGHT2 = "flight2";

        private static int taskId;
        private static int totalTasks;
        private static Map<String, Values> flightMap;
        private static double EARTH_RADIUS_KM = 6378.137; // mean radius in kilometer
        private static double KNOT_TO_KM_PER_SEC = 0.000514444;

        private double distThresholdKm;
        private int speculativeCompNum;
        private int speculativeCompTimeStepSec;
        
        public DistFilterBolt(double distThresholdKm, 
                              int speculativeCompNum, int speculativeCompTimeStepSec) {
            this.distThresholdKm = distThresholdKm;
            this.speculativeCompNum = speculativeCompNum;
            this.speculativeCompTimeStepSec = speculativeCompTimeStepSec;
        }
        
        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            int orgTaskId = context.getThisTaskId();
            totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
            taskId = orgTaskId % totalTasks;
            log.info("original taskId: " + orgTaskId + ", taskId: " + taskId + 
                     ", totalTasks: " + totalTasks);
            flightMap = new HashMap<String, Values>();
        }

        public Values computeLatLong(double lat1, double lng1, double bearing, double distKm) {
            lat1 = Math.toRadians(lat1);
            lng1 = Math.toRadians(lng1);
            bearing = Math.toRadians(bearing);
            double relativeDist = distKm / EARTH_RADIUS_KM;

            double lat2 = Math.asin(Math.sin(lat1) * Math.cos(relativeDist) + 
                                    Math.cos(lat1) * Math.sin(relativeDist) * Math.cos(bearing));
            double lng2 = lng1 + Math.atan2(Math.sin(bearing) * 
                                            Math.sin(relativeDist) * Math.cos(lat1),
                                            Math.cos(relativeDist) - Math.sin(lat1) * Math.sin(lat2));

            return new Values(Math.toDegrees(lat2), Math.toDegrees(lng2));
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

        private long lastLogDisplayedTimeMs = 0;
        private long numTuplesRecvd = 0;
        private long numPairsComputed = 0;
        private long numTuplesEmitted = 0;

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String icao1 = input.getString(0);
            Long posTime1 = input.getLong(1);
            Double lat1 = input.getDouble(2);
            Double lng1 = input.getDouble(3);
            Double spd1 = input.getDouble(4) * KNOT_TO_KM_PER_SEC;
            Double trak1 = input.getDouble(5);
            
            numTuplesRecvd++;
            
            long deltaTime = System.currentTimeMillis() - lastLogDisplayedTimeMs;
            if (deltaTime >= 3000) {
                double recvdTp = (double)numTuplesRecvd / deltaTime;
                double pairsTp = (double)numPairsComputed / deltaTime;
                double emittedTp = (double)numTuplesEmitted / deltaTime;
                log.info("recvdTp/emittedTp/map.size = " + 
                         recvdTp + "/" + emittedTp + "/" + flightMap.size());
                numTuplesRecvd = 0;
                numPairsComputed = 0;
                numTuplesEmitted = 0;
                lastLogDisplayedTimeMs = System.currentTimeMillis();
            }

            if (icao1.hashCode() % totalTasks == taskId) {
                // if the input belongs to my task or it is newer than previous one, 
                // put it in the map
                Values vals = flightMap.get(icao1);
                if (vals == null || (Long)vals.get(0) < posTime1)
                    flightMap.put(icao1, new Values(posTime1, lat1, lng1, spd1, trak1));
            }
            else {
                for (Map.Entry<String, Values> entry : flightMap.entrySet()) {
                    String icao2 = entry.getKey();
                    Values vals = entry.getValue();     
                    Long posTime2 = (Long)vals.get(0);        
                    Double lat2 = (Double)vals.get(1);
                    Double lng2 = (Double)vals.get(2);
                    Double spd2 = (Double)vals.get(3);
                    Double trak2 = (Double)vals.get(4);
                    Long currTime = Math.max(posTime1, posTime2);  // epoch in millisec

                    for (int i = 0; i < speculativeCompNum; i++) {
                        double dist1 = spd1 * (currTime - posTime1) / 1000; // in km
                        double dist2 = spd2 * (currTime - posTime2) / 1000; // in km

                        Values latLng1 = computeLatLong(lat1, lng1, trak1, dist1);
                        Values latLng2 = computeLatLong(lat2, lng2, trak2, dist2);
                        double distKm = computeDist((Double)latLng1.get(0), (Double)latLng1.get(1),
                                                    (Double)latLng2.get(0), (Double)latLng2.get(1));
                        numPairsComputed++;

                        if (distKm <= distThresholdKm) {
                            String flight1 = icao1 + ":(" + latLng1.get(0) + "," + latLng1.get(1) + ")";
                            String flight2 = icao2 + ":(" + latLng2.get(0) + "," + latLng2.get(1) + ")";

                            if (0 < icao1.compareTo(icao2)) {
                                String temp = flight1;
                                flight1 = flight2;
                                flight2 = temp;
                            }
                            log.debug(
                                distKm + ", " + currTime +
                                ", [" + icao1 + ":(" + latLng1.get(0) + "," + latLng1.get(1) + "):(" + lat1 + "," + lng1 + ")," + posTime1 + "," + spd1 + "," + trak1 + "]" +
                                ", [" + icao2 + ":(" + latLng2.get(0) + "," + latLng2.get(1) + "):(" + lat2 + "," + lng2 + ")," + posTime2 + "," + spd2 + "," + trak2 + "]");
                            collector.emit(new Values(distKm, currTime, flight1, flight2));
                            numTuplesEmitted++;
                        }
                        currTime += (1000 * speculativeCompTimeStepSec);
                    }
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS_DIST, FIELDS_POSTIME, FIELDS_FLIGHT1, FIELDS_FLIGHT2)); 
        }
        
    }

    @Override
    public StormTopology getTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new KafkaSpout(spoutConf_), spouts_parallel_);
        builder.setBolt(LATLONG_FILTER_ID, new LatLongFilterBolt(), bolts_parallel_)
            .shuffleGrouping(SPOUT_ID);
        builder.setBolt(DIST_FILTER_ID, 
                        new DistFilterBolt(distThresholdKm_, speculativeCompNum_, 
                                           speculativeCompTimeStepSec_), bolts_parallel_)
            .allGrouping(LATLONG_FILTER_ID);
        // builder.setBolt(ROLLING_SORT_ID, 
        //                 new RollingSort.SortBolt(sortEmitFreq_, sortChunkSize_, 
        //                                          logTopDataOnly_), 1)
        //     .globalGrouping(DIST_FILTER_ID);

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        CollisionWarning app = new CollisionWarning(args);
        app.submitTopology(args[0]);
    }
}

