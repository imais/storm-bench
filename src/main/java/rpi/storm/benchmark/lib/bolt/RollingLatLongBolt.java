/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package rpi.storm.benchmark.lib.bolt;

import org.apache.log4j.Logger;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import rpi.storm.benchmark.lib.reducer.LatLongReducer;
import intel.storm.benchmark.tools.SlidingWindow;
import intel.storm.benchmark.lib.bolt.RollingBolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


public class RollingLatLongBolt extends RollingBolt {

    private static final Logger LOG = Logger.getLogger(RollingLatLongBolt.class);
    public static final String FIELDS_ICAO = "icao";
    public static final String FIELDS_LAT = "lat";
    public static final String FIELDS_LONG = "long";
    public static final String FIELDS_POSTIME = "postime";

    private final SlidingWindow<Object, Values> window;

    public RollingLatLongBolt(int winLen, int emitFreq) {
        super(winLen, emitFreq);
        window = new SlidingWindow<Object, Values>(new LatLongReducer(), getWindowChunks());
    }

    @Override
    public void emitCurrentWindow(BasicOutputCollector collector) {
        emitCurrentWindowLatLong(collector);
    }

    @Override
    public void updateCurrentWindow(Tuple tuple) {
        updateLatLong(tuple);
    }

    private void emitCurrentWindowLatLong(BasicOutputCollector collector) {
        Map<Object, Values> latlongs = window.reduceThenAdvanceWindow();
        for (Entry<Object, Values> entry : latlongs.entrySet()) {
            Object obj = entry.getKey();
            Values vals = (Values)entry.getValue();
            // System.out.println("[RLLB.emit]: " + obj + ", " + 
            //                    vals.get(0) + ", " + vals.get(1) + ", " + vals.get(2));
            collector.emit(new Values(obj, vals.get(0), vals.get(1), vals.get(2)));
        }
    }

    private void updateLatLong(Tuple tuple) {
        Object obj = tuple.getValue(0);                 // Icao
        Values vals = new Values(tuple.getValue(1),     // PosTime
                                 tuple.getValue(2),     // Lat
                                 tuple.getValue(3));    // Long
        window.add(obj, vals);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FIELDS_ICAO, FIELDS_POSTIME, FIELDS_LAT, FIELDS_LONG));
    }
}
