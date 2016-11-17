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

package intel.storm.benchmark.lib.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FilterBolt<T> extends BaseBasicBolt {
  private static final long serialVersionUID = -4957635695743420459L;
  public static final String FIELDS = "filtered";
  private final T toFilter;

  public FilterBolt(T toFilter) {
    this.toFilter = toFilter;
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    if (!filter(input, toFilter)) {
      collector.emit(new Values(input.getValue(1)));
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS));
  }

  public static <T> boolean filter(Tuple input, T toFilter) {
    return toFilter.equals(input.getValue(0));
  }
}
