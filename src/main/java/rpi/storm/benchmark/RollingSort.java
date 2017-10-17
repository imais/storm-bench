package rpi.storm.benchmark;

import backtype.storm.Config;
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

import intel.storm.benchmark.util.TupleHelpers;
import yahoo.benchmark.common.Utils;
import rpi.storm.benchmark.common.BenchmarkBase;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


public class RollingSort extends BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(RollingSort.class);

    public static final String SPOUT_ID = "spout";
    public static final String SORT_BOLT_ID ="sort";
    
    private int emitFreq_;
    private int chunkSize_;

    public RollingSort(String[] args) throws ParseException {
        super(args);
        emitFreq_ = getConfInt(globalConf_, "rolling_sort.emit_freq");
        chunkSize_ = getConfInt(globalConf_, "rolling_sort.chunk_size");
    }

    @Override
    public StormTopology getTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new KafkaSpout(spoutConf_), parallel_);
        builder.setBolt(SORT_BOLT_ID, new SortBolt(emitFreq_, chunkSize_), parallel_)
            .localOrShuffleGrouping(SPOUT_ID);

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        RollingSort app = new RollingSort(args);
        app.submitTopology(args[0]);
    }

    public static class SortBolt extends BaseBasicBolt {
        public static final String EMIT_FREQ = "emit.frequency";
        public static final int DEFAULT_EMIT_FREQ = 60;  // 60s
        public static final String CHUNK_SIZE = "chunk.size";
        public static final int DEFAULT_CHUNK_SIZE = 100;
        public static final String FIELDS = "sorted_data";

        private final int emitFrequencyInSeconds;
        private final int chunkSize;
        private int index = 0;
        private MutableComparable[] data;


        public SortBolt(int emitFrequencyInSeconds, int chunkSize) {
            this.emitFrequencyInSeconds = emitFrequencyInSeconds;
            this.chunkSize = chunkSize;
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            this.data = new MutableComparable[this.chunkSize];
            for (int i = 0; i < this.chunkSize; i++) {
                this.data[i] = new MutableComparable();
            }
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            if (TupleHelpers.isTickTuple(tuple)) {
                Arrays.sort(data);
                basicOutputCollector.emit(new Values(data));
                log.info("index = " + index);
            } else {
                Object obj = tuple.getValue(0);
                if (obj instanceof Comparable) {
                    data[index].set((Comparable) obj);
                } else {
                    throw new RuntimeException("tuple value is not a Comparable");
                }
                index = (index + 1 == chunkSize) ? 0 : index + 1;
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields(FIELDS));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            Map<String, Object> conf = new HashMap<String, Object>();
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
            return conf;
        }
    }

    private static class MutableComparable implements Comparable, Serializable {
        private static final long serialVersionUID = -5417151427431486637L;
        private Comparable c = null;

        public MutableComparable() {

        }

        public MutableComparable(Comparable c) {
            this.c = c;
        }

        public void set(Comparable c) {
            this.c = c;
        }

        public Comparable get() {
            return c;
        }

        @Override
        public int compareTo(Object other) {
            if (other == null) return 1;
            Comparable oc = ((MutableComparable) other).get();
            if (null == c && null == oc) {
                return 0;
            } else if (null == c) {
                return -1;
            } else if (null == oc) {
                return 1;
            } else {
                return c.compareTo(oc);
            }
        }
    }
}
