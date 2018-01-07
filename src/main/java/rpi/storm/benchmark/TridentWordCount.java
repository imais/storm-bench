package rpi.storm.benchmark;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.spout.SchemeAsMultiScheme;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.testing.MemoryMapState;

import intel.storm.benchmark.lib.operation.WordSplit;
import rpi.storm.benchmark.common.BenchmarkBase;

public class TridentWordCount extends BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(TridentWordCount.class);

    public static final String SPOUT_ID = "spout";
    public static final String SPLIT_ID = "split";
    public static final String COUNT_ID = "count";

    private IPartitionedTridentSpout spout;

    public TridentWordCount(String[] args) throws ParseException {
        super(args);
    }

    @Override
    public StormTopology getTopology() {
        TridentKafkaConfig kafkaConf = 
            new TridentKafkaConfig(spoutConf_.hosts, spoutConf_.topic, spoutConf_.clientId);
        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spout  = new TransactionalTridentKafkaSpout(kafkaConf);

        TridentTopology trident = new TridentTopology();

        trident.newStream("wordcount", spout).name("sentence").parallelismHint(spouts_parallel_).shuffle()
            .each(new Fields(StringScheme.STRING_SCHEME_KEY), new WordSplit(), new Fields("word"))
            .parallelismHint(bolts_parallel_)
            .groupBy(new Fields("word"))
            .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
            .parallelismHint(bolts_parallel_);

        return trident.build();
    }

    public static void main(String[] args) throws Exception {
        TridentWordCount app = new TridentWordCount(args);
        app.submitTopology(args[0]);
    }
}
