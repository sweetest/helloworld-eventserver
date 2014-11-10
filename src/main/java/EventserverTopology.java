import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.io.InputStream;
import java.text.ParseException;
import java.util.Map;

/**
 * Created by Naver on 14. 3. 24.
 */
public class EventserverTopology {
    public static void main(String[] args) {
        try {
            ZkHosts zkBrokerHosts = new ZkHosts("your-zookeeper-host", "your-kafka-broker-dir-in-zookeeper");
            SpoutConfig logSpoutConfig = new SpoutConfig(zkBrokerHosts,
                    "your-topic-name",
                    "/kafka/consumers/eventserver/logs",
                    "logs");
            logSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            logSpoutConfig.startOffsetTime = -2;//-2 from beggining, -1 from the latest
            logSpoutConfig.forceFromStart = true;
            logSpoutConfig.fetchSizeBytes = 4 * 1024 * 1024;
            KafkaSpout logSpout = new KafkaSpout(logSpoutConfig);

            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("logSpout", logSpout, 1);
            builder.setBolt("percolatorBolt", new PercolatorBolt(), 1)
                    .shuffleGrouping("logSpout");
            builder.setBolt("thresholdCheckBolt", new ThresholdCheckBolt(), 1).fieldsGrouping("percolatorBolt", new Fields("ruleId"));
            builder.setBolt("notifierBolt", new NotifierBolt(), 1).fieldsGrouping("thresholdCheckBolt", new Fields("ruleId"));

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("your-topology-name", null, builder.createTopology());
        } catch (Exception e) {
            System.out.println(e.getStackTrace());
        }
    }
}
