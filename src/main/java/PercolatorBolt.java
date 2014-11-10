import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.Map;

import static org.elasticsearch.action.percolate.PercolateSourceBuilder.docBuilder;

/**
 * Created by Naver on 14. 3. 22.
 */
public class PercolatorBolt extends BaseBasicBolt {
    private Client client;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        Settings settings =
                ImmutableSettings.settingsBuilder().put("cluster.name", "your-cluster-name").put("client.transport.sniff", "true").build();
        client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("your-es-host", 9300));
        super.prepare(stormConf, context);
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            String log = tuple.getString(0);
            PercolateResponse response = client.preparePercolate()
                    .setIndices("your-index-name")
                    .setDocumentType(".percolator")
                    .setPercolateDoc(docBuilder().setDoc(log))
                    .execute().actionGet();
            ;
            if (response.getCount() > 0) {
                for (PercolateResponse.Match match : response) {
                    String ruleId = match.getId().toString();
                    collector.emit(new Values(ruleId, log));
                }
            }
        } catch (Exception e) {
            collector.reportError(e);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ruleId", "log"));
    }
}
