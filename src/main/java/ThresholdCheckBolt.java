import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Naver on 14. 3. 22.
 */
public class ThresholdCheckBolt extends BaseBasicBolt {
    HashMap<String, Integer> map = new HashMap<String, Integer>();

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            String ruleId = tuple.getString(0);
            String log = tuple.getString(1);
            if (map.containsKey(ruleId)) {
                map.put(ruleId, map.get(ruleId) + 1);
            } else {
                map.put(ruleId, 1);
            }

            if (map.get(ruleId) == 10) {
                collector.emit(new Values(ruleId, log));
                map.remove(ruleId);
            }
        } catch (Exception e) {
            System.out.println(e.getStackTrace());
            collector.reportError(e);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ruleId", "log"));
    }
}