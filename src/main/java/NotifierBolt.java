import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by Naver on 14. 3. 22.
 */
public class NotifierBolt extends BaseBasicBolt {
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            String ruleId = tuple.getString(0);
            String log = tuple.getString(1);

            System.out.println(ruleId + " occured 10 times.\n last log : \n" + log);
            //trigger alert.....
        } catch (Exception e) {
            System.out.println(e.getStackTrace());
            collector.reportError(e);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}