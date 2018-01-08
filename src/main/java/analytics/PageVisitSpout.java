package analytics;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class PageVisitSpout extends BaseRichSpout {

    private SpoutOutputCollector outputCollector;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.outputCollector = collector;
    }

    public void nextTuple() {
        String[] urls = {"http://example.com/index.html", "http://example.com/404.html",
                "http://example.com/subscribe.html"};
        Integer[] userIds = {1, 2, 3, 4, 5};

        String url = urls[ThreadLocalRandom.current().nextInt(urls.length)];
        Integer userId = userIds[ThreadLocalRandom.current().nextInt(userIds.length)];

        Values values = new Values(url, userId);
        this.outputCollector.emit(values, values);
        Utils.sleep(2000);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "userId"));
    }

    @Override
    public void ack(Object msgId) {
        System.out.printf("Correctly processed: %s\n", msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.printf("ERROR processing: %s\n", msgId);
        Values tuple = (Values)msgId;
        this.outputCollector.emit(tuple, msgId);
    }
}
