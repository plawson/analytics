package analytics;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;

public class UserPageVisitCount extends BaseWindowedBolt {

    @Override
    public void execute(TupleWindow inputWindow) {
        HashMap<String, HashMap<Integer, Long>> userPageVisits = new HashMap<>();
        String now = new SimpleDateFormat("yyy-MM-dd HH:mm:ss").format(new Date());
        System.out.println("=====================================================================================");
        for (Tuple input: inputWindow.get()) {
            String url = input.getStringByField("url");
            Integer userId = input.getIntegerByField("userId");

            userPageVisits.putIfAbsent(url, new HashMap<>());
            userPageVisits.get(url).putIfAbsent(userId, (long) 0);
            userPageVisits.get(url).put(userId, userPageVisits.get(url).get(userId) + 1);
        }

        for (Entry<String, HashMap<Integer, Long>> pageStats: userPageVisits.entrySet()) {
            String page = pageStats.getKey();
            for (Entry<Integer, Long> userStats: pageStats.getValue().entrySet()) {
                Integer userId = userStats.getKey();
                Long pageVisits = userStats.getValue();
                System.out.printf("%s => Page: %s, user ID: %d, page visits in the last hour: %d\n", now, page, userId, pageVisits);
            }
        }
    }
}
