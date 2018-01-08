package analytics;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException
    {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("page-visits", new PageVisitSpout());
        builder.setBolt("user-page-visit-counts", new UserPageVisitCount()
                .withWindow(BaseWindowedBolt.Duration.of(1000*60*60), BaseWindowedBolt.Duration.of(1000*30)))
                .fieldsGrouping("page-visits", new Fields("url"));

        StormTopology topology = builder.createTopology();
        Config config = new Config();
        config.setMessageTimeoutSecs((60*60) + 40);
        String topologyName = "analytics-topology";

        if (args.length > 1 && args[0].equals("remote")) {
            StormSubmitter.submitTopology(topologyName, config, topology);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, config, topology);
        }
    }
}
