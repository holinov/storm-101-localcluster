package com.mycompany.app;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.io.IOException;

public class JoinerTopologyTest {

    public static void main(String[] args) throws IOException {
        Config conf = new Config();
        conf.setNumWorkers(5);
        conf.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("SPOUT-1",new MySpout(),1);
        builder.setBolt("BOLT-1",new Bolt1(), 3)
                .shuffleGrouping("SPOUT-1");
        builder.setBolt("JOINER", new JoinerBolt(),1)
                .shuffleGrouping("BOLT-1")
                .shuffleGrouping("SPOUT-1","str1");

        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TOPO1",conf,builder.createTopology());


        System.in.read();

        cluster.shutdown();
    }
}
