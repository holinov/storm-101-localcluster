package com.mycompany.app;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;


public class JoinerBolt extends BaseRichBolt {

    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    private int ssCnt = 0;
    private int strCnt = 0;

    public void execute(Tuple input) {
        final String string = input.getString(0);
        if (string.startsWith("SS")) {
            ssCnt++;
        } else {
            strCnt++;
        }
        System.out.println("Msg: " + string);
        System.out.println("SS: " + ssCnt + " Str: " + strCnt);

        if (ssCnt == 100 && strCnt == 100) {
            System.out.println("FINISHED");
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
