package com.mycompany.app;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by kholinow on 14.07.16.
 */
public class MySpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void nextTuple() {
        for (int i = 0; i < 100; i++) {
            collector.emit(new Values("Str: " + i));
        }

        for (int i = 0; i < 100; i++) {
            collector.emit("str1", new Values("SS: " + i));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("val"));
        declarer.declareStream("str1", new Fields("val"));
    }
}
