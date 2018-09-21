package com.imooc.bigdata;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class FieldGroupingStromTopology {
    public static class DataSourceSpout extends BaseRichSpout{

        private SpoutOutputCollector spoutOutputCollector;
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.spoutOutputCollector=spoutOutputCollector;
        }

        private int number=0;
        //这个方法是个死循环，会不断的执行
        public void nextTuple() {
            this.spoutOutputCollector.emit(new Values(number%2,++number));

            System.out.println("Spout:"+number);

            //防止产生数据太快
            Utils.sleep(1000);
        }

        //声明发出的tuple的字段
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            //声明发出的tuple的名称
            outputFieldsDeclarer.declare(new Fields("flag","num"));
        }
    }


    public static class SumBolt extends BaseRichBolt{

        //也只会被执行一次
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        }

        int sum=0;
        //这其实也是一个死循环，接受数据并处理
        public void execute(Tuple tuple) {
            Integer value = tuple.getIntegerByField("num");

            sum+=value;

//            System.out.println("Bolt:Sum="+sum);

            System.out.println("Thread:"+Thread.currentThread().getId()+",receciveData:"+value);
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }

    public static void main(String[] args) {
        //创建一个topologyBuilder
        TopologyBuilder builder = new TopologyBuilder();

        //设置Spout和Bolt
        builder.setSpout("DataSourceSpout",new DataSourceSpout());
        //设置Bolt，而且设置Bolt的数据来源于哪里？
        builder.setBolt("SumBolt",new SumBolt(),3).fieldsGrouping("DataSourceSpout",new Fields("flag"));



        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("LocalSumStromTopology",new Config(),builder.createTopology());
    }
}
