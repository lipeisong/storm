package com.imooc.bigdata.Integration.redis;

import org.apache.commons.io.FileUtils;
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

import java.io.File;
import java.io.IOException;
import java.util.*;

public class LocalWordCountStromRedisTopology {
    public static class FileSpout extends BaseRichSpout{
        private SpoutOutputCollector collector;

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector=collector;
        }

        //读取文件，发送读取文件内容每一行数据
        public void nextTuple() {
            //获取某个文件夹下面的所有文件
            Collection<File> files = FileUtils.listFiles(new File("C:\\Users\\123\\Desktop\\wordcount"), new String[]{"txt"}, true);

            for (File file : files) {
                try {
                    List<String> lines= FileUtils.readLines(file);

                    for (String line : lines) {
                        this.collector.emit(new Values(line));
                    }

                    //将文件改名，否则会一直在读这个文件
                    FileUtils.moveFile(file,new File(file.getAbsolutePath()+System.currentTimeMillis()));

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
                 declarer.declare(new Fields("line"));
        }
    }

    //把接受到每一行数据进行切割
    public static class SplitBolt extends BaseRichBolt{
        private OutputCollector collector;

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector=collector;
        }

        public void execute(Tuple input) {
            String line = input.getStringByField("line");
            String[] words = line.split(",");

            if(words!=null && words.length>0){
                for (String word : words) {
                    collector.emit(new Values(word));
                }
            }

        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
             declarer.declare(new Fields("word"));
        }
    }

    //对切割后的字符串进行统计
    public static class WordCountBolt extends BaseRichBolt{

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }
        public Map<String,Integer> map=new HashMap<String, Integer>();

        public void execute(Tuple input) {
            String word = input.getStringByField("word");
            Integer count = map.get(word);
            if(count==null){
                count=0;
            }
            count++;
            map.put(word,count);

            System.out.println("==============================");

            Set<Map.Entry<String, Integer>> entries = map.entrySet();
            for (Map.Entry<String, Integer> entry : entries) {
                System.out.println(entry.getKey()+":"+entry.getValue());
            }


        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {

        //创建TopologyBuilder
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("FileSpout",new FileSpout());
        builder.setBolt("SplitBolt",new SplitBolt()).shuffleGrouping("FileSpout");
        builder.setBolt("WordCountBolt",new WordCountBolt()).shuffleGrouping("SplitBolt");



        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("LocalWordCountStromTopology",new Config(),builder.createTopology());
    }
}
