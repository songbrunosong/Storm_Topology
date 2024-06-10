package com.brunosong.storm;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

public class KafkaSpoutPrintBoltTopology {

    public static void main(String[] args) throws AuthorizationException, InvalidTopologyException, AlreadyAliveException {

        String bootstrapServers = "localhost:9092";
        String kafkaTopic = "SmartCar-Topic";
        String zkRoot = "/kafka-spout";
        String consumerGroupId = "storm-consumer-group";

        KafkaSpoutConfig<String, String> spoutConfig = KafkaSpoutConfig.builder(bootstrapServers, kafkaTopic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId)
                .build();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout<>(spoutConfig), 1);
        builder.setBolt("print-bolt", new PrintBolt(), 1).shuffleGrouping("kafka-spout");

        Config config = new Config();
        config.setDebug(true);

        StormSubmitter.submitTopology(args[0], config, builder.createTopology());

    }




}
