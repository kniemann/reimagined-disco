package org;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;


/**
 * Created by kevin on 3/12/2016.
 */
public class BasicConsumeLoop implements Runnable {
    static Logger logger = Logger.getLogger(App.class);
    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean shutdown;
    private final CountDownLatch shutdownLatch;

    public BasicConsumeLoop() {
        Properties props = new Properties();
        //props.put("zookeeper.connect", "172.28.128.3:2181/kafka");
        props.put("group.id", "group1");
        props.put("schema.registry.url", "http://172.28.128.3:8081");
        props.put("bootstrap.servers","172.28.128.3:9092");
        props.put("client.id","KafkaTestApp_Consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        this.consumer = new KafkaConsumer<>(props);
        List<String> new_topics = new ArrayList<String>();
        this.shutdown = new AtomicBoolean(false);
        this.shutdownLatch = new CountDownLatch(1);
    }

    public void process(ConsumerRecord<String, String> record) {
        logger.info("Processing record:");
        logger.info(record);
    }

    public void run() {
        try {
            consumer.subscribe(Arrays.asList("foo", "bar"));
            while (!shutdown.get()) {
                //System.out.println("Listening for record.");
                ConsumerRecords<String, String> records = consumer.poll(500);

                //System.out.println("Processing record.");
                records.forEach(record -> process(record));
                //System.out.println("Finished.");


            }
        } finally {
            consumer.close();
            shutdownLatch.countDown();
        }
    }

    public void shutdown() throws InterruptedException {
        shutdown.set(true);
        shutdownLatch.await();
    }
}
