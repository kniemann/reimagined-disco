package org;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.codehaus.jackson.schema.JsonSchema;

/**
 * Hello world!
 *
 */
public class App 
{
    static Logger logger = Logger.getLogger(App.class);



    public static void main( String[] args ) throws ExecutionException, InterruptedException, IOException {
        Properties log_props = new Properties();
        log_props.load(new FileInputStream("resources/log4j.properties"));
        PropertyConfigurator.configure(log_props);

        //BasicConfigurator.configure();
        logger.info("Starting MyKafkaApp.");
        String topic = "foo";
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://172.28.128.3:8081");
        props.put("bootstrap.servers","172.28.128.3:9092");
        //props.put("broker.list","172.28.128.3:9092");
        props.put("client.id","KafkaTestApp_Producer");

        User user1 = new User();
        user1.setName("Kevin");
        user1.setFavoriteNumber(256);

        ObjectMapper m = new ObjectMapper();
        SchemaFactoryWrapper visitor = new SchemaFactoryWrapper();
        m.acceptJsonFormatVisitor(m.constructType(User.class), visitor);
        com.fasterxml.jackson.module.jsonSchema.JsonSchema jsonSchema = visitor.finalSchema();
        String userSchema = m.writerWithDefaultPrettyPrinter().writeValueAsString(jsonSchema);
        logger.info(userSchema);
        // Set any other properties
        KafkaProducer producer = new KafkaProducer(props);
        String key = "key1";
        String userExampleSchema = "{\"type\":\"record\"," +
                "\"name\":\"myrecord\"," +
                "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
       // Schema schema = parser.parse(userExampleSchema);
       // GenericRecord avroRecord = new GenericData.Record(schema);
        //avroRecord.put("f1", "value1");

        //User Schema
        Schema schema = parser.parse(userSchema);
        GenericRecord userAvroRecord = new GenericData.Record(schema);
        userAvroRecord.put("name", "Kevin");



        final ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, key, userAvroRecord);
        try {
            Future future = producer.send(record);
            RecordMetadata md = (RecordMetadata)future.get();
            logger.info("Topic:" + md.topic() +"\n Offset:" +md.offset()+ "\n Partition:"+md.partition());

        } catch(SerializationException e) {
            // may need to do something with it
        }

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

        BasicConsumeLoop consumer = new BasicConsumeLoop();
        executor.execute(consumer);

//
//        final ProducerRecord<Object, Object> record = new ProducerRecord<>("MyKafkaApp.data", "data", user1);
//        Future<RecordMetadata> future = producer.send(record);
//        System.out.println(future.get());




    }
}
