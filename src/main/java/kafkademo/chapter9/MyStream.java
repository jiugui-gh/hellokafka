package kafkademo.chapter9;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyStream {
    private final Logger LOG = LoggerFactory.getLogger(MyStream.class);
    
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "myStream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091,localhost:9092,localhost:9093");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, "kafkademo.chapter9.MyDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 设置偏移量重置属性
  
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, Object> stream = builder.stream("mytopic");
        
        stream.map(new KeyValueMapper<String, Object, KeyValue<? extends String,? extends Object>>() {

            @Override
            public KeyValue<? extends String, ? extends Object> apply(String key, Object value) {
                // TODO Auto-generated method stub
                return new KeyValue<String, Object>(key, value);
            }

            
        });
        stream.print();
        
        KafkaStreams kafkaStreams = new KafkaStreams(builder,props);
        kafkaStreams.start();
    }
}
