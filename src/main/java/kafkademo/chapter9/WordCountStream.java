package kafkademo.chapter9;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;

/**
 * 使用告诫KStream DSL统计一个流数据单词频率
 * @author Pinkboy
 *
 */
public class WordCountStream {

 
    public static void main(String[] args) {
        Properties props = new Properties();// 实例化
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my_wordcount"); //配置一个应用ID
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,  "localhost:9091,localhost:9092,localhost:9093"); // 配置Kafka集群地址
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); // 设置序列化与反序列类键属性
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); // 设置序列化与反序列类值属性
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 设置偏移量重置属性
        
        KStreamBuilder builder = new KStreamBuilder(); // 实例化一个流处理构建对象
        KStream<String, String> source = builder.stream("streams_wordcount_input");
        
        
        // 执行统计单词逻辑
        KTable<String, Long> counts =  source.flatMapValues(new ValueMapper<String, Iterable<String>>() {

            @Override
            public Iterable<String> apply(String value) {
                // TODO Auto-generated method stub
                System.out.println("flatMapValues");
                return Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" "));
            }
        }).map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
            @Override
            public KeyValue<String, String> apply(String key, String value) {
                System.out.println("map");
                System.out.println("key =" + key);
                System.out.println("value = " + value);
                System.out.println();
                return new KeyValue<>(value, value);
            }
        }).groupByKey().count("counts");
        counts.foreach(new ForeachAction<String, Long>() {

            @Override
            public void apply(String key, Long value) {
                // TODO Auto-generated method stub
                System.out.println(key + " = " + value);
            }
        });
        //counts.print();
        
        
        KafkaStreams streams = new KafkaStreams(builder,props);
        streams.start();

    }
    
 /*   public static void main(String[] args) throws Exception {
        Properties props = new Properties(); // 实例化一个属性对象
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstreams_wordcount"); // 配置一个应用ID
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091,localhost:9092,localhost:9093"); // 配置Kafka集群地址
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); // 设置序列化与反序列类键属性
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); // 设置序列化与反序列类值属性

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 设置偏移量重置属性

        KStreamBuilder builder = new KStreamBuilder(); // 实例化一个流处理构建对象

        KStream<String, String> source = builder.stream("streams_wordcount_input"); // 指定一个输入流主题

        // 执行统计单词逻辑
        KTable<String, Long> counts = source.flatMapValues(new ValueMapper<String, Iterable<String>>() {
            @Override
            public Iterable<String> apply(String value) {
                return Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" "));
            }
        }).map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
            @Override
            public KeyValue<String, String> apply(String key, String value) {
                return new KeyValue<>(value, value);
            }
        }).groupByKey().count("counts");

        counts.print(); // 输出统计结果

        KafkaStreams streams = new KafkaStreams(builder, props); // 实例化一个流处理对象
        streams.start(); // 执行流处理
    }*/
}
