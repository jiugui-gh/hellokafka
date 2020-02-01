package kafkademo.chapter5.deserialize;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * 实现一个消费者实例代码
 * @author Pinkboy
 *
 */
public class JConsumerDeserialize extends Thread {

    public static void main(String[] args) {
        JConsumerDeserialize jConsumerDeserialize = new JConsumerDeserialize();
        jConsumerDeserialize.start();
    }
    
    // 初始化kafka集群信息
    private Properties configure() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9091,localhost:9092,localhost:9093");// 指定Kafka集群地址        
        props.put("group.id", "myfirstgroup"); // 指定消费者组
        props.put("enable.auto.commit", "true"); //开启自动提交
        props.put("auto.commit.interval.ms", "1000"); // 自动提交的时间间隔
        // 反序列化消息主键
        props.put("key.deserializer", "kafkademo.chapter5.deserialize.JSalaryDeserializer");
        // 反序列化消费记录
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
    
    @Override
    public void run() {
        // TODO Auto-generated method stub
        // 创建一个消费者实例对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configure());
        consumer.subscribe(Arrays.asList("mytopic"));
        
        boolean flag = true;
        while(flag) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
        consumer.close();
    }
}
