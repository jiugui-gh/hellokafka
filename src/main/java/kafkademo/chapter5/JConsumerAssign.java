package kafkademo.chapter5;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * 实现一个手动分配分区的消费者实例
 * @author Pinkboy
 *
 */
public class JConsumerAssign extends Thread {

    public static void main(String[] args) {
        JConsumerAssign jconsumer = new JConsumerAssign();
        jconsumer.start();
    }
    
    private Properties configure() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9091,localhost:9092,localhost:9093");// 指定Kafka集群地址        
        props.put("group.id", "myfirstgroup"); // 指定消费者组
        props.put("enable.auto.commit", "true"); //开启自动提交
        props.put("auto.commit.interval.ms", "1000"); // 自动提交的时间间隔
        // 反序列化消息主键
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 反序列化消费记录
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return props;
    }
    
    @Override
    public void run() {
        // TODO Auto-generated method stub
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(configure());
        // 设置自定义分区
        TopicPartition tp0 = new TopicPartition("mytopic", 0);
        TopicPartition tp1 = new TopicPartition("mytopic", 1);
        TopicPartition tp2 = new TopicPartition("mytopic", 2);
        TopicPartition tp3 = new TopicPartition("mytopic", 3);
        TopicPartition tp4 = new TopicPartition("mytopic", 4);
        TopicPartition tp5 = new TopicPartition("mytopic", 5);
        // 手动分配
        consumer.assign(Arrays.asList(tp0,tp1,tp2,tp3,tp4,tp5));
        // 实时消费标记
        boolean flag = true;
        while(flag) {
            // 获取主题消息数据
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
        // 出现异常关闭消费者对象
        consumer.close();
    }
    
}
