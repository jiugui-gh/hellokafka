package kafkademo.chapter5;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * 自动获取分区的用法
 * 实现一个消费者实例代码
 * @author Pinkboy
 *
 */
public class JConsumerSubscribe extends Thread{

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
        // 创建一个消费者实例对象
        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(configure());
        // 订阅消费主题集合
        consumer.subscribe(Arrays.asList("mytopic","mytopic1","ip_login"));
        // 实时消费标记
        boolean flag = true;
        while(flag) {
            // 获取主题消息数据
            ConsumerRecords<Object, Object> records = consumer.poll(100);
            for (ConsumerRecord<Object, Object> consumerRecord : records) {
                // 循环打印消息记录
                System.out.printf("offset = %d, key = %s, value = %s%n",consumerRecord.offset(),consumerRecord.key(),consumerRecord.value());
            }
        }
        // 出现异常关闭消费者对象
        consumer.close();
    }
    public static void main(String[] args) {
        JConsumerSubscribe jconsumer = new JConsumerSubscribe();
        jconsumer.start();
    }
}
