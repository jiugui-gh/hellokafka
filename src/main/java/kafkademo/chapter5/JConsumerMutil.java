package kafkademo.chapter5;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * 多线程消费者实例
 * @author Pinkboy
 *
 */
public class JConsumerMutil {

 // 创建一个日志对象
    private final static Logger LOG = LoggerFactory.getLogger(JConsumerMutil.class);
    private final KafkaConsumer<String, String> consumer; // 声明一个消费者实例
    private ExecutorService executorService; // 声明一个线程池接口
    
    public JConsumerMutil() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9091,localhost:9092,localhost:9093");// 指定Kafka集群地址
        props.put("group.id", "myfirstgroup");// 指定消费者组
        props.put("enable.auto.commit", "true");// 开启自动提交
        props.put("auto.commit.interval.ms", "1000");// 自动提交的时间间隔
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// 反序列化消息主键
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// 反序列化消费记录
        consumer = new KafkaConsumer<String, String>(props);// 实例化消费者对象
        consumer.subscribe(Arrays.asList("mytopic"));// 订阅消费者主题
    }
    
    public static void main(String[] args) {
        JConsumerMutil consumer = new JConsumerMutil();
        try {
            consumer.execute();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            LOG.error("Mutil consumer from kafka has error,msg is " + e.getMessage());
            consumer.shutdown();
        }
    }

    private void shutdown() {
        // TODO Auto-generated method stub
        try {
            if(consumer != null) {
                consumer.close();
            }
            if(executorService != null) {
                executorService.shutdown();
            }
            if(!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                LOG.error("Shutdown kafka consumer thread timeout.");
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            Thread.currentThread().interrupt();
        }
    }

    private void execute() {
        // TODO Auto-generated method stub
        // 初始化线程池
        executorService = Executors.newFixedThreadPool(6);
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(3000);
            if(0 != records.count()) {
                executorService.submit(new KafkaConsumerThread(records));
            }
        }
    }
    class KafkaConsumerThread implements Runnable{
        
        private ConsumerRecords<String, String> records;
        
        public KafkaConsumerThread(ConsumerRecords<String, String> records) {
            super();
            this.records = records;
        }

        @Override
        public void run() {
            // TODO Auto-generated method stub
            for (TopicPartition partition : records.partitions()) {
                // 获取消费记录数据集
                synchronized(JConsumerMutil.class) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    System.out.println("------------------------------");
                    System.out.println("Thread id : "+Thread.currentThread().getId());
                    System.out.println(partition.toString());
                   // LOG.info("Thread id : "+Thread.currentThread().getId());
                    for (ConsumerRecord<String, String> consumerRecord : partitionRecords) {
                        System.out.printf("offset = %d, key = %s, value = %s%n", consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
                    }
                    System.out.println("------------------------------");
                }
                
            }
        }
        
    }
}
