package kafkademo.chapter4.serialization;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JProducerSerial extends Thread{

    private static Logger LOG = LoggerFactory.getLogger(JProducerSerial.class);
    
    private static final int MAX_THREAD_NUM = 6;
    
    /** 配置Kafka连接信息. */
    public Properties configure() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9091,localhost:9092,localhost:9093");// 指定Kafka集群地址
        props.put("acks", "1"); // 设置应答模式, 1表示有一个Kafka代理节点返回结果
        props.put("retries", 0); // 重试次数
        props.put("batch.size", 16384); // 批量提交大小
        props.put("linger.ms", 1); // 延时提交
        props.put("buffer.memory", 33554432); // 缓冲大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // 序列化主键
        props.put("value.serializer", "kafkademo.chapter4.serialization.JSalarySeralizer");// 自定义序列化值

        return props;
    }
    public static void main(String[] args) {
        ExecutorService executor = Executors.newScheduledThreadPool(MAX_THREAD_NUM);
        executor.submit(new JProducerSerial());
        
        executor.shutdown();
    }
    
    @Override
    public void run() {
        // TODO Auto-generated method stub
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(configure());
        JSalarySerial jss = new JSalarySerial();
        jss.setId("2018");
        jss.setSalary("100");
        
        producer.send(new ProducerRecord<Object, Object>("mytopic", "key" ,jss), new Callback() {
            
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                // TODO Auto-generated method stub
                if (e != null) {
                    LOG.error("Send error, msg is " + e.getMessage());
                } else {
                    LOG.info("The offset of the record we just sent is: " + metadata.offset());
                }
            }
        });
        
        try {
            sleep(3000);
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            LOG.error("Interrupted thread error, msg is " + e1.getMessage());
        }
        
        producer.close();
    }
}
