package kafkademo.chapter4;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

public class JProducer extends Thread{

    private final Logger LOG = LoggerFactory.getLogger(JProducer.class);
    
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
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 序列化值

        return props;
    }
    
    @Override
    public void run() {
        // TODO Auto-generated method stub
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(configure());
        // 发送100条json格式的数据
        for(int i = 0; i < 10; i++) {
            JSONObject json = new JSONObject();
            json.put("id", i);
            json.put("ip", "162.168.0." + i);
            json.put("date", new Date().toString());
            String key = "key" + i;
            
            producer.send(new ProducerRecord<Object, Object>("streams_wordcount_input", key, json.toString()), new Callback() {
                
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
        }
        
        producer.close();
    }
    
    public static void main(String[] args) {
        JProducer t = new JProducer();
        t.start();
    }
}
