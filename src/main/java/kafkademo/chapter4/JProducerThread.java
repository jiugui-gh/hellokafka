package kafkademo.chapter4;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

/**
 * 实现一个生产者客户端应用程序
 * @author Pinkboy
 *
 */
public class JProducerThread extends Thread{

    // 创建一个日志对象
    private final Logger LOG = LoggerFactory.getLogger(JProducerThread.class);
    // 声明醉倒线程数
    private final static int MAX_THREAD_SIZE = 6;
    // 配置Kafka连接信息
    public Properties configure() {
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9091,localhost:9092,localhost:9093");// 指定kafka集群地址
        props.put("acks", "1"); // 设置应答模式, 1表示有一个Kafka代理节点返回结果
        props.put("retries", 0); // 重试次数
        props.put("batch.size", 16384); // 批量提交大小
        props.put("linger.ms", 1); // 延时提交
        props.put("buffer.memory", 33554432); // 缓冲大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // 序列化主键
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 序列化值
        props.put("partitioner.class", "kafkademo.chapter4.JPartitioner");// 指定自定义分区类
        //props.put("partitioner.class", "kafkademo.JProducerThread");
        
        /*props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks","-1");
        props.put("retries",3);
        props.put("batch.size",323840);
        props.put("linger.ms",10);
        props.put("buffer.memory",33554432);
        props.put("max.block.ms",3000);*/

        /*     props.put("acks", "1"); // 设置应答模式, 1表示有一个Kafka代理节点返回结果
        props.put("retries", 0); // 重试次数
        props.put("batch.size", 16); // 批量提交大小
        props.put("linger.ms", 1); // 延时提交
        props.put("buffer.memory", 335); // 缓冲大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // 序列化主键
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 序列化值
        props.put("partitioner.class", "kafkademo.JProducerThread");// 指定自定义分区类
*/        
        return props;
    }
    
    public static void main(String[] args) throws IOException {
        // 创建一个固定线程数量的线程池
        ExecutorService executorService = Executors.newFixedThreadPool(MAX_THREAD_SIZE);
        // 提交任务
        executorService.submit(new JProducerThread());
        //关闭线程池
        executorService.shutdown();
        
        
    }
    
    // 实现一个单线程生产者客户端
    @Override
    public void run() {
        // TODO Auto-generated method stub
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(configure());
        // 发送10条json个数的数据
        for(int i = 0; i <  10; i++) {
            JSONObject json = new JSONObject();
            json.put("id", i);
            json.put("ip", "192.168.0."+i);
            json.put("date", new Date().toString());
            String k = "key" + i;
            // 异步发送
            producer.send(new ProducerRecord<Object, Object>("mytopic", k , json.toString()), new Callback() {

                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    // TODO Auto-generated method stub
                    if(e != null) {
                        LOG.error("Send error, msg is " + e.getMessage());
                    }else {
                        System.out.println("---------------");
                        LOG.info("The offset of the record we just sent is : " + metadata.offset());
                    }
                }
                
            });
            try {
                sleep(3000); // 间隔3秒
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                LOG.error("Interrupted thread error, msg is " + e1.getMessage());
            }
            
           
        }
        producer.close(); // 关闭生产者对象
    }
    /*@Override
    public void run() {
        // TODO Auto-generated method stub
        System.out.println("333333333333");
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(configure());
        } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        // 发送100条JSON格式的数据
        
        for (int i = 0; i < 10; i++) {
            // 封装JSON格式
            System.out.println("1111111111");
            JSONObject json = new JSONObject();
            
            json.put("id", i);
            json.put("ip", "192.168.0." + i);
            json.put("date", new Date().toString());
            String k = "key" + i;
            // 异步发送
            producer.send(new ProducerRecord<String, String>("mytopic", k, json.toJSONString()), new org.apache.kafka.clients.producer.Callback() {
                
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
            producer.send(new ProducerRecord<String, String>("ip_login_rt", k, json.toJSONString()), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        LOG.error("Send error, msg is " + e.getMessage());
                    } else {
                        LOG.info("The offset of the record we just sent is: " + metadata.offset());
                    }
                }
            });
        }
        try {
            sleep(30000);// 间隔3秒
        } catch (InterruptedException e) {
            LOG.error("Interrupted thread error, msg is " + e.getMessage());
        }

        producer.close();// 关闭生产者对象
    }*/
}

