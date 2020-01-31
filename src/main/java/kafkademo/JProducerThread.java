package kafkademo;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        props.put("acks", "1"); // 设置应答模式,1表示有一个Kafka代理节点返回结果
        props.put("retries", 0); // 重试次数
        props.put("batch.size", 16384); // 批量提交大小
        props.put("linger.ms", 1); // 延时提交
        props.put("buffer.memory", 33554432); // 缓冲大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // 序列化组主键
        props.put("value.seriailzer",  "org.apache.kafka.common.serialization.StringSerializer"); // 序列化值
        props.put("partitioner.class", "kafkademo.JProducerThread"); //指定自定义分区类
        
        return props;
    }
    
    public static void main(String[] args) {
        // 创建一个固定线程数量的线程池
        ExecutorService executorService = Executors.newFixedThreadPool(MAX_THREAD_SIZE);
        // 提交任务
        executorService.submit(new JProducerThread());
        //关闭线程池
        executorService.shutdown();
    }
    
    @Override
    public void run() {
        // TODO Auto-generated method stub
        super.run();
    }
}

