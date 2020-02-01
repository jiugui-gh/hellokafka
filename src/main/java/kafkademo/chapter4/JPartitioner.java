package kafkademo.chapter4;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class JPartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {
        // TODO Auto-generated method stub
        
    }

    // 实现Kafka主题分区索引算法
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // TODO Auto-generated method stub
        System.out.println("自定义分区");
        int partition = 0;
        String k = (String) key;
        partition = Math.abs(k.hashCode()) % cluster.partitionCountForTopic(topic);
        return partition;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }

    
}
