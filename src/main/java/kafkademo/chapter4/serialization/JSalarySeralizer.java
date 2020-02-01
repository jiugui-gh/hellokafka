package kafkademo.chapter4.serialization;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

/**
 * 自定义序列化实现
 * @author Pinkboy
 *
 */
public class JSalarySeralizer implements Serializer<JSalarySerial>{

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // TODO Auto-generated method stub
        
    }
    /**
     * 实现自定义序列化
     */
    @Override
    public byte[] serialize(String topic, JSalarySerial data) {
        // TODO Auto-generated method stub
        System.out.println("调用自定义序列化实现");
        return SerializeUtils.serialize(data);
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }
    

}
