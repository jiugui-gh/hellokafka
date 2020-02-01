package kafkademo.chapter5.deserialize;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import kafkademo.chapter4.serialization.SerializeUtils;
/**
 * 自定义反序列化
 * @author Pinkboy
 *
 */
public class JSalaryDeserializer implements Deserializer<Object>{

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        // TODO Auto-generated method stub
        return SerializeUtils.deserializer(data);
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }

}
