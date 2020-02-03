package kafkademo.chapter9;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import kafkademo.chapter4.serialization.JSalarySeralizer;
import kafkademo.chapter4.serialization.JSalarySerial;
import kafkademo.chapter5.deserialize.JSalaryDeserializer;

@SuppressWarnings("rawtypes")
public class MyDeserializer implements Serde{

    @Override
    public void configure(Map configs, boolean isKey) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Serializer<JSalarySerial> serializer() {
        // TODO Auto-generated method stub
        return new JSalarySeralizer();
    }

    @Override
    public Deserializer<Object> deserializer() {
        // TODO Auto-generated method stub
        return new JSalaryDeserializer();
    }

}
