package kafkademo.chapter4.serialization;

import java.io.UnsupportedEncodingException;

/**
 * 封装一个序列化的工具类
 * @author Pinkboy
 *
 */
public class SerializeUtils {

    // 实现序列化
    public static byte[] serialize(Object obj) {
        try {
            return obj.toString().getBytes();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        return null;
    }
   
    // 实现反序列化   简单版本
    public static <T> Object deserializer(byte[] bytes) {
        try {
            return new String(bytes,"UTF-8");
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        return null;
    }
}
