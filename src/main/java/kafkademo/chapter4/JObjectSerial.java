package kafkademo.chapter4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class JObjectSerial implements Serializable{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    // private static Logger LOG = LoggerFactory.getLogger(JObjectSerial.class);
    
    public byte id = 1; // 用户id
    public byte money = 100; // 充值金额

    // 实现序列化接口
    public static void main(String[] args) throws Exception {
        //byte [] outBuf = new byte[1024];
        //byte [] inBuf = new byte[1024];
        
       
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(new JObjectSerial());
        
        
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object obj = ois.readObject();
        
        if(obj instanceof JObjectSerial) {
            System.out.println(obj);
        }else {
            System.out.println("错误");
        }
        oos.close();
    }

    @Override
    public String toString() {
        return "JObjectSerial [id=" + id + ", money=" + money + "]";
    }
    
    
}
