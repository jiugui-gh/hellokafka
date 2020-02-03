package kafkademo.chapter9.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// 初始化生产者实例并随机生成数据
public class RandomNetworkDataProducer implements Runnable{

    private static final long INCOMING_DATA_INTERVAL = 500;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(RandomNetworkDataProducer.class);

    @Override
    public void run() {
        // TODO Auto-generated method stub
        LOGGER.info("Initializing kafka producer...");
        
    }

    
}
