package kafkademo.chapter8.file;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

public class CustomerFileStreamSourceConnector extends SourceConnector {
    // 定义主题配置变量
    public static final String TOPIC_CONFIG = "topic";
    // 定义文件配置变量
    public static final String FILE_CONFIG = "file";

    // 实例化一个配置对象
    private static final ConfigDef CONFIG_DEF = new ConfigDef().define(FILE_CONFIG, Type.STRING, Importance.HIGH, "Source filename.").define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, "The topic to publish data to");

    // 声明文件名变量
    private String filename;
    // 声明主题变量
    private String topic;

    /** 获取版本. */
    public String version() {
        return AppInfoParser.getVersion();
    }

    /** 开始初始化. */
    public void start(Map<String, String> props) {
        filename = props.get(FILE_CONFIG);
        topic = props.get(TOPIC_CONFIG);
        try {
            if (topic == null || topic.isEmpty())
                throw new ConnectException("FileStreamSourceConnector configuration must include 'topic' setting");
            if (topic.contains(","))
                throw new ConnectException("FileStreamSourceConnector should only have a single topic when used as a source.");
        } catch (ConnectException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /** 实例化输入类. */
    public Class<? extends Task> taskClass() {
        return CustomerFileStreamSourceTask.class;
    }

    /** 获取配置信息. */
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();
        if (filename != null)
            config.put(FILE_CONFIG, filename);
        config.put(TOPIC_CONFIG, topic);
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {
    }

    /** 获取配置对象. */
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}

/*// 与控制台一起工作的非常简单的连接器，此连机器支持source模式和sink模式
public class CustomerFileStreamSourceConnector extends SourceConnector {

    // 定义主题配置变量
    public static final String TOPIC_CONFIG = "topic";
    // 定义文件配置变量
    public static final String FILE_CONFIG = "file";
    
    // 实例化一个配置对象
    private static final ConfigDef CONFIG_DEF = new ConfigDef().define(FILE_CONFIG, Type.STRING , Importance.HIGH, "Source filename")
                                                                .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, "The topic to publish data to");
    
    // 声明文件名变量
    private String filename;
    // 声明主题变量
    private String topic;
    
    // 获取版本
    public String version() {
        // TODO Auto-generated method stub
        return AppInfoParser.getVersion();
    }
    
    // 开始初始化
    @Override
    public void start(Map<String, String> props) {
        // TODO Auto-generated method stub
        filename = props.get(FILE_CONFIG);
        topic = props.get(TOPIC_CONFIG);
        try {
            if(topic == null || topic.isEmpty()) 
                throw new ConnectException("FileStreamSourceConnector configuration must include 'topic' setting");
            if (topic.contains(","))
                throw new ConnectException("FileStreamSourceConnector should only have a single topic when used as a source.");
        } catch (ConnectException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
      
    }
    
    @Override
    public Class<? extends Task> taskClass() {
        // TODO Auto-generated method stub
        return CustomerFileStreamSourceTask.class;
    }
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // TODO Auto-generated method stub
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();
        if(filename != null) {
            config.put(FILE_CONFIG, filename);
        }
        config.put(TOPIC_CONFIG, topic);
        configs.add(config);
        return configs;
    }
    @Override
    public void stop() {
        // TODO Auto-generated method stub
        
    }
    @Override
    public ConfigDef config() {
        // TODO Auto-generated method stub
        return CONFIG_DEF;
    }

}
*/