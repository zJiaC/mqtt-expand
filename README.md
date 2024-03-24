# MQTT消费用例
1.继承AbstractConsumer抽象类并重写handleMessage(业务逻辑处理),saveFailMessage(失败消息保存)

2.加上@Mqtt注解，并填写主题以及消息质量，这样子在项目启动时侯，就会自动订阅该主题
```
@Mqtt(topics = "topic",qos = 2)
public class Test extends AbstractConsumer<String> {

  private Logger logger = LoggerFactory.getLogger(Test.class);


  @Override
  public void handleMessage(String body) throws Exception {
    logger.info("消息为:{}",body);
  }

  @Override
  public void saveFailMessage(String topic, MqttMessage message, Exception e) {

  }
}
```
