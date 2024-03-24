package com.consumer;

import cn.hutool.json.JSONUtil;
import com.jc.entity.MqttModel;
import java.nio.charset.StandardCharsets;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractConsumer<T,R> implements IMqttMessageListener {

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  @Override
  public void messageArrived(String topic, MqttMessage message) throws Exception {
    String json          = new String(message.getPayload(), StandardCharsets.UTF_8);
    try {
      if(checkMessageKey(topic,message)){
        throw new Exception("MESSAGE_REPEAT_CONSUMPTION");
      }
      MqttModel mqttModel = JSONUtil.toBean(json, MqttModel.class);
      logger.info("线程名:{},租户编码为:{},topic主题:{},AbstractConsumer:消费者消息: {}",Thread.currentThread().getName(),mqttModel.getTenantCode(),topic, json);
      // 消费者消费消息
      R result = this.handleMessage(topic,mqttModel.getTenantCode(),(T) mqttModel.getBody());
      //保存消费成功消息
      saveLog(result,message,mqttModel);
    } catch (Throwable e){
      logger.error("AbstractConsumer:消费报错,消息为:{}, 异常为:",json, e);
      saveFailMessage(topic,message,e);
    }
  }

  /**
   * 消费方法
   * @param body 请求数据
   */
  public abstract R handleMessage(String topic,String tenantCode, T body) throws Exception;

  /**
   * 保存消费失败的消息
   *
   * @param message mq所包含的信息
   * @param e 异常
   */
  public void saveFailMessage(String topic,MqttMessage message, Throwable e){

  }

  /**
   * 判断是否重复消费
   * @return true 重复消费 false 不重复消费
   */
  public boolean checkMessageKey(String topic, MqttMessage message){
    return false;
  }

  /**
   * 保存消费成功消息
   * @param result
   * @param message
   * @param rabbitMqModel
   */
  public abstract void saveLog(R result,MqttMessage message,MqttModel rabbitMqModel);
}
