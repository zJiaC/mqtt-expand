package com.jc.autoconfigure;

import cn.hutool.core.util.ObjectUtil;
import com.annotation.Mqtt;
import com.consumer.AbstractConsumer;
import com.jc.profile.MqttProfile;
import com.jc.profile.MqttProfile.MqttWill;
import com.jc.profile.TenantMqttProfile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import javax.annotation.PostConstruct;
import com.utils.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;

/**
 * @author zJiaC
 * Created by 2024/3/21
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnWebApplication
@EnableConfigurationProperties(TenantMqttProfile.class)
@Import(value = {MqttUtils.class})
public class MqttExpandAutoConfiguration {
  private final TenantMqttProfile profile;

  private final MqttUtils utils;

  private final ApplicationContext applicationContext;


  public MqttExpandAutoConfiguration(TenantMqttProfile profile, MqttUtils utils,
      ApplicationContext applicationContext, ApplicationContext applicationContext1) {
    this.profile = profile;
    this.utils   = utils;
    this.applicationContext = applicationContext1;
  }

  public MqttConnectOptions getMqttConnectOptions(MqttProfile mqttProfile) {
    MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
    mqttConnectOptions.setUserName(mqttProfile.getUserName());
    mqttConnectOptions.setPassword(mqttProfile.getPassword().toCharArray());
    mqttConnectOptions.setServerURIs(new String[]{mqttProfile.getUrl()});
    //设置同一时间可以发送的最大未确认消息数量
    mqttConnectOptions.setMaxInflight(mqttProfile.getMaxInflight());
    //设置超时时间
    mqttConnectOptions.setConnectionTimeout(mqttProfile.getCompletionTimeout());
    //设置自动重连
    mqttConnectOptions.setAutomaticReconnect(mqttProfile.getAutomaticReconnect());
    //cleanSession 设为 true;当客户端掉线时;服务器端会清除 客户端session;重连后 客户端会有一个新的session,cleanSession
    // 设为false，客户端掉线后 服务器端不会清除session，当重连后可以接收之前订阅主题的消息。当客户端上线后会接受到它离线的这段时间的消息
    mqttConnectOptions.setCleanSession(mqttProfile.getCleanSession());
    // 设置会话心跳时间 单位为秒   设置会话心跳时间 单位为秒 服务器会每隔1.5*20秒的时间向客户端发送心跳判断客户端是否在线，但这个方法并没有重连的机制
    mqttConnectOptions.setKeepAliveInterval(mqttProfile.getKeepAliveInterval());
    //设置遗嘱消息
    if (ObjectUtil.isNotEmpty(mqttProfile.getWill())) {
      MqttWill will = mqttProfile.getWill();
      mqttConnectOptions.setWill(will.getTopic(), will.getMessage().getBytes(), will.getQos(), will.getRetained());
    }
    return mqttConnectOptions;
  }


  @PostConstruct
  public void init() throws MqttException {
    Map<String,MqttProfile> tenantProfileMap = profile.getTenant();
    if(ObjectUtil.isEmpty(tenantProfileMap)){
      return ;
    }
    for(Entry<String,MqttProfile> entry: tenantProfileMap.entrySet()){
      String tenantCode = entry.getKey();
      MqttProfile mqttProfile = entry.getValue();

      MqttClient mqttClient = new MqttClient(mqttProfile.getUrl(), ObjectUtil.defaultIfNull(mqttProfile.getClientId(),
          UUID.randomUUID().toString()));
      List<Object> clazzList  = new ArrayList<>(applicationContext.getBeansWithAnnotation(Mqtt.class).values());
      MqttConnectOptions options    = getMqttConnectOptions(mqttProfile);
      mqttClient.connect(options);
      utils.putOptionsMap(tenantCode,options);

      if (ObjectUtil.isNotEmpty(clazzList)) {
        Map<AbstractConsumer, Mqtt> map = new HashMap<>(clazzList.size());
        for (Object abstractConsumer : clazzList) {
          Mqtt mqtt = AnnotationUtils.findAnnotation(abstractConsumer.getClass(), Mqtt.class);

          if (ObjectUtil.isEmpty(mqtt)) {
            continue;
          }
          AbstractConsumer consumer = (AbstractConsumer) abstractConsumer;
          for(String topic : mqtt.topics()){
            mqttClient.subscribe(topic, mqtt.qos(), consumer);
          }
          map.put(consumer, mqtt);
          utils.putTenantAbstractConsumerMap(tenantCode,map);
        }
      }
      Collection<MqttCallback> mqttCallbacks = applicationContext.getBeansOfType(MqttCallback.class).values();
      if (ObjectUtil.isNotEmpty(mqttCallbacks)) {
        mqttClient.setCallback(mqttCallbacks.stream().findFirst().get());
      }
      utils.putClient(tenantCode,mqttClient);
    }
  }

  @Bean
  @ConditionalOnMissingBean(MessageChannel.class)
  public MessageChannel mqttInputChannel() {
    return new DirectChannel();
  }
}
