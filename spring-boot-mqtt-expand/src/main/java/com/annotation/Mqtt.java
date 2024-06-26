package com.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * mqtt注解
 */
@Target(value = { ElementType.FIELD, ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Component
public @interface Mqtt {

  /**
   * 主题
   */
  String[] topics();

  /**
   * 消息质量
   */
  int qos() default 0;
}
