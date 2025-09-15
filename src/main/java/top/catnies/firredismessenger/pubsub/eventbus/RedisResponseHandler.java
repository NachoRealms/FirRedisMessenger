package top.catnies.firredismessenger.pubsub.eventbus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RedisResponseHandler {

    // 监听的频道列表（必须填写）
    String channel();

    // 监听的消息主题（必须填写）
    String subject();

    // 权重
    int priority() default 10;

    // 是否自动订阅相关频道
    boolean autoSubscribe() default true;

}