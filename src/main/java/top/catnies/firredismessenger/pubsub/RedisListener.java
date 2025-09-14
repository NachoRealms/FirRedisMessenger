package top.catnies.firredismessenger.pubsub;

/**
 * 没有任何方法, 只是一个标记接口, 用于标识这个类是一个 Redis 监听器.
 * 当一个类实现了这个接口, 则会被注册为一个 Redis 监听器, 并在 Redis 接收到消息时被调用.
 */
public interface RedisListener {
}
