package top.catnies.firredismessenger;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Getter;
import top.catnies.firredismessenger.pubsub.RedisPubSubManager;
import top.catnies.firredismessenger.queue.RedisStreamManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisManager {
    /* 维护数据 */
    @Getter private final RedisUri connectUri;
    @Getter private final String serverId; // 服务器唯一ID
    @Getter private RedisClient redisClient; // Redis 客户端
    @Getter private StatefulRedisConnection<String, String> connection; // 普通连接
    @Getter private final Map<String, Long> serverStatus = new ConcurrentHashMap<>(); // 客户端在线心跳列表

    /* 关联对象 */
    @Getter private RedisPubSubManager pubSubManager;
    @Getter private RedisStreamManager streamManager;

    // TODO 是否要避免多个相同ID的服务器进行连接?
    public RedisManager(RedisUri connectUri, String serverId) {
        this.serverId = serverId;
        this.connectUri = connectUri;
        this.connect();
    }

    // 链接 Redis 数据库
    private void connect() {
        try {
            // 创建 Redis 链接
            redisClient = RedisClient.create(connectUri.redisUri());
            connection = redisClient.connect();
            assert connection != null;
            // 缓存 模块功能
            // PubSub 模块功能
            pubSubManager = new RedisPubSubManager(this);
            // Stream 模块功能
            streamManager = new RedisStreamManager(this);

            System.out.println("Redis connection established.");
        } catch (Exception e) {
            System.err.println("Error in Redis connection: " + e.getMessage());
        }
    }

    /**
     * 关闭数据库链接
     */
    public void shutdown() {
        if (connection != null) connection.close();
        if (redisClient != null) redisClient.shutdown();
    }

}
