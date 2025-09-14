package top.catnies.firredismessenger.queue;

import com.google.common.eventbus.EventBus;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import top.catnies.firredismessenger.RedisManager;
import top.catnies.firredismessenger.queue.decoder.RedisMessageDecoder;
import top.catnies.firredismessenger.queue.decoder.RedisMessageDecoderRegistry;
import top.catnies.firredismessenger.queue.message.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisStreamManager {

    @Getter private static RedisStreamManager instance;

    private final RedisManager redisManager;

    @Getter private final String serverId;
    @Getter private final StatefulRedisConnection<String, String> connection;
    @Getter private final RedisAsyncCommands<String, String> async;
    @Getter private final RedisCommands<String, String> sync;

    // 任务处理器
    private final ExecutorService consumerPool = Executors.newVirtualThreadPerTaskExecutor();
    // 存储所有等待回调的请求, key -> messageId
    private final ConcurrentMap<String, RedisMessageFuture> pendingRequests = new ConcurrentHashMap<>();
    // 事件管理器
    @Getter private final EventBus eventBus = new EventBus("RedisStreamManager");


    // 创建一个 RedisStreamManager
    public RedisStreamManager(RedisManager redisManager) {
        instance = this;
        this.redisManager = redisManager;
        this.serverId = redisManager.getServerId();
        this.connection = redisManager.getConnection();
        this.sync = connection.sync();
        this.async = connection.async();
        initRedisMessageDecoder();

        this.registerXGroupConsumer("_response-" + serverId, serverId); // 监听回复通道
        this.registerXGroupConsumer("_message-" + serverId, serverId); // 客户端独立接收通道
    }

    // 初始化消息序列化器
    private void initRedisMessageDecoder() {
        RedisMessageDecoderRegistry.register(RedisStringMessage.class.getName(), new RedisStringMessage.RedisStringMessageDecoder());
    }

    /**
     * 发布消息
     * @param topic StreamID
     * @param message 消息对象
     * @return Future 回调对象
     */
    @NotNull
    public RedisMessageFuture xaddMessage(@NotNull String topic, @NotNull RedisMessage message) {
        String callbackId = UUID.randomUUID().toString();
        // 序列化消息
        RedisMessageDecoder<RedisMessage> messageDecoder = RedisMessageDecoderRegistry.getDecoder(message.getClass().getName());
        if (messageDecoder != null) {
            // 构建消息
            Map<String, String> payload = messageDecoder.serialize(message);
            HashMap<String, String> data = new HashMap<>();
            data.put("_className", message.getClass().getName());
            data.put("_callbackId", callbackId);
            data.put("_origin", serverId);
            data.put("_timestamp", String.valueOf(System.currentTimeMillis()));
            data.putAll(payload);

            // 消息回调
            RedisMessageFuture messageFuture = new RedisMessageFuture();
            pendingRequests.put(callbackId, messageFuture);

            // 发送消息
            RedisFuture<String> xadd = async.xadd(topic, data);
            xadd.thenAccept(id -> System.out.println("发出了消息id为: " + id + " !"));
            return messageFuture;
        }
        throw new IllegalArgumentException("No decoder found for message class: " + message.getClass().getName());
    }

    /**
     * 回复一个消息
     * @param originMessage 接收到的原始消息
     * @param responsePayload 需要回复的内容
     */
    public void xaddResponseMessage(@NotNull RedisEnvelope<?> originMessage, @NotNull Map<String, String> responsePayload) {
        RedisMessageMetadata metadata = originMessage.metadata();
        HashMap<String, String> data = new HashMap<>();
        data.put("_callbackId", metadata.callbackId());
        data.put("_origin", serverId);
        data.put("_timestamp", String.valueOf(System.currentTimeMillis()));
        data.putAll(responsePayload);
        RedisFuture<String> xadd = async.xadd("_response-" + metadata.origin(), data);
        xadd.thenAccept(id -> System.out.println("发出了回复消息id为: " + id + " !"));
    }

    /**
     * 注册消费者组并启动监听
     * @param topic StreamID
     * @param groupName GroupID
     */
    @SuppressWarnings("unchecked")
    public void registerXGroupConsumer(String topic, String groupName) {
        // 如果 Stream 不存在，创建 Stream
        try {
            sync.xgroupCreate(
                    XReadArgs.StreamOffset.from(topic, "0"),
                    groupName,
                    XGroupCreateArgs.Builder.mkstream()
            );
        } catch (RedisBusyException ignore /* 消费组已经存在 */) {
        } catch (Exception e) {
            System.err.println("Error in registering XGroup consumer: " + e.getMessage());
        }
        // 提交监听任务
        consumerPool.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                List<StreamMessage<String, String>> messages = sync.xreadgroup(
                        Consumer.from(groupName, serverId),
                        XReadArgs.Builder.block(50), // TODO 这个值怎么修改? 现在是轮询吗?
                        XReadArgs.StreamOffset.lastConsumed(topic)
                );
                if (messages == null || messages.isEmpty()) {
                    continue;
                }
                for (StreamMessage<String, String> msg : messages) {
                    handleIncomingMessage(topic, groupName, msg);
                }
            }
        });
    }

    /**
     * 当客户端接收到进来的消息时
     * @param topic StreamID
     * @param group GroupID
     * @param msg 消息
     */
    @SuppressWarnings("unchecked")
    private void handleIncomingMessage(String topic, String group, StreamMessage<String, String> msg) {
        Map<String, String> body = msg.getBody();
        // 如果是Response消息
        if (topic.equals("_response-" + serverId)) {
            RedisMessageFuture future = pendingRequests.remove(body.get("_callbackId"));
            if (future != null) future.triggerResponse(body);
            async.xack(topic, group, msg.getId());
            return;
        }
        // 反序列化消息
        RedisMessageMetadata metadata = new RedisMessageMetadata(
            body.get("_className"),
            body.get("_callbackId"),
            body.get("_origin"),
            Long.parseLong(body.get("_timestamp"))
        );
        RedisMessageDecoder<RedisMessage> decoder = RedisMessageDecoderRegistry.getDecoder(metadata.className());
        RedisMessage payload = decoder.deserialize(body);

        // 广播消息事件
        eventBus.post(new RedisEnvelope<>(metadata, payload));
    }
}
