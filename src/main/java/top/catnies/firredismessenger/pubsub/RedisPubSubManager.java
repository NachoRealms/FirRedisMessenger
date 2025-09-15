package top.catnies.firredismessenger.pubsub;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import top.catnies.firredismessenger.RedisManager;
import top.catnies.firredismessenger.pubsub.eventbus.RedisPubSubEventBus;
import top.catnies.firredismessenger.pubsub.packet.*;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class RedisPubSubManager {
    /* 维护数据 */
    @Getter private final StatefulRedisConnection<byte[], byte[]> connection; // 发布消息
    @Getter private final StatefulRedisPubSubConnection<byte[], byte[]> pubSubConnection; // 发布订阅连接
    @Getter private final Set<String> subscribedChannels = ConcurrentHashMap.newKeySet(); // 已订阅的频道集合

    /* 关联对象 */
    @Getter private final RedisManager redisManager;
    @Getter private final RedisPubSubCallback callbackManager;
    @Getter private final RedisPubSubEventBus messageEventBus;
    @Getter private final RedisPacketRegistry packetRegistry;

    /* 消息ID自增器 */
    private final AtomicInteger MESSAGE_ID_GENERATOR = new AtomicInteger(0);
    private int nextMessageId() {
        return MESSAGE_ID_GENERATOR.updateAndGet(prev -> prev == Integer.MAX_VALUE ? 1 : prev + 1);
    }

    // 创建一个 PubSub 管理器
    public RedisPubSubManager(RedisManager redisManager) {
        this.redisManager = redisManager;
        // 初始化链接
        this.connection = redisManager.getRedisClient().connect(ByteArrayCodec.INSTANCE);
        this.pubSubConnection = redisManager.getRedisClient().connectPubSub(ByteArrayCodec.INSTANCE);
        // 初始化对象
        this.callbackManager = new RedisPubSubCallback(this);
        this.messageEventBus = new RedisPubSubEventBus(this);
        this.packetRegistry = new RedisPacketRegistry(this);
        // 注册监听器
        pubSubConnection.addListener(new RedisPubSubAdapter<>() {
            @Override
            public void message(byte[] channel, byte[] message) {
                String channelStr = new String(channel, StandardCharsets.UTF_8);

                // 解码数据包 -> [id], [payload]
                IRedisPacket packet = decodePacket(message);

                // 如果是携带接收者的数据包
                if (packet instanceof IRedisReceiverPacket receiverPacket) {
                    boolean shouldIgnore = true;
                    String[] receivers = receiverPacket.getReceivers();
                    // 空数组代表广播, 需要接收;
                    if (receivers == null || receivers.length == 0) {
                        shouldIgnore = false;
                    }
                    // 检查自己是不是接收者;
                    else {
                        for (String receiver : receivers) {
                            if (receiver.equals(redisManager.getServerId())) {
                                shouldIgnore = false;
                                break;
                            }
                        }
                    }
                    if (shouldIgnore) return;
                }

                // 判断是否是回调数据包, 如果是则走回调;
                if (packet instanceof IRedisCallbackPacket callbackPacket) {
                    callbackManager.completeResponse(callbackPacket.getCallbackId(), packet);
                    return;
                }

                // 如果是需要回复的数据包, 则分发给 @RedisResponseHandler;
                if (packet instanceof IRedisResponsePacket originPacket) {
                    IRedisCallbackPacket callbackPacket = messageEventBus.handleResponsePacket(channelStr, packet);
                    if (callbackPacket != null) {
                        publishResponsePacket(channelStr, originPacket, callbackPacket);
                    }
                }

                // 正常处理消息, 分发给 @RedisHandler;
                messageEventBus.handlePublishPacket(channelStr, packet);
            }
        });
    }

    /**
     * 订阅消息频道
     * @param channel 目标频道
     */
    public void subscribeChannel(String channel) {
        if (subscribedChannels.add(channel)) {
            pubSubConnection.async().subscribe(channel.getBytes());
        }
    }

    /**
     * 取消订阅消息频道
     * @param channel 目标频道
     */
    public void unsubscribeChannel(String channel) {
        if (subscribedChannels.remove(channel)) {
            pubSubConnection.async().unsubscribe(channel.getBytes());
        }
    }

    /**
     * 是否已经订阅某个频道
     */
    public boolean isSubscribed(String channel) {
        return subscribedChannels.contains(channel);
    }

    /**
     * 发布一个 Redis 数据包;
     * @param channel 目标频道
     * @param packet 数据包
     * @param responseCallback 当收到回复时, 触发的回调;
     * @param timeoutMillis 多久没收到ACK/RESPONSE时触发超时回调;
     * @param timeoutCallback 当数据包超时时, 触发的回调(需要ack/response至少一个为true)
     */
    public void publishPacket(
            @NotNull String channel,
            @NotNull IRedisPacket packet,
            @Nullable Consumer<IRedisPacket> responseCallback,
            long timeoutMillis,
            @Nullable Runnable timeoutCallback
    ) {
        // 如果是需要回复的数据包, 往内部自动注入消息ID, 然后创建回调任务;
        int messageId = nextMessageId();
        if (IRedisResponsePacket.class.isAssignableFrom(packet.getClass())) {
            IRedisResponsePacket responsePacket = (IRedisResponsePacket) packet;
            responsePacket.setMessageId(messageId);

            // 注册回复回调和超时回调
            if (responseCallback != null) {
                callbackManager.register(messageId, responseCallback, timeoutMillis, timeoutCallback);
            }
        }

        // 序列化消息, 发布消息
        byte[] bytes = encodePacket(packet);
        connection.async().publish(channel.getBytes(StandardCharsets.UTF_8), bytes);
    }

    /**
     * 发布一个 RESPONSE 数据包
     * @param channel 目标频道
     * @param originPacket 原始数据包
     * @param callbackPacket 回复的数据包
     */
    public void publishResponsePacket(@NotNull String channel, @NotNull IRedisResponsePacket originPacket, @NotNull IRedisCallbackPacket callbackPacket) {
        int callbackId = originPacket.getMessageId();
        callbackPacket.setCallbackId(callbackId);
        callbackPacket.setReceivers(new String[]{ originPacket.getSender() });
        int messageId = nextMessageId();

        // 序列化消息, 发布消息
        byte[] bytes = encodePacket(callbackPacket);
        connection.async().publish(channel.getBytes(StandardCharsets.UTF_8), bytes);
    }

    /**
     * 序列化数据包, 将数据包转成 Byte[]; <br>
     * 协议格式: packetId:int + packetBodyBytes
     *
     * @param packet 需要序列化的数据包
     * @return 字节数组
     */
    private <P extends IRedisPacket> byte[] encodePacket(P packet) {
        // 寻找序列化器
        // int packetId = packet.getPacketId();
        int packetId = packetRegistry.getPacketId(packet.getClass());
        RedisPacketCodec.IRedisPacketCodec<P> codec = (RedisPacketCodec.IRedisPacketCodec<P>) packetRegistry.getPacketCodec(packetId);
        if (codec == null) {
            throw new IllegalArgumentException("数据包 " + packet.getClass().getName() + " 没有在注册表注册过喵!");
        }

        // 创建缓存区, 序列化入缓冲区;
        ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer(256);
        byte[] bytes;
        try {
            byteBuf.writeInt(packetId);
            codec.encode(packet, byteBuf);
            bytes = ByteBufUtil.getBytes(byteBuf, 0, byteBuf.readableBytes(), false);
        } finally {
            byteBuf.release();
        }

        return bytes;
    }

    /**
     * 反序列化数据包, 将Byte[]转成IRedisPacket;
     * @param message 字节数组
     * @return 数据包
     */
    @SuppressWarnings("unchecked")
    private <P extends IRedisPacket> P decodePacket(byte[] message) {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(message);
        try {
            int packetId = byteBuf.readInt();
            RedisPacketCodec.IRedisPacketCodec<P> codec =
                    (RedisPacketCodec.IRedisPacketCodec<P>) packetRegistry.getPacketCodec(packetId);
            if (codec == null) {
                throw new IllegalArgumentException("packetId=" + packetId + " 没有在注册表注册过喵!");
            }
            return codec.decode(byteBuf);
        } finally {
            byteBuf.release();
        }
    }

    /**
     * 关闭 PubSub 模块
     */
    public void shutdown() {
        if (callbackManager != null) callbackManager.shutdown();
        if (messageEventBus != null) messageEventBus.shutdown();
        if (pubSubConnection != null) pubSubConnection.close();
    }

}
