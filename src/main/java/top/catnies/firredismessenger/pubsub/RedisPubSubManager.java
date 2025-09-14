package top.catnies.firredismessenger.pubsub;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import top.catnies.firredismessenger.RedisManager;
import top.catnies.firredismessenger.pubsub.packet.*;
import top.catnies.firredismessenger.pubsub.packet.impl.AckRedisPacket;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class RedisPubSubManager {
    public static final String[] ALL_RECEIVERS = {"*"}; // 表示消息发送给所有接收方

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
        this.callbackManager = new RedisPubSubCallback();
        this.messageEventBus = new RedisPubSubEventBus(this);
        this.packetRegistry = new RedisPacketRegistry();
        // 注册监听器
        pubSubConnection.addListener(new RedisPubSubAdapter<>() {
            @Override
            public void message(byte[] channel, byte[] message) {
                String channelStr = new String(channel, StandardCharsets.UTF_8);
                IRedisPacket packet = decodePacket(message);
                // 看看这个消息是不是发给自己的;
                for (String receiver : packet.getMetadata().receivers()) {
                    if (receiver.equals("*") || receiver.equals(redisManager.getServerId())) {
                        break;
                    }
                    return;
                }
                // 如果是正常消息;
                RedisMessageType messageType = RedisMessageType.byId(packet.getMetadata().messageTypeId());
                if (messageType == RedisMessageType.PUBLISH) {
                    // 如果需要ACK则自动回复;
                    if (packet.getMetadata().requiresAck()) {
                        publishAckPacket(channelStr, packet);
                    }
                    // 如果需要Response的则交给回复处理器, 然后自动回复;
                    if (packet.getMetadata().requiresResponse()) {
                        IRedisPacket iRedisPacket = messageEventBus.handleResponsePacket(channelStr, packet);
                        if (iRedisPacket != null) {
                            publishResponsePacket(channelStr, packet, iRedisPacket);
                        }
                    }
                    // 否则就正常处理消息喵;
                    messageEventBus.handlePublishPacket(channelStr, packet);
                    return;
                }
                // 如果是ACK消息, 触发ACK回调;
                if (messageType == RedisMessageType.ACK) {
                    int callbackId = packet.getMetadata().callbackId();
                    callbackManager.completeAck(callbackId);
                    return;
                }
                // 如果是RESPONSE消息, 触发RESPONSE回调;
                if (messageType == RedisMessageType.RESPONSE) {
                    int callbackId = packet.getMetadata().callbackId();
                    callbackManager.completeResponse(callbackId, packet);
                }
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
     * @param receivers 接收者们的ID, 如果使用 * 代表全部;
     * @param packet 数据包
     * @param ackCallback 当收到ACK时, 触发的回调;
     * @param responseCallback 当收到回复时, 触发的回调;
     * @param timeoutMillis 多久没收到ACK/RESPONSE时触发超时回调;
     * @param timeoutCallback 当数据包超时时, 触发的回调(需要ack/response至少一个为true)
     */
    public void publishPacket(
            @NotNull String channel,
            @NotNull String[] receivers,
            @NotNull IRedisPacket packet,
            @Nullable Runnable ackCallback,
            @Nullable Consumer<IRedisPacket> responseCallback,
            long timeoutMillis,
            @Nullable Runnable timeoutCallback
    ) {
        // 完善数据包
        boolean requireAck = ackCallback != null;
        boolean requireResponse = responseCallback != null;
        int packetId = packetRegistry.getPacketId(packet.getClass());
        RedisPacketMetadata metadata = new RedisPacketMetadata(
                packetId,
                RedisMessageType.PUBLISH.id(),
                nextMessageId(),
                redisManager.getServerId(),
                receivers,
                requireAck,
                requireResponse,
                -1,
                System.currentTimeMillis()
        );
        // 创建回调
        RedisPacketFuture redisPacketFuture = new RedisPacketFuture();
        // 注册ACK回调任务
        if (requireAck) redisPacketFuture.onAck(ackCallback);
        // 注册回复回调任务
        if (requireResponse) redisPacketFuture.onResponse(responseCallback);
        // 注册超时回调任务
        callbackManager.register(metadata.messageId(), redisPacketFuture, timeoutMillis, timeoutCallback);
        // 序列化消息
        packet.setMetadata(metadata);
        byte[] encodedPacket = encodePacket(packet);
        // 发布消息
        connection.async().publish(channel.getBytes(StandardCharsets.UTF_8), encodedPacket);
    }

    /**
     * 发布一个 ACK 数据包
     * @param channel 目标频道
     * @param originPacket 需要回复的原始数据包
     */
    public void publishAckPacket(@NotNull String channel, @NotNull IRedisPacket originPacket) {
        // 完善ACK数据包
        AckRedisPacket packet = new AckRedisPacket();
        int packetId = packetRegistry.getPacketId(packet.getClass());
        RedisPacketMetadata metadata = new RedisPacketMetadata(
                packetId,
                RedisMessageType.ACK.id(),
                nextMessageId(),
                redisManager.getServerId(),
                new String[]{originPacket.getMetadata().sender()},
                false,
                false,
                originPacket.getMetadata().messageId(),
                System.currentTimeMillis()
        );
        // 序列化消息
        packet.setMetadata(metadata);
        byte[] encodedPacket = encodePacket(packet);
        // 发布消息
        connection.async().publish(channel.getBytes(StandardCharsets.UTF_8), encodedPacket);
    }

    /**
     * 发布一个 RESPONSE 数据包
     * @param channel 目标频道
     * @param originPacket 原始数据包
     * @param responsePacket 回复的数据包
     */
    public void publishResponsePacket(@NotNull String channel, @NotNull IRedisPacket originPacket, @NotNull IRedisPacket responsePacket) {
        // 完善数据包
        int packetId = packetRegistry.getPacketId(responsePacket.getClass());
        RedisPacketMetadata metadata = new RedisPacketMetadata(
                packetId,
                RedisMessageType.RESPONSE.id(),
                nextMessageId(),
                redisManager.getServerId(),
                new String[]{originPacket.getMetadata().sender()},
                false,
                false,
                originPacket.getMetadata().messageId(),
                System.currentTimeMillis()
        );
        // 序列化消息
        responsePacket.setMetadata(metadata);
        byte[] encodedPacket = encodePacket(responsePacket);
        // 发布消息
        connection.async().publish(channel.getBytes(StandardCharsets.UTF_8), encodedPacket);
    }


    // 协议格式: <metadataBytesLength:int>  <metadataBytes>  <packetBodyBytes>
    // 统一的序列化
    private byte[] encodePacket(IRedisPacket packet) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(bos)) {
            // 先将meta进行序列化
            IRedisPacketMetadata metadata = packet.getMetadata();
            byte[] metadataBytes = ((RedisPacketMetadata) metadata).toBytes();
            // 从packetId可以找到coder, 再将消息的body进行序列化
            // noinspection unchecked
            RedisPacketCoder.IRedisPacketCoder<IRedisPacket> coder = (RedisPacketCoder.IRedisPacketCoder<IRedisPacket>) packetRegistry.getPacketCoder(metadata.packetId());
            if (coder == null) {
                throw new IllegalArgumentException("Packet " + packet.getClass().getName() + " is not registered coder");
            }
            byte[] bodyBytes = coder.encode(packet);
            // 写入 metadata length, metadata, body
            dos.writeInt(metadataBytes.length);
            dos.write(metadataBytes);
            dos.write(bodyBytes);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Encoding packet failed", e);
        }
    }

    // 统一的反序列化
    private IRedisPacket decodePacket(byte[] data) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             DataInputStream dis = new DataInputStream(bis)) {
            // 先读取 metadata length, 然后先反序列化metadata
            int metadataLen = dis.readInt();
            byte[] metadataBytes = new byte[metadataLen];
            dis.readFully(metadataBytes);
            RedisPacketMetadata metadata = RedisPacketMetadata.fromBytes(metadataBytes);
            // 有了 metadata 就可以找 coder, 再反序列化body部分
            RedisPacketCoder.IRedisPacketCoder<?> coder = packetRegistry.getPacketCoder(metadata.packetId());
            if (coder == null) {
                throw new IllegalArgumentException("No coder found for typeId " + metadata.messageTypeId());
            }
            byte[] bodyBytes = dis.readAllBytes();
            IRedisPacket packet = coder.decode(bodyBytes);
            // 最后组合起来就是完整的packet了
            packet.setMetadata(metadata);
            return packet;
        } catch (IOException e) {
            throw new RuntimeException("Decoding packet failed", e);
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
