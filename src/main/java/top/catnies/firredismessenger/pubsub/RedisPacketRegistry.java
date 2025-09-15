package top.catnies.firredismessenger.pubsub;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.jetbrains.annotations.Nullable;
import top.catnies.firredismessenger.pubsub.packet.IRedisPacket;
import top.catnies.firredismessenger.pubsub.packet.RedisPacketCodec;
import top.catnies.firredismessenger.pubsub.packet.impl.StringRedisCallbackPacket;
import top.catnies.firredismessenger.pubsub.packet.impl.StringRedisPacket;
import top.catnies.firredismessenger.pubsub.packet.impl.StringRedisResponsePacket;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisPacketRegistry implements IRedisPacketRegistry {

    private static final String CLASS_TO_ID_KEY = "packet:classToId";
    private static final String ID_TO_CLASS_KEY = "packet:idToClass";
    private static final String ID_COUNTER_KEY  = "packet:idCounter";

    private final RedisPubSubManager manager;
    private final StatefulRedisConnection<byte[], byte[]> connection;
    private final RedisPacketRegistration<?>[] idToRegistration = new RedisPacketRegistration<?>[65535]; // 数组索引查找
    private final Map<Class<?>, Integer> classToId = new ConcurrentHashMap<>(); // 反向查找

    public RedisPacketRegistry(RedisPubSubManager manager) {
        this.manager = manager;
        this.connection = manager.getConnection();
        this.registerBasePackets();
    }

    // 注册基本数据包
    public void registerBasePackets() {
        this.register(StringRedisPacket.class, StringRedisPacket.CODEC); // 基于String的RedisPacket.
        this.register(StringRedisResponsePacket.class, StringRedisResponsePacket.CODEC); // 基于String的RedisPacket.
        this.register(StringRedisCallbackPacket.class, StringRedisCallbackPacket.CODEC); // 基于String的RedisPacket.
    }

    // 注册
    @Override
    public synchronized <P extends IRedisPacket> void register(Class<P> clazz, RedisPacketCodec.IRedisPacketCodec<P> codec) {
        RedisCommands<byte[], byte[]> cmd = connection.sync();
        String className = clazz.getName();

        // 已经注册过就直接返回
        Integer existingLocalId = classToId.get(clazz);
        if (existingLocalId != null) {
            throw new IllegalArgumentException("Packet class " + className + " is already registered");
        }

        // 先查询缓存里有没有
        byte[] classNameBytes = className.getBytes(StandardCharsets.UTF_8);
        byte[] existingIdBytes = cmd.hget(CLASS_TO_ID_KEY.getBytes(), classNameBytes);
        int typeId;

        // Redis 存在，使用已有 id;
        if (existingIdBytes != null) {
            typeId = Integer.parseInt(new String(existingIdBytes, StandardCharsets.UTF_8));
        }

        // 不存在, 就让Redis给分配一个;
        else {
            typeId = cmd.incr(ID_COUNTER_KEY.getBytes()).intValue();
            boolean success1 = cmd.hsetnx(CLASS_TO_ID_KEY.getBytes(), classNameBytes,
                    String.valueOf(typeId).getBytes(StandardCharsets.UTF_8));
            boolean success2 = cmd.hsetnx(ID_TO_CLASS_KEY.getBytes(),
                    String.valueOf(typeId).getBytes(StandardCharsets.UTF_8),
                    classNameBytes);
            // 并发被别人注册过，回退重新获取
            if (!success1 || !success2) {
                byte[] finalIdBytes = cmd.hget(CLASS_TO_ID_KEY.getBytes(), classNameBytes);
                if (finalIdBytes == null) {
                    throw new IllegalStateException("Failed to register packet class due to race condition: " + className);
                }
                typeId = Integer.parseInt(new String(finalIdBytes, StandardCharsets.UTF_8));
            }
        }

        // 本地缓存
        RedisPacketRegistration<?> reg = new RedisPacketRegistration<>(typeId, clazz, codec);
        idToRegistration[typeId] = reg;
        classToId.put(clazz, typeId);
    }

    // 查询
    @Override
    @Nullable
    public RedisPacketRegistration<? extends IRedisPacket> getRegistration(int typeId) {
        if (idToRegistration[typeId] != null) {
            return this.idToRegistration[typeId];
        }
        return null;
    }

    // 查询
    @Override
    @Nullable
    public Class<? extends IRedisPacket> getPacketClass(int typeId) {
        if (idToRegistration[typeId] != null) {
            return this.idToRegistration[typeId].clazz();
        }
        return null;
    }

    // 查询
    @Override
    @Nullable
    public RedisPacketCodec.IRedisPacketCodec<? extends IRedisPacket> getPacketCodec(int typeId) {
        if (idToRegistration[typeId] != null) {
            return this.idToRegistration[typeId].coder();
        }
        return null;
    }

    // 查询
    @Override
    public int getPacketId(Class<?> packetClass) {
        Integer packetId = classToId.get(packetClass);
        if (packetId != null) return packetId;
        throw new IllegalArgumentException("Packet class " + packetClass + " is not registered");
    }

    // 数据类
    public record RedisPacketRegistration<P extends IRedisPacket>(
            int id,
            Class<P> clazz,
            RedisPacketCodec.IRedisPacketCodec<? extends P> coder
    ) {}

}
