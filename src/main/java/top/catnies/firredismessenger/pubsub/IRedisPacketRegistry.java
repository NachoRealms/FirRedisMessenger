package top.catnies.firredismessenger.pubsub;

import top.catnies.firredismessenger.pubsub.packet.IRedisPacket;
import top.catnies.firredismessenger.pubsub.packet.RedisPacketCodec;

public interface IRedisPacketRegistry {

    /**
     * 将数据包类型和ID进行映射
     * @param clazz 数据包类
     * @param codec 数据包序列化器
     */
    <P extends IRedisPacket> void register(Class<P> clazz, RedisPacketCodec.IRedisPacketCodec<P> codec);

    /**
     * 根据ID查询数据包注册实例
     * @param typeId 数据包类型ID
     * @return 数据包注册实例
     */
    RedisPacketRegistry.RedisPacketRegistration<? extends IRedisPacket> getRegistration(int typeId);

    /**
     * 根据ID查询数据包类型
     * @param typeId 数据包类型ID
     * @return 数据包类
     */
    Class<? extends IRedisPacket> getPacketClass(int typeId);

    /**
     * 获取数据包的序列化器
     * @param typeId 数据包类型ID
     * @return 序列化器
     */
    RedisPacketCodec.IRedisPacketCodec<?> getPacketCodec(int typeId);

    /**
     * 根据数据包类查询ID
     * @param packetClass 数据包类
     * @return 数据包类型ID
     */
    int getPacketId(Class<?> packetClass);

}