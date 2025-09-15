package top.catnies.firredismessenger.pubsub.packet;

public interface IRedisPayloadPacket<T> extends IRedisPacket {

    /**
     * 获取消息内容
     * @return 消息内容
     */
    T getPayload();
    void setPayload(T payload);

}
