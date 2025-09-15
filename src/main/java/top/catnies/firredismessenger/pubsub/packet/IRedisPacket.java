package top.catnies.firredismessenger.pubsub.packet;


public interface IRedisPacket {

    /**
     * 数据包类型的唯一ID, 每一种数据包实现只对应一个ID
     * @return 数据包ID
     */
    int getPacketId();

    /**
     * 获取消息主题
     * @return 消息主题
     */
    String getSubject();
    void setSubject(String subject);

}