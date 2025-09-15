package top.catnies.firredismessenger.pubsub.packet;

public interface IRedisResponsePacket extends IRedisPacket, IRedisReceiverPacket {

    /**
     * 数据包消息对应的唯一ID, 每条消息只对应有一个ID
     * @return 消息ID
     */
    int getMessageId();
    void setMessageId(int messageId);

    /**
     * 数据包的发送者
     */
    String getSender();
    void setSender(String sender);

}
