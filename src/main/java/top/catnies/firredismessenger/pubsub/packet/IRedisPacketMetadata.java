package top.catnies.firredismessenger.pubsub.packet;

public interface IRedisPacketMetadata {

    /**
     * 数据包类型的唯一ID, 每一种数据包实现只对应一个ID
     * @return 数据包ID
     */
    int packetId();

    /**
     * 数据包Class对应的ID数据, 方便接收方快速反查类
     * @return 数据包类型ID
     */
    int messageTypeId();

    /**
     * 数据包消息对应的唯一ID, 每条消息只对应有一个ID
     *
     * @return 消息ID
     */
    int messageId();

    /**
     * 数据包的发送者
     */
    String sender();

    /**
     * 数据包的接收者
     */
    String[] receivers();

    /**
     * 是否要求接收方自动发ACK包
     */
    boolean requiresAck();

    /**
     * 是否要求接收方发RESPONSE
     */
    boolean requiresResponse();


    /**
     * 回调消息ID, 当消息类型是ACK/Response时, 携带这个数据, 代表需要ACK/Response的消息ID
     */
    int callbackId();

    /**
     * 数据包发送时的时间
     * @return 时间戳
     */
    long publishTime();

    /**
     * 序列化成字节数组
     */
    byte[] toBytes();

}
