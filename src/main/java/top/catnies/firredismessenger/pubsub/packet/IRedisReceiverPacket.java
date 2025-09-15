package top.catnies.firredismessenger.pubsub.packet;


/**
 * 指定接收者的数据包;
 *
 */
public interface IRedisReceiverPacket extends IRedisPacket {

    /**
     * 数据包的接收者
     */
    String[] getReceivers();
    void setReceivers(String[] receivers);

}
