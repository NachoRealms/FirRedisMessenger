package top.catnies.firredismessenger.pubsub.packet;

public interface IRedisCallbackPacket extends IRedisPacket, IRedisReceiverPacket {

    /**
     * 回调ID, 回复的目标的消息ID;
     */
    int getCallbackId();
    void setCallbackId(int callbackId);

}
