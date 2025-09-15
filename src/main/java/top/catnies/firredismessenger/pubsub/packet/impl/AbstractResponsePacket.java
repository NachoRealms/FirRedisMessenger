package top.catnies.firredismessenger.pubsub.packet.impl;

import lombok.Getter;
import lombok.Setter;
import top.catnies.firredismessenger.pubsub.packet.IRedisPayloadPacket;
import top.catnies.firredismessenger.pubsub.packet.IRedisResponsePacket;

@Getter
@Setter
public abstract class AbstractResponsePacket<T> implements IRedisPayloadPacket<T>, IRedisResponsePacket {

    private String sender;
    private int messageId;

    private String subject;
    private T payload;
    private String[] receivers;

    public AbstractResponsePacket(String subject, T payload, String[] receivers) {
        this.subject = subject;
        this.payload = payload;
        this.receivers = receivers;
    }

}
