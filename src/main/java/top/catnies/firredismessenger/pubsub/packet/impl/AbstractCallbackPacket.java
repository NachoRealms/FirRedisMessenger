package top.catnies.firredismessenger.pubsub.packet.impl;

import lombok.Getter;
import lombok.Setter;
import top.catnies.firredismessenger.pubsub.packet.IRedisCallbackPacket;
import top.catnies.firredismessenger.pubsub.packet.IRedisPayloadPacket;

@Getter
@Setter
public abstract class AbstractCallbackPacket<T> implements IRedisCallbackPacket, IRedisPayloadPacket<T> {

    private String sender;
    private String[] receivers;

    private String subject;
    private T payload;
    private int callbackId;

    public AbstractCallbackPacket(String subject, T payload) {
        this.subject = subject;
        this.payload = payload;
    }

}
