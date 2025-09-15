package top.catnies.firredismessenger.pubsub.packet.impl;

import lombok.Getter;
import lombok.Setter;
import top.catnies.firredismessenger.pubsub.packet.IRedisPayloadPacket;

@Getter
@Setter
public abstract class AbstractBroadcastPacket<T> implements IRedisPayloadPacket<T> {

    private String subject;
    private T payload;

    public AbstractBroadcastPacket(String subject, T payload) {
        this.subject = subject;
        this.payload = payload;
    }

}
