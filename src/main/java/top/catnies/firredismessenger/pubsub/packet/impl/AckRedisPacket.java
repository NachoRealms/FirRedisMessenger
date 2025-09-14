package top.catnies.firredismessenger.pubsub.packet.impl;

import top.catnies.firredismessenger.pubsub.packet.IRedisPacket;
import top.catnies.firredismessenger.pubsub.packet.IRedisPacketMetadata;
import top.catnies.firredismessenger.pubsub.packet.RedisPacketCoder;

public class AckRedisPacket implements IRedisPacket {
    public static final RedisPacketCoder.IRedisPacketCoder<AckRedisPacket> CODER = RedisPacketCoder.of(
            (p) -> new byte[0],
            (b) -> new AckRedisPacket()
    );

    private IRedisPacketMetadata metadata;

    @Override
    public IRedisPacketMetadata getMetadata() {
        return this.metadata;
    }

    @Override
    public void setMetadata(IRedisPacketMetadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public String getSubject() {
        return null;
    }

    @Override
    public void setSubject(String subject) {
        throw new UnsupportedOperationException("AckRedisPacket can not set subject");
    }
}
