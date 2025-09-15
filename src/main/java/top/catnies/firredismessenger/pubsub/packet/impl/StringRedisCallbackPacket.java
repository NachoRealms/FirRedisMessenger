package top.catnies.firredismessenger.pubsub.packet.impl;

import lombok.Getter;
import lombok.Setter;
import top.catnies.firredismessenger.pubsub.packet.IRedisCallbackPacket;
import top.catnies.firredismessenger.pubsub.packet.IRedisPayloadPacket;
import top.catnies.firredismessenger.pubsub.packet.RedisPacketCodec;
import top.catnies.firredismessenger.util.ByteBufUtils;

@Getter
@Setter
public class StringRedisCallbackPacket implements IRedisCallbackPacket, IRedisPayloadPacket<String> {

    // 序列化器
    public static final RedisPacketCodec.IRedisPacketCodec<StringRedisCallbackPacket> CODEC = RedisPacketCodec.of(
            // 编码
            (packet, byteBuf) -> {
                ByteBufUtils.writeString(byteBuf, packet.getSubject());
                ByteBufUtils.writeString(byteBuf, packet.getPayload());
                byteBuf.writeInt(packet.getCallbackId());
            },
            // 解码
            (byteBuf) -> {
                String subject = ByteBufUtils.readString(byteBuf);
                String payload = ByteBufUtils.readString(byteBuf);
                StringRedisCallbackPacket packet = StringRedisCallbackPacket.of(subject, payload);
                packet.setCallbackId(byteBuf.readInt());
                return packet;
            }
    );

    private static int packetId;
    private String subject;
    private String payload;
    private int callbackId;

    private StringRedisCallbackPacket() {}

    public static StringRedisCallbackPacket of(String subject, String payload) {
        StringRedisCallbackPacket packet = new StringRedisCallbackPacket();
        packet.setSubject(subject);
        packet.setPayload(payload);
        return packet;
    }

    @Override
    public int getPacketId() {
        return packetId;
    }
}
