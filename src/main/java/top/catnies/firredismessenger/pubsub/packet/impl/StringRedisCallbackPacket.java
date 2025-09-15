package top.catnies.firredismessenger.pubsub.packet.impl;

import lombok.Getter;
import lombok.Setter;
import top.catnies.firredismessenger.pubsub.packet.RedisPacketCodec;
import top.catnies.firredismessenger.util.ByteBufUtils;

@Getter
@Setter
public class StringRedisCallbackPacket extends AbstractCallbackPacket<String> {

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
                StringRedisCallbackPacket packet = new StringRedisCallbackPacket(subject, payload);
                packet.setCallbackId(byteBuf.readInt());
                return packet;
            }
    );

    private static int packetId;

    private String subject;
    private String payload;
    private int callbackId;

    public StringRedisCallbackPacket(String subject, String payload) {
        super(subject, payload);
    }

    @Override
    public int getPacketId() {
        return packetId;
    }
}
