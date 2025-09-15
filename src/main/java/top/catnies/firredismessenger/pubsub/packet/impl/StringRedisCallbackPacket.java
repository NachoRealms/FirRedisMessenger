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
                ByteBufUtils.writeString(byteBuf, packet.getSender());
                ByteBufUtils.writeStringArray(byteBuf, packet.getReceivers());
            },
            // 解码
            (byteBuf) -> {
                String subject = ByteBufUtils.readString(byteBuf);
                String payload = ByteBufUtils.readString(byteBuf);
                int callbackId = byteBuf.readInt();
                String sender = ByteBufUtils.readString(byteBuf);
                String[] receivers = ByteBufUtils.readStringArray(byteBuf);

                StringRedisCallbackPacket packet = new StringRedisCallbackPacket(subject, payload);
                packet.setCallbackId(callbackId);
                packet.setSender(sender);
                packet.setReceivers(receivers);
                return packet;
            }
    );

    private static int packetId;

    public StringRedisCallbackPacket(String subject, String payload) {
        super(subject, payload);
    }

    @Override
    public int getPacketId() {
        return packetId;
    }
}
