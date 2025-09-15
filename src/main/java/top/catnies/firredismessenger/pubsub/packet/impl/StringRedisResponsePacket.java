package top.catnies.firredismessenger.pubsub.packet.impl;

import lombok.Getter;
import lombok.Setter;
import top.catnies.firredismessenger.pubsub.packet.RedisPacketCodec;
import top.catnies.firredismessenger.util.ByteBufUtils;

@Getter
@Setter
public class StringRedisResponsePacket extends AbstractResponsePacket<String> {

    // 序列化器
    public static final RedisPacketCodec.IRedisPacketCodec<StringRedisResponsePacket> CODEC = RedisPacketCodec.of(
            // 编码
            (packet, byteBuf) -> {
                ByteBufUtils.writeString(byteBuf, packet.getSubject());
                ByteBufUtils.writeString(byteBuf, packet.getPayload());
                byteBuf.writeInt(packet.getMessageId());
                ByteBufUtils.writeString(byteBuf, packet.getSender());
                ByteBufUtils.writeStringArray(byteBuf, packet.getReceivers());
            },
            // 解码
            (byteBuf) -> {
                String subject = ByteBufUtils.readString(byteBuf);
                String payload = ByteBufUtils.readString(byteBuf);
                int messageId = byteBuf.readInt();
                String sender = ByteBufUtils.readString(byteBuf);
                String[] receivers = ByteBufUtils.readStringArray(byteBuf);

                StringRedisResponsePacket packet = new StringRedisResponsePacket(subject, payload, receivers);
                packet.setMessageId(messageId);
                packet.setSender(sender);
                packet.setReceivers(receivers);
                return packet;
            }
    );

    private static int packetId;

    public StringRedisResponsePacket(String subject, String payload, String[] receivers) {
        super(subject, payload, receivers);
    }

    @Override
    public int getPacketId() {
        return packetId;
    }
}
