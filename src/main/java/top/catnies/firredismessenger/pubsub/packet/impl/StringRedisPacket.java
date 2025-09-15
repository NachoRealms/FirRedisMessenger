package top.catnies.firredismessenger.pubsub.packet.impl;

import lombok.Getter;
import lombok.Setter;
import top.catnies.firredismessenger.pubsub.packet.IRedisPayloadPacket;
import top.catnies.firredismessenger.pubsub.packet.IRedisResponsePacket;
import top.catnies.firredismessenger.pubsub.packet.RedisPacketCodec;
import top.catnies.firredismessenger.util.ByteBufUtils;

@Getter
@Setter
public class StringRedisPacket implements IRedisPayloadPacket<String>, IRedisResponsePacket {

    // 序列化器
    public static final RedisPacketCodec.IRedisPacketCodec<StringRedisPacket> CODEC = RedisPacketCodec.of(
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

                StringRedisPacket packet = new StringRedisPacket(subject, payload);
                packet.setMessageId(messageId);
                packet.setSender(sender);
                packet.setReceivers(receivers);
                return packet;
            }
    );

    private static int packetId;

    private String subject;
    private String payload;
    private int messageId;
    private String sender;
    private String[] receivers;

    public StringRedisPacket(
        String subject,
        String payload
    ) {
        this.subject = subject;
        this.payload = payload;
    }

    @Override
    public int getPacketId() {
        return packetId;
    }
}
