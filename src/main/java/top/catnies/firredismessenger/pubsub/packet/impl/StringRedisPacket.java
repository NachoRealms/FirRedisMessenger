package top.catnies.firredismessenger.pubsub.packet.impl;

import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import lombok.Getter;
import lombok.Setter;
import top.catnies.firredismessenger.pubsub.packet.IRedisPacketMetadata;
import top.catnies.firredismessenger.pubsub.packet.IRedisPayloadPacket;
import top.catnies.firredismessenger.pubsub.packet.RedisPacketCoder;
import top.catnies.firredismessenger.util.ByteUtils;

import java.io.*;

@Getter
@Setter
public class StringRedisPacket implements IRedisPayloadPacket<String> {
    public static final RedisPacketCoder.IRedisPacketCoder<StringRedisPacket> CODER = RedisPacketCoder.of(
            (p) -> {
                try (
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        DataOutputStream dos = new DataOutputStream(bos)
                ) {
                    ByteUtils.writeString(dos, p.getSubject());
                    ByteUtils.writeString(dos, (String) p.getPayload());
                    return bos.toByteArray();
                } catch (IOException e) {
                    throw new EncoderException("Failed to encode StringRedisPacket", e);
                }
            },
            (b) -> {
                try (
                        ByteArrayInputStream bis = new ByteArrayInputStream(b);
                        DataInputStream dis = new DataInputStream(bis)
                ) {
                    String subject = ByteUtils.readString(dis);
                    String payload = ByteUtils.readString(dis);
                    return new StringRedisPacket(subject, payload);
                } catch (IOException e) {
                    throw new DecoderException("Failed to decode StringRedisPacket", e);
                }
            }
    );

    private IRedisPacketMetadata metadata;
    private String subject;
    private String payload;

    public StringRedisPacket(
        String subject,
        String payload
    ) {
        this.metadata = null;
        this.subject = subject;
        this.payload = payload;
    }
}
