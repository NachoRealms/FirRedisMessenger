package top.catnies.firredismessenger.pubsub.packet;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class RedisPacketCodec {
    public static <P extends IRedisPacket> IRedisPacketCodec<P> of(
            BiConsumer<P, ByteBuf> writer,
            Function<ByteBuf, P> reader
    ) {
        return new IRedisPacketCodec<>() {
            @Override
            public void encode(P packet, ByteBuf output) throws EncoderException {
                writer.accept(packet, output);
            }

            @Override
            public P decode(ByteBuf data) throws DecoderException {
                return reader.apply(data);
            }
        };
    }

    public interface IRedisPacketCodec<P extends IRedisPacket> {
        /**
         * 序列化, 将数据包写入 output 内;
         * @param packet 数据包
         * @param output 需要写入的 ByteBuf
         * @throws EncoderException 错误
         */
        void encode(P packet, ByteBuf output) throws EncoderException;

        /**
         * 反序列化
         * @param data 输入数据
         * @return 反序列化完成的对象
         * @throws DecoderException 错误
         */
        P decode(ByteBuf data) throws DecoderException;
    }
}
