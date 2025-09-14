package top.catnies.firredismessenger.example;

import lombok.SneakyThrows;
import top.catnies.firredismessenger.RedisManager;
import top.catnies.firredismessenger.RedisUri;
import top.catnies.firredismessenger.pubsub.packet.impl.StringRedisPacket;

public class PubSubSender {

    @SneakyThrows
    public static void main(String[] args) {
        RedisUri redisUri = RedisUri.builder().ip("127.0.0.1").host(6379).build();
        RedisManager redisManager = new RedisManager(redisUri, "Lobby");

        redisManager.getPubSubManager().subscribeChannel("qwq");

        while (true) {
            Thread.sleep(1000);

            StringRedisPacket packet = new StringRedisPacket("aaa", "bbb");
            redisManager.getPubSubManager()
                .publishPacket(
                    // 目标频道
                    "qwq",
                    // 期望的接收方
                    new String[]{"Spawn"},
                    // 数据包
                    packet,
                    // ACK回调
                    () -> {
                        System.out.println("我发出去的包" + packet.getMetadata().messageId() + "收到了ACK回调哦!");
                    },
                    // 回复回调
                    responsePacket -> {
                        StringRedisPacket stringRedisPacket = (StringRedisPacket) responsePacket;
                        String payload = stringRedisPacket.getPayload();
                        System.out.println("我发出去的包" + packet.getMetadata().messageId() + "收到了回复消息哦, 回复内容是: " + payload);
                    },
                    // 超时回调
                    5000,
                    () -> {
                        System.out.println("我发出去的包" + packet.getMetadata().messageId() + "好像一直没有人ACK和Response呢!");
                    }
                );
        }
    }

}
