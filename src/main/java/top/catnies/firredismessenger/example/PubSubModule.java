package top.catnies.firredismessenger.example;

import lombok.SneakyThrows;
import top.catnies.firredismessenger.RedisManager;
import top.catnies.firredismessenger.RedisUri;
import top.catnies.firredismessenger.pubsub.eventbus.RedisHandler;
import top.catnies.firredismessenger.pubsub.eventbus.RedisListener;
import top.catnies.firredismessenger.pubsub.eventbus.RedisResponseHandler;
import top.catnies.firredismessenger.pubsub.packet.IRedisCallbackPacket;
import top.catnies.firredismessenger.pubsub.packet.impl.StringRedisCallbackPacket;
import top.catnies.firredismessenger.pubsub.packet.impl.StringRedisResponsePacket;

import java.util.Random;

public class PubSubModule {

    @SneakyThrows
    public static void main(String[] args) {
        RedisUri redisUri = RedisUri.builder().ip("127.0.0.1").host(6379).build();
        RedisManager redisManager = new RedisManager(redisUri, "Lobby");

        redisManager.getPubSubManager().getMessageEventBus().registerListeners(new MyListener());

        while (true) {
            Thread.sleep(1000);

            StringRedisResponsePacket packet = new StringRedisResponsePacket("aaa", "bbb");
            redisManager.getPubSubManager()
                .publishPacket(
                    // 目标频道
                    "qwq",
                    // 数据包
                    packet,
                    // 回复回调
                    responsePacket -> {
                        StringRedisCallbackPacket stringRedisPacket = (StringRedisCallbackPacket) responsePacket;
                        String payload = stringRedisPacket.getPayload();
                        System.out.println("我发出去的包收到了回复消息哦, 回复内容是: " + payload);
                    },
                    // 超时回调
                    5000,
                    () -> {
                        System.out.println("我发出去的包好像一直没有人Response呢!");
                    }
                );
        }
    }

    static class MyListener implements RedisListener {

        @RedisHandler(channel = "qwq", subject = "aaa")
        public void onStringPacket(StringRedisResponsePacket packet) {
            String payload = packet.getPayload();
            System.out.println(payload);
        }

        @RedisResponseHandler(channel = "qwq", subject = "aaa", priority = 2)
        public IRedisCallbackPacket onResponsePacket(StringRedisResponsePacket packet, IRedisCallbackPacket responsePacket) {
            // 随机给回复一个数字
            int i = new Random().nextInt();
            return StringRedisCallbackPacket.of("bbb", String.valueOf(i));
        }

        @RedisResponseHandler(channel = "qwq", subject = "aaa", priority = 1)
        public IRedisCallbackPacket onResponsePacket2(StringRedisResponsePacket packet, IRedisCallbackPacket responsePacket) {
            // 给包里加点料
            if (responsePacket instanceof StringRedisCallbackPacket stringRedisPacket) {
                stringRedisPacket.setPayload(stringRedisPacket.getPayload() + "料料!");
                return stringRedisPacket;
            }
            return responsePacket;
        }
    }

}
