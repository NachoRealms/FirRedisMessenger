package top.catnies.firredismessenger.example;

import lombok.SneakyThrows;
import top.catnies.firredismessenger.RedisManager;
import top.catnies.firredismessenger.RedisUri;
import top.catnies.firredismessenger.pubsub.RedisHandler;
import top.catnies.firredismessenger.pubsub.RedisListener;
import top.catnies.firredismessenger.pubsub.RedisResponseHandler;
import top.catnies.firredismessenger.pubsub.packet.impl.StringRedisPacket;

import java.util.Random;

import static top.catnies.firredismessenger.RedisUri.builder;

public class PubSubReceiver {

    public static RedisManager manager;

    @SneakyThrows
    public static void main(String[] args) {
        RedisUri redisUri = builder().ip("127.0.0.1").host(6379).build();
        manager = new RedisManager(redisUri, "Spawn");
        manager.getPubSubManager().getMessageEventBus().registerListeners(new MyListener());

        while (true) {
        }
    }

    static class MyListener implements RedisListener {

        @RedisHandler(channel = "qwq", subject = "aaa")
        public void onStringPacket(StringRedisPacket packet) {
            String payload = packet.getPayload();
            System.out.println(payload);
        }

        @RedisResponseHandler(channel = "qwq", subject = "aaa")
        public StringRedisPacket onResponsePacket(StringRedisPacket packet) {
            // 随机给回复一个数字
            int i = new Random().nextInt();
            return new StringRedisPacket("bbb", String.valueOf(i));
        }
    }

}
