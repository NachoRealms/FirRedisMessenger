package top.catnies.firredismessenger.pubsub.eventbus;

import org.jetbrains.annotations.NotNull;
import top.catnies.firredismessenger.pubsub.RedisPubSubManager;
import top.catnies.firredismessenger.pubsub.packet.IRedisCallbackPacket;
import top.catnies.firredismessenger.pubsub.packet.IRedisPacket;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class RedisPubSubEventBus {

    /* 维护数据 */
    // 普通消息处理器
    private final Map<HandlerKey, CopyOnWriteArrayList<HandlerWrapper<?>>> publishHandlers = new ConcurrentHashMap<>();
    // 响应消息处理器
    private final Map<HandlerKey, CopyOnWriteArrayList<ResponseHandlerWrapper<?>>> responseHandlers = new ConcurrentHashMap<>();
    // listener 注册反查表
    private final Map<RedisListener, Set<HandlerKey>> listenerMappings = new ConcurrentHashMap<>();
    // 已经注册过的消息类型
    private final Set<Class<? extends IRedisPacket>> registeredResponsePacketTypes = ConcurrentHashMap.newKeySet();
    // 接收消息处理的线程池
    private final ExecutorService dispatchExecutor = Executors.newVirtualThreadPerTaskExecutor();

    /* 关联对象 */
    private final RedisPubSubManager pubSubManager;


    public RedisPubSubEventBus(RedisPubSubManager pubSubManager) {
        this.pubSubManager = pubSubManager;
    }


    /**
     * 路由键
     */
    private static class HandlerKey {
        public final String channel;
        public final String subject;
        public final Class<? extends IRedisPacket> packetType;
        private final int hash;

        public HandlerKey(String channel, String subject, Class<? extends IRedisPacket> packetType) {
            this.channel = channel;
            this.subject = subject;
            this.packetType = packetType;
            this.hash = Objects.hash(channel, packetType, subject);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HandlerKey that = (HandlerKey) o;
            return Objects.equals(channel, that.channel)
                    && Objects.equals(subject, that.subject)
                    && Objects.equals(packetType, that.packetType);
        }

        @Override public int hashCode() {
            return hash;
        }
    }

    /**
     * 处理器包装类
     * @param key 处理的频道, 主题, 包类的集合
     * @param handler 处理器回调逻辑
     * @param priority 权重, 越大的越先处理
     */
    private record HandlerWrapper<T extends IRedisPacket>(
            HandlerKey key,
            Consumer<T> handler,
            int priority
    ) {}

    /**
     * 响应型处理器包装类
     * @param key 处理的频道, 主题, 包类的集合
     * @param handler 处理器回调逻辑
     */
    private record ResponseHandlerWrapper<T extends IRedisPacket>(
            HandlerKey key,
            BiFunction<T, IRedisCallbackPacket, IRedisCallbackPacket> handler,
            int priority
    ) {}

    /**
     * 消息入口：这里接收一个已经解码好的 Packet
     * @param channel 频道
     * @param packet 数据包
     */
    public void handlePublishPacket(@NotNull String channel, @NotNull IRedisPacket packet) {
        if (!registeredResponsePacketTypes.contains(packet.getClass())) return;
        String subject = packet.getSubject();
        HandlerKey handlerKey = new HandlerKey(channel, subject, packet.getClass());
        List<HandlerWrapper<?>> list = publishHandlers.get(handlerKey);

        if (list == null || list.isEmpty()) return;

        // 按 priority 顺序执行
        for (HandlerWrapper<?> wrapper : list) {
            dispatchExecutor.execute(() -> {
                try {
                    // 类型安全强转
                    @SuppressWarnings("unchecked")
                    Consumer<IRedisPacket> handler = (Consumer<IRedisPacket>) wrapper.handler;
                    handler.accept(packet);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    /**
     * 消息入口: 接收一个需要回复的, 并且已经解码好的Packet
     * @param channel 频道
     * @param packet 数据包
     * @return 回复数据包 （可能为 null）
     */
    public IRedisCallbackPacket handleResponsePacket(String channel, IRedisPacket packet) {
        if (!registeredResponsePacketTypes.contains(packet.getClass())) return null;
        HandlerKey key = new HandlerKey(channel, packet.getSubject(), packet.getClass());
        var list = responseHandlers.get(key);
        if (list == null || list.isEmpty()) return null;

        // 将上一个处理器处理好的回复包继续传递给下一个, 最终返回;
        IRedisCallbackPacket handledPacket = null;
        for (ResponseHandlerWrapper<?> wrapper : list) {
            @SuppressWarnings("unchecked")
            BiFunction<IRedisPacket, IRedisCallbackPacket, IRedisCallbackPacket> handler = (BiFunction<IRedisPacket, IRedisCallbackPacket, IRedisCallbackPacket>) wrapper.handler();
            handledPacket = handler.apply(packet, handledPacket);
        }
        return handledPacket;
    }

    /**
     * 注册对象中所有带 @RedisListener 的方法
     */
    public void registerListeners(RedisListener listener) {
        for (Method method : listener.getClass().getDeclaredMethods()) {
            if (method.isAnnotationPresent(RedisHandler.class)) {
                registerPublishMethod(listener, method);
            }
            if (method.isAnnotationPresent(RedisResponseHandler.class)) {
                registerResponseMethod(listener, method);
            }
        }
    }

    /**
     * 取消注册给定对象的所有 handler（包括 RedisHandler 和 RedisResponseHandler）
     */
    public void unregisterListeners(RedisListener listener) {
        Set<HandlerKey> keys = listenerMappings.remove(listener);
        if (keys == null || keys.isEmpty()) return;

        for (HandlerKey key : keys) {
            var ph = publishHandlers.get(key);
            if (ph != null) ph.removeIf(h -> listener == unwrapListener(h.handler));
            var rh = responseHandlers.get(key);
            if (rh != null) rh.removeIf(h -> listener == unwrapListener(h.handler));
        }
    }
    private Object unwrapListener(Object handler) {
        return handler;
    }

    /**
     * 注册消息处理器
     * @param channel 处理的频道
     * @param packetType 处理的包类型
     * @param subject 处理的主题
     * @param handler 处理器回调逻辑
     * @param priority 权重, 越大的越先处理
     */
    public <T extends IRedisPacket> void registerPublishHandler(
            String channel,
            Class<T> packetType,
            String subject,
            Consumer<T> handler,
            int priority
    ) {
        HandlerKey key = new HandlerKey(channel, subject, packetType);
        publishHandlers.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>())
                .add(new HandlerWrapper<>(key, handler, priority));
        publishHandlers.get(key).sort((a, b) -> Integer.compare(b.priority, a.priority));
        registeredResponsePacketTypes.add(packetType);
    }

    /**
     * 注销处理器
     * @param channel 处理的频道
     * @param packetType 处理的包类型
     * @param subject 处理的主题
     * @param handler 处理器回调逻辑
     */
    public <T extends IRedisPacket> void unregisterPublishHandler(
            String channel,
            Class<T> packetType,
            String subject,
            Consumer<T> handler
    ) {
        HandlerKey key = new HandlerKey(channel, subject, packetType);
        publishHandlers.get(key).removeIf(h -> h.handler().equals(handler));
        if (publishHandlers.get(key).isEmpty()) publishHandlers.remove(key);
        registeredResponsePacketTypes.remove(packetType);
    }

    /**
     * 注册回复处理器
     * @param channel 频道
     * @param packetType 消息类型
     * @param subject 主题
     * @param handler 处理器
     */
    public <T extends IRedisPacket> void registerResponseHandler(
            String channel,
            Class<T> packetType,
            String subject,
            BiFunction<T, IRedisCallbackPacket, IRedisCallbackPacket> handler,
            int priority
    ) {
        HandlerKey key = new HandlerKey(channel, subject, packetType);
        responseHandlers.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>())
                .add(new ResponseHandlerWrapper<>(key, handler, priority));
        responseHandlers.get(key).sort((a, b) -> Integer.compare(b.priority, a.priority));
        registeredResponsePacketTypes.add(packetType);
    }

    /**
     * 注销回复处理器
     * @param channel 频道
     * @param packetType 消息类型
     * @param subject 主题
     * @param handler 处理器
     */
    public <T extends IRedisPacket> void unregisterResponseHandler(
            String channel,
            Class<T> packetType,
            String subject,
            BiFunction<T, IRedisPacket, IRedisPacket> handler
    ) {
        HandlerKey key = new HandlerKey(channel, subject, packetType);
        responseHandlers.get(key).removeIf(h -> h.handler().equals(handler));
        if (responseHandlers.get(key).isEmpty()) responseHandlers.remove(key);
        registeredResponsePacketTypes.remove(packetType);
    }

    /**
     * 注册带有 @RedisHandler 注解的方法
     * @param listener 监听器
     * @param method 方法对象
     */
    private <T extends IRedisPacket> void registerPublishMethod(RedisListener listener, Method method) {
        // 参数只能有一个
        if (method.getParameterCount() != 1 || !IRedisPacket.class.isAssignableFrom(method.getParameterTypes()[0])) {
            throw new IllegalArgumentException("RedisHandler 方法必须有一个 IRedisPacket 参数");
        }

        // 解析注解
        @SuppressWarnings("unchecked")
        Class<T> packetType = (Class<T>) method.getParameterTypes()[0];
        RedisHandler annotation = method.getAnnotation(RedisHandler.class);
        String channel = annotation.channel();
        int priority = annotation.priority();
        boolean autoSubscribe = annotation.autoSubscribe();
        HandlerKey key = new HandlerKey(channel, annotation.subject(), packetType);
        Consumer<T> consumer = createConsumer(listener, method);

        // 创建处理器对象, 然后注册
        var wrapper = new HandlerWrapper<>(key, consumer, priority);

        // 在 handlers 主表注册, 排序保证优先级
        publishHandlers.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(wrapper);
        publishHandlers.get(key).sort((a, b) -> Integer.compare(b.priority, a.priority));

        // 保存到 registeredListeners 用于反注册
        listenerMappings.computeIfAbsent(listener, k -> ConcurrentHashMap.newKeySet()).add(key);

        // 自动订阅频道
        if (autoSubscribe && !pubSubManager.isSubscribed(channel)) {
            pubSubManager.subscribeChannel(channel);
        }
        registeredResponsePacketTypes.add(packetType);
    }

    /**
     * 注册带有 @RedisResponseHandler 注解的方法
     * @param listener 监听器
     * @param method 方法对象
     */
    private <T extends IRedisPacket> void registerResponseMethod(RedisListener listener, Method method) {
        // 参数校验
        if (method.getParameterCount() != 2
                || !IRedisPacket.class.isAssignableFrom(method.getParameterTypes()[0])
                || !IRedisCallbackPacket.class.isAssignableFrom(method.getParameterTypes()[1])) {
            throw new IllegalArgumentException("RedisResponseHandler 方法必须两个参数，第一个 IRedisPacket，第二个 IRedisCallbackPacket 喵!");
        }
        if (!IRedisPacket.class.isAssignableFrom(method.getReturnType())) {
            throw new IllegalArgumentException("RedisResponseHandler 返回类型必须是 IRedisPacket");
        }

        // 解析注解
        @SuppressWarnings("unchecked")
        Class<T> packetType = (Class<T>) method.getParameterTypes()[0];
        RedisResponseHandler annotation = method.getAnnotation(RedisResponseHandler.class);
        String subject = annotation.subject();
        String channel = annotation.channel();
        int priority = annotation.priority();
        boolean autoSubscribe = annotation.autoSubscribe();
        HandlerKey key = new HandlerKey(channel, subject, packetType);
        BiFunction<T, IRedisCallbackPacket, IRedisCallbackPacket> func = createBiFunction(listener, method);

        // 创建处理器对象, 然后注册
        var wrapper = new ResponseHandlerWrapper<>(key, func, priority);

        // 在 responseHandlers 主表注册, 排序保证优先级
        responseHandlers.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(wrapper);
        responseHandlers.get(key).sort((a, b) -> Integer.compare(b.priority, a.priority));

        // 保存到 registeredListeners 用于反注册
        listenerMappings.computeIfAbsent(listener, k -> ConcurrentHashMap.newKeySet()).add(key);

        // 自动订阅频道
        if (autoSubscribe && !pubSubManager.isSubscribed(channel)) {
            pubSubManager.subscribeChannel(channel);
        }
        registeredResponsePacketTypes.add(packetType);
    }

    /* 创建 Consumer -> 用 MethodHandle 代替反射 */
    private <T extends IRedisPacket> Consumer<T> createConsumer(RedisListener listener, Method method) {
        try {
            method.setAccessible(true);
            MethodHandle mh = MethodHandles.lookup().unreflect(method).bindTo(listener);
            return packet -> {
                try {
                    mh.invoke(packet);
                } catch (Throwable e) {
                    System.err.println("Error in Redis message handler: " + e.getMessage());
                }
            };
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /* 创建 BiFunction -> 用 MethodHandle 代替反射 */
    private <T extends IRedisPacket> BiFunction<T, IRedisCallbackPacket, IRedisCallbackPacket> createBiFunction(RedisListener listener, Method method) {
        try {
            method.setAccessible(true);
            MethodHandle mh = MethodHandles.lookup().unreflect(method).bindTo(listener);
            return (packet, resp) -> {
                try {
                    return (IRedisCallbackPacket) mh.invoke(packet, resp);
                } catch (Throwable e) {
                    System.err.println("Error in Redis message response handler: " + e.getMessage());
                    return null;
                }
            };
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭消息路由
     */
    public void shutdown() {
        dispatchExecutor.shutdown();
        publishHandlers.clear();
    }
}
