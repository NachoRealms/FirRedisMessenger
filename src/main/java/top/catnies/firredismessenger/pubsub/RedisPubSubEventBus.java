package top.catnies.firredismessenger.pubsub;

import org.jetbrains.annotations.NotNull;
import top.catnies.firredismessenger.pubsub.packet.IRedisPacket;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class RedisPubSubEventBus {

    // [频道: [消息Class -> [主题: [处理器(自带权重)] ]]
    private final Map<String, Map<Class<? extends IRedisPacket>, Map<String, CopyOnWriteArrayList<HandlerWrapper<?>>>>> handlers = new ConcurrentHashMap<>();
    // [频道: [消息Class -> [主题: [处理器(自带权重)] ]]
    private final Map<String, Map<Class<? extends IRedisPacket>, Map<String, CopyOnWriteArrayList<ResponseHandlerWrapper<?>>>>> responseHandlers = new ConcurrentHashMap<>();
    // 通过 Listener 实现类继承的所有监听器
    private final Map<RedisListener, Set<Object>> registeredListeners = new ConcurrentHashMap<>();
    // 接收消息处理的线程池
    private final ExecutorService dispatchExecutor = Executors.newVirtualThreadPerTaskExecutor();

    /* 关联对象 */
    private final RedisPubSubManager pubSubManager;

    public RedisPubSubEventBus(RedisPubSubManager pubSubManager) {
        this.pubSubManager = pubSubManager;
    }

    /**
     * 处理器包装类
     * @param channel 处理的频道
     * @param packetType 处理的包类型
     * @param subject 处理的主题
     * @param handler 处理器回调逻辑
     * @param priority 权重, 越大的越先处理
     */
    private record HandlerWrapper<T extends IRedisPacket>(
            String channel,
            Class<T> packetType,
            String subject,
            Consumer<T> handler,
            int priority
    ) {}

    /**
     * 响应型处理器包装类
     * @param channel 处理的频道
     * @param packetType 处理的包类型
     * @param subject 处理的主题
     * @param handler 处理器回调逻辑
     */
    private record ResponseHandlerWrapper<T extends IRedisPacket>(
            String channel,
            Class<T> packetType,
            String subject,
            BiFunction<T, IRedisPacket, IRedisPacket> handler,
            int priority
    ) {}

    /**
     * 消息入口：这里接收一个已经解码好的 Packet
     * @param channel 频道
     * @param packet 数据包
     */
    public void handlePublishPacket(@NotNull String channel, @NotNull IRedisPacket packet) {
        Map<Class<? extends IRedisPacket>, Map<String, CopyOnWriteArrayList<HandlerWrapper<?>>>> typeMap = handlers.get(channel);
        if (typeMap == null) return;
        Map<String, CopyOnWriteArrayList<HandlerWrapper<?>>> subjectMap = typeMap.get(packet.getClass());
        if (subjectMap == null) return;
        String subject = packet.getSubject();
        CopyOnWriteArrayList<HandlerWrapper<?>> list = subjectMap.get(subject);
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
    public IRedisPacket handleResponsePacket(String channel, IRedisPacket packet) {
        Map<Class<? extends IRedisPacket>, Map<String, CopyOnWriteArrayList<ResponseHandlerWrapper<?>>>> typeMap = responseHandlers.get(channel);
        if (typeMap == null) return null;
        Map<String, CopyOnWriteArrayList<ResponseHandlerWrapper<?>>> subjectMap = typeMap.get(packet.getClass());
        if (subjectMap == null) return null;
        String subject = packet.getSubject();
        CopyOnWriteArrayList<ResponseHandlerWrapper<?>> list = subjectMap.get(subject);
        if (list == null) return null;

        // 将上一个处理器处理好的回复包继续传递给下一个, 最终返回;
        IRedisPacket handledPacket = null;
        for (ResponseHandlerWrapper<?> wrapper : list) {
            @SuppressWarnings("unchecked")
            BiFunction<IRedisPacket, IRedisPacket, IRedisPacket> handler = (BiFunction<IRedisPacket, IRedisPacket, IRedisPacket>) wrapper.handler();
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
        Set<Object> wrappers = registeredListeners.remove(listener);
        if (wrappers == null || wrappers.isEmpty()) {
            return;
        }
        for (Object wrapperObj : wrappers) {
            // 普通 publish handler 注销
            if (wrapperObj instanceof HandlerWrapper<?> wrapper) {
                Map<Class<? extends IRedisPacket>, Map<String, CopyOnWriteArrayList<HandlerWrapper<?>>>> typeMap =
                        handlers.get(wrapper.channel());
                if (typeMap == null) continue;
                Map<String, CopyOnWriteArrayList<HandlerWrapper<?>>> subjectMap = typeMap.get(wrapper.packetType());
                if (subjectMap == null) continue;
                CopyOnWriteArrayList<HandlerWrapper<?>> list = subjectMap.get(wrapper.subject());
                if (list == null) continue;
                list.removeIf(h -> h.handler().equals(wrapper.handler()));
            }
            // Response handler 注销
            else if (wrapperObj instanceof ResponseHandlerWrapper<?> wrapper) {
                Map<Class<? extends IRedisPacket>, Map<String, CopyOnWriteArrayList<ResponseHandlerWrapper<?>>>> typeMap =
                        responseHandlers.get(wrapper.channel());
                if (typeMap == null) continue;
                Map<String, CopyOnWriteArrayList<ResponseHandlerWrapper<?>>> subjectMap = typeMap.get(wrapper.packetType());
                if (subjectMap == null) continue;
                CopyOnWriteArrayList<ResponseHandlerWrapper<?>> list = subjectMap.get(wrapper.subject());
                if (list == null) continue;
                list.removeIf(h -> h.handler().equals(wrapper.handler()));
            }
        }
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
        handlers.computeIfAbsent(channel, c -> new ConcurrentHashMap<>())
                .computeIfAbsent(packetType, t -> new ConcurrentHashMap<>())
                .computeIfAbsent(subject, s -> new CopyOnWriteArrayList<>())
                .add(new HandlerWrapper<>(channel, packetType, subject, handler, priority));
        // 排序: 优先级高的在前
        handlers.get(channel).get(packetType).get(subject)
                .sort((a, b) -> Integer.compare(b.priority, a.priority));
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
        Map<Class<? extends IRedisPacket>, Map<String, CopyOnWriteArrayList<HandlerWrapper<?>>>> typeMap = handlers.get(channel);
        if (typeMap == null) return;
        Map<String, CopyOnWriteArrayList<HandlerWrapper<?>>> subjectMap = typeMap.get(packetType);
        if (subjectMap == null) return;
        CopyOnWriteArrayList<HandlerWrapper<?>> list = subjectMap.get(subject);
        if (list == null) return;
        list.removeIf(h -> h.handler.equals(handler));
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
            BiFunction<T, IRedisPacket, IRedisPacket> handler,
            int priority
    ) {
        responseHandlers
                .computeIfAbsent(channel, c -> new ConcurrentHashMap<>())
                .computeIfAbsent(packetType, t -> new ConcurrentHashMap<>())
                .computeIfAbsent(subject, s -> new CopyOnWriteArrayList<>())
                .add(new ResponseHandlerWrapper<>(channel, packetType, subject, handler, priority));
        // 排序: 优先级高的在前
        responseHandlers.get(channel).get(packetType).get(subject)
                .sort((a, b) -> Integer.compare(b.priority, a.priority));
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
        Map<Class<? extends IRedisPacket>, Map<String, CopyOnWriteArrayList<ResponseHandlerWrapper<?>>>> typeMap = responseHandlers.get(channel);
        if (typeMap == null) return;
        Map<String, CopyOnWriteArrayList<ResponseHandlerWrapper<?>>> subjectMap = typeMap.get(packetType);
        if (subjectMap == null) return;
        CopyOnWriteArrayList<ResponseHandlerWrapper<?>> list = subjectMap.get(subject);
        if (list == null) return;
        list.removeIf(h -> h.handler().equals(handler));
    }

    /**
     * 注册带有 @RedisHandler 注解的方法
     * @param listener 监听器
     * @param method 方法对象
     */
    private <T extends IRedisPacket> void registerPublishMethod(RedisListener listener, Method method) {
        // 参数只能有一个
        if (method.getParameterCount() != 1) {
            throw new IllegalArgumentException("RedisListener 内部 RedisHandler 的方法只能有一个参数, 并且参数必须是 IRedisPacket 的实现类喵!");
        }
        Class<?> paramType = method.getParameterTypes()[0];
        if (!IRedisPacket.class.isAssignableFrom(paramType)) {
            throw new IllegalArgumentException("RedisListener 内部 RedisHandler 方法参数必须是 IRedisPacket 的实现类: " + method);
        }

        // 解析注解
        @SuppressWarnings("unchecked")
        Class<T> packetType = (Class<T>) paramType;
        RedisHandler annotation = method.getAnnotation(RedisHandler.class);
        String subject = annotation.subject();
        String channel = annotation.channel();
        int priority = annotation.priority();
        boolean autoSubscribe = annotation.autoSubscribe();

        // 调用目标方法
        Consumer<T> handler = packet -> {
            try {
                method.setAccessible(true);
                method.invoke(listener, packet);
            } catch (Exception e) {
                System.err.println("Error in Redis message handler: " + e.getMessage());
            }
        };

        // 创建处理器对象, 然后注册
        HandlerWrapper<T> wrapper = new HandlerWrapper<>(channel, packetType, subject, handler, priority);

        // 在 handlers 主表注册, 排序保证优先级
        handlers.computeIfAbsent(channel, c -> new ConcurrentHashMap<>())
                .computeIfAbsent(packetType, t -> new ConcurrentHashMap<>())
                .computeIfAbsent(subject, s -> new CopyOnWriteArrayList<>())
                .add(wrapper);
        // 排序: 优先级高的在前
        handlers.get(channel).get(packetType).get(subject)
                .sort((a, b) -> Integer.compare(b.priority, a.priority));

        // 保存到 registeredListeners 用于反注册
        registeredListeners.computeIfAbsent(listener, k -> ConcurrentHashMap.newKeySet()).add(wrapper);

        // 自动订阅频道
        if (autoSubscribe && !pubSubManager.isSubscribed(channel)) {
            pubSubManager.subscribeChannel(channel);
        }
    }

    /**
     * 注册带有 @RedisResponseHandler 注解的方法
     * @param listener 监听器
     * @param method 方法对象
     */
    private <T extends IRedisPacket> void registerResponseMethod(RedisListener listener, Method method) {
        // 参数只能有一个
        if (method.getParameterCount() != 2 || !IRedisPacket.class.isAssignableFrom(method.getParameterTypes()[0])) {
            throw new IllegalArgumentException("RedisListener 内部 RedisResponseHandler 的方法只能有2个参数, 第一个是接收到的数据包, 第二个是准备回复的数据包, 并且参数必须是 IRedisPacket 的实现类喵!");
        }
        if (!IRedisPacket.class.isAssignableFrom(method.getReturnType())) {
            throw new IllegalArgumentException("RedisListener 内部 RedisResponseHandler 方法必须返回一个 IRedisPacket 喵!");
        }

        // 解析注解
        @SuppressWarnings("unchecked")
        Class<T> packetType = (Class<T>) method.getParameterTypes()[0];
        RedisResponseHandler annotation = method.getAnnotation(RedisResponseHandler.class);
        String subject = annotation.subject();
        String channel = annotation.channel();
        int priority = annotation.priority();
        boolean autoSubscribe = annotation.autoSubscribe();

        // 调用目标方法
        BiFunction<T, IRedisPacket, IRedisPacket> handler = (packet, responsePacket) -> {
            try {
                method.setAccessible(true);
                return (IRedisPacket) method.invoke(listener, packet, responsePacket);
            } catch (Exception e) {
                System.err.println("Error in Redis message response handler: " + e.getMessage());
                return null;
            }
        };

        // 创建处理器对象, 保存到 registerResponseHandler 用于反注册
        ResponseHandlerWrapper<T> wrapper = new ResponseHandlerWrapper<>(channel, packetType, subject, handler, priority);

        // 在 responseHandlers 主表注册, 排序保证优先级
        responseHandlers
                .computeIfAbsent(channel, c -> new ConcurrentHashMap<>())
                .computeIfAbsent(packetType, t -> new ConcurrentHashMap<>())
                .computeIfAbsent(subject, s -> new CopyOnWriteArrayList<>())
                .add(wrapper);
        responseHandlers.get(channel).get(packetType).get(subject)
                .sort((a, b) -> Integer.compare(b.priority, a.priority));

        // 保存到 registeredListeners 用于反注册
        registeredListeners.computeIfAbsent(listener, k -> ConcurrentHashMap.newKeySet()).add(wrapper);

        // 自动订阅频道
        if (autoSubscribe && !pubSubManager.isSubscribed(channel)) {
            pubSubManager.subscribeChannel(channel);
        }
    }

    /**
     * 关闭消息路由
     */
    public void shutdown() {
        dispatchExecutor.shutdown();
        handlers.clear();
    }
}
