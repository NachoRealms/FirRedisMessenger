package top.catnies.firredismessenger.pubsub;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import top.catnies.firredismessenger.pubsub.packet.IRedisPacket;

import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class RedisPubSubCallback {
    /* 维护数据 */
    // 存储：messageId -> PacketFuture
    @Getter private final Map<Integer, CallbackEntry> pendingMap = new ConcurrentHashMap<>(); // 正在等待超时回调任务
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2); // 回调执行器

    /* 关联对象 */
    private final RedisPubSubManager manager;

    public RedisPubSubCallback(RedisPubSubManager manager) {
        this.manager = manager;
    }

    // 回调键, 临时存储
    public static class CallbackEntry {
        final CompletableFuture<IRedisPacket> future;
        final ScheduledFuture<?> timeoutTask;
        CallbackEntry(CompletableFuture<IRedisPacket> future, ScheduledFuture<?> timeoutTask) {
            this.future = future;
            this.timeoutTask = timeoutTask;
        }
    }

    /**
     * 注册一个数据包回调任务
     *
     * @param messageId 数据包消息唯一 ID
     * @param responseCallback   对应的 future
     * @param timeoutMs 超时时间（0 或负数表示不设置超时）
     * @param timeoutCallback 超时回调，可为 null
     */
    public void register(int messageId, @NotNull Consumer<IRedisPacket> responseCallback, long timeoutMs, @Nullable Runnable timeoutCallback) {
        ScheduledFuture<?> timeoutTask = null;
        if (timeoutMs > 0 && timeoutCallback != null) {
            timeoutTask = scheduler.schedule(() -> {
                CallbackEntry entry = pendingMap.remove(messageId);
                if (entry != null) {
                    timeoutCallback.run();
                    entry.future.completeExceptionally(new TimeoutException("Packet [" + messageId + "] timed out"));
                }
            }, timeoutMs, TimeUnit.MILLISECONDS);
        }
        CompletableFuture<IRedisPacket> responseFuture = new CompletableFuture<>();
        responseFuture.thenAccept(responseCallback);
        pendingMap.put(messageId, new CallbackEntry(responseFuture, timeoutTask));
    }

    /**
     * 收到 RESPONSE 时调用
     */
    public <T extends IRedisPacket> void completeResponse(int messageId, T response) {
        CallbackEntry entry = pendingMap.get(messageId);
        if (entry == null) return;
        entry.future.complete(response);
        // 取消超时任务
        cancelTimeout(entry);
        pendingMap.remove(messageId);
    }

    /**
     * 出错时调用
     */
    private void cancelTimeout(CallbackEntry entry) {
        if (entry.timeoutTask != null && !entry.timeoutTask.isDone()) {
            entry.timeoutTask.cancel(false);
        }
    }

    /**
     * 关闭回调管理器，清理资源.
     */
    public void shutdown() {
        pendingMap.clear();
        scheduler.shutdownNow();
    }
}
