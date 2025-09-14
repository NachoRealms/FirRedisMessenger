package top.catnies.firredismessenger.pubsub;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import top.catnies.firredismessenger.pubsub.packet.IRedisPacket;

import java.util.Map;
import java.util.concurrent.*;

public class RedisPubSubCallback {
    // 存储：messageId -> PacketFuture
    @Getter private final Map<Integer, CallbackEntry> pendingMap = new ConcurrentHashMap<>(); // 正在等待超时回调任务
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2); // 回调执行器

    // 回调键, 临时存储
    public static class CallbackEntry {
        final RedisPacketFuture future;
        final ScheduledFuture<?> timeoutTask;
        CallbackEntry(RedisPacketFuture future, ScheduledFuture<?> timeoutTask) {
            this.future = future;
            this.timeoutTask = timeoutTask;
        }
        // 是否两个回调都完成了
        public boolean allDone() {
            return future.isAckDone() && future.isResponseDone();
        }
    }

    /**
     * 注册一个数据包回调任务
     *
     * @param messageId 数据包消息唯一 ID
     * @param future   对应的 future
     * @param timeoutMs 超时时间（0 或负数表示不设置超时）
     * @param timeoutCallback 超时回调，可为 null
     */
    public void register(int messageId, @NotNull RedisPacketFuture future, long timeoutMs, @Nullable Runnable timeoutCallback) {
        ScheduledFuture<?> timeoutTask = null;
        if (timeoutMs > 0 && timeoutCallback != null) {
            timeoutTask = scheduler.schedule(() -> {
                CallbackEntry entry = pendingMap.remove(messageId);
                if (entry != null) {
                    timeoutCallback.run();
                    entry.future.completeException(new TimeoutException("Packet [" + messageId + "] timed out"));
                }
            }, timeoutMs, TimeUnit.MILLISECONDS);
        }
        pendingMap.put(messageId, new CallbackEntry(future, timeoutTask));
    }

    /**
     * 收到 ACK 时调用
     */
    public void completeAck(int messageId) {
        CallbackEntry entry = pendingMap.get(messageId);
        if (entry == null) return;
        entry.future.completeAck();
        // 收到 ack 就取消超时
        cancelTimeout(entry);
        // 如果 ack & response 都完成了，就删除
        if (entry.allDone()) {
            pendingMap.remove(messageId);
        }
    }

    /**
     * 收到 RESPONSE 时调用
     */
    public <T extends IRedisPacket> void completeResponse(int messageId, T response) {
        CallbackEntry entry = pendingMap.get(messageId);
        if (entry == null) return;
        entry.future.completeResponse(response);
        // 收到 response 就取消超时
        cancelTimeout(entry);
        // 如果 ack & response 都完成了，就删除
        if (entry.allDone()) {
            pendingMap.remove(messageId);
        }
    }

    /**
     * 出错时调用
     */
    public void completeException(String packetId, Throwable t) {
        CallbackEntry entry = pendingMap.remove(packetId);
        if (entry != null) {
            cancelTimeout(entry);
            entry.future.completeException(t);
        }
    }
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
