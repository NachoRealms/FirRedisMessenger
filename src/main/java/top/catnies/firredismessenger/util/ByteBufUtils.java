package top.catnies.firredismessenger.util;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

public class ByteBufUtils {

    /**
     * 写入字符串: 先写入长度(4字节 int)，再写内容字节
     */
    public static void writeString(ByteBuf buf, String value) {
        if (value == null) {
            buf.writeInt(-1); // 表示null
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }

    /**
     * 读取字符串
     */
    public static String readString(ByteBuf buf) {
        int len = buf.readInt();
        if (len == -1) {
            return null;
        }
        byte[] bytes = new byte[len];
        buf.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * 写入字符串数组
     */
    public static void writeStringArray(ByteBuf buf, String[] arr) {
        if (arr == null) {
            buf.writeInt(-1); // null 数组
            return;
        }
        buf.writeInt(arr.length); // 数组长度
        for (String s : arr) {
            writeString(buf, s); // 循环写每个 String（支持 null）
        }
    }

    /**
     * 读取字符串数组
     */
    public static String[] readStringArray(ByteBuf buf) {
        int len = buf.readInt();
        if (len == -1) {
            return null; // null 数组
        }
        String[] arr = new String[len];
        for (int i = 0; i < len; i++) {
            arr[i] = readString(buf); // 读取每个 String（支持 null）
        }
        return arr;
    }

}
