package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Test;
import static org.junit.Assert.*;

public class CoalesceLlmTest {

    @Test
    public void testCoalesceMultipleBuffers() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[]{1, 2});
        ByteBuf b2 = Unpooled.wrappedBuffer(new byte[]{3});
        ByteBufList list = ByteBufList.get(b1, b2);

        ByteBuf result = ByteBufList.coalesce(list);
        assertEquals(3, result.readableBytes());
        assertEquals(1, result.readByte());
        assertEquals(2, result.readByte());
        assertEquals(3, result.readByte());
    }

    @Test
    public void testCoalesceEmptyList() {
        ByteBufList list = ByteBufList.get();
        ByteBuf result = ByteBufList.coalesce(list);
        assertEquals(0, result.readableBytes());
    }
}
