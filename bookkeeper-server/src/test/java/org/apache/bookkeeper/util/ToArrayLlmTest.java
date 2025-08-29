package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Test;
import static org.junit.Assert.*;

public class ToArrayLlmTest {

    @Test
    public void testToArrayWithMultipleBuffers() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[]{1, 2});
        ByteBuf b2 = Unpooled.wrappedBuffer(new byte[]{3});
        ByteBufList list = ByteBufList.get(b1, b2);

        byte[] result = list.toArray();
        assertArrayEquals(new byte[]{1, 2, 3}, result);
    }

    @Test
    public void testToArrayEmpty() {
        ByteBufList list = ByteBufList.get();
        assertEquals(0, list.toArray().length);
    }
}
