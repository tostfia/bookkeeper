package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Test;
import static org.junit.Assert.*;

public class PrependLlmTest {

    @Test
    public void testPrependSingleBuffer() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[]{1});
        ByteBuf b2 = Unpooled.wrappedBuffer(new byte[]{2});
        ByteBufList list = ByteBufList.get(b1);
        list.prepend(b2);

        assertEquals(2, list.size());
        assertEquals(2, list.readableBytes());
        assertEquals(2, list.getBuffer(0).getByte(0)); // b2 is now first
    }

    @Test
    public void testPrependToEmptyList() {
        ByteBuf b = Unpooled.wrappedBuffer(new byte[]{9});
        ByteBufList list = ByteBufList.get();
        list.prepend(b);

        assertEquals(1, list.size());
        assertEquals(9, list.getBuffer(0).getByte(0));
    }
}