package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.junit.Test;
import static org.junit.Assert.*;

public class ReadableBytesLlmTest {

    @Test
    public void testEmptyList() {
        ByteBufList list = ByteBufList.get();
        assertEquals(0, list.readableBytes());
    }

    @Test
    public void testMultipleBuffers() {
        ByteBuf b1 = Unpooled.buffer();
        b1.writeBytes(new byte[]{1, 2});
        ByteBuf b2 = Unpooled.buffer();
        b2.writeBytes(new byte[]{3, 4, 5});
        ByteBufList list = ByteBufList.get(b1, b2);

        assertEquals(5, list.readableBytes());
    }
}