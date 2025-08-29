package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.junit.Test;
import static org.junit.Assert.*;



public class HasArrayLlmTest {

    @Test
    public void testHasArrayTrueSingleArrayBackedBuffer() {
        ByteBuf buf = Unpooled.wrappedBuffer(new byte[]{1, 2});
        ByteBufList list = ByteBufList.get(buf);
        assertTrue(list.hasArray());
    }

    @Test
    public void testHasArrayFalseMultipleBuffers() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[]{1});
        ByteBuf b2 = Unpooled.wrappedBuffer(new byte[]{2});
        ByteBufList list = ByteBufList.get(b1, b2);
        assertFalse(list.hasArray());
    }

    @Test
    public void testHasArrayFalseNoArrayBackedBuffer() {
        ByteBuf buf = Unpooled.directBuffer(); // garantito non array-backed
        buf.writeBytes(new byte[]{1, 2});
        ByteBufList list = ByteBufList.get(buf);

        assertFalse("Expected hasArray() to be false", list.hasArray());
    }
}