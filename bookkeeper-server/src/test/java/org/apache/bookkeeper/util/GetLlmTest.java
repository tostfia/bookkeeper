package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Test;
import static org.junit.Assert.*;

public class GetLlmTest {

    @Test
    public void testGetWithSingleBuffer() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(new byte[]{1, 2, 3});
        ByteBufList list = ByteBufList.get(buf);

        assertEquals(1, list.size());
        assertEquals(3, list.readableBytes());
        assertSame(buf, list.getBuffer(0));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetBufferOutOfBounds() {
        ByteBuf buf = Unpooled.buffer();
        ByteBufList list = ByteBufList.get(buf);
        list.getBuffer(1); // should throw
    }

    @Test
    public void testGetWithNullBufferAllowed() {
        ByteBufList list = ByteBufList.get((ByteBuf) null);
        assertEquals(1, list.size());
        assertNull(list.getBuffer(0)); // conferma che il buffer è null
    }
}