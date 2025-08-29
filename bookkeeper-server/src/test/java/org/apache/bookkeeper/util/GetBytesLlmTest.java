package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import static org.junit.Assert.*;

public class GetBytesLlmTest {

    @Test
    public void testGetBytesPartialCopy() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[]{1, 2});
        ByteBuf b2 = Unpooled.wrappedBuffer(new byte[]{3, 4});
        ByteBufList list = ByteBufList.get(b1, b2);

        byte[] dst = new byte[3];
        int copied = list.getBytes(dst);
        assertEquals(3, copied);
        assertArrayEquals(new byte[]{1, 2, 3}, dst);
    }

    @Test
    public void testGetBytesEmptyList() {
        ByteBufList list = ByteBufList.get();
        byte[] dst = new byte[5];
        int copied = list.getBytes(dst);
        assertEquals(0, copied);
    }
}
