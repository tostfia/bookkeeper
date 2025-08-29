package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import static org.junit.Assert.*;

public class GetBufferLlmTest {

    @Test
    public void testGetValidIndex() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[]{1});
        ByteBufList list = ByteBufList.get(b1);
        assertEquals(1, list.getBuffer(0).getByte(0));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetInvalidIndex() {
        ByteBufList list = ByteBufList.get();
        list.getBuffer(0); // should throw
    }
}
