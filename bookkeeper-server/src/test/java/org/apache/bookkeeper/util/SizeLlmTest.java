package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Test;
import static org.junit.Assert.*;

public class SizeLlmTest {

    @Test
    public void testSizeWithMultipleBuffers() {
        ByteBuf b1 = Unpooled.buffer();
        ByteBuf b2 = Unpooled.buffer();
        ByteBufList list = ByteBufList.get(b1, b2);
        assertEquals(2, list.size());
    }

    @Test
    public void testSizeEmptyList() {
        ByteBufList list = ByteBufList.get();
        assertEquals(0, list.size());
    }
}
