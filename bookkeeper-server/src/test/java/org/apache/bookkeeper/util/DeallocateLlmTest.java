package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.junit.Test;
import static org.junit.Assert.*;

public class DeallocateLlmTest {

    @Test
    public void testDeallocateReleasesBuffers() {
        ByteBuf b1 = Unpooled.buffer();
        b1.writeByte(1);
        ByteBufList list = ByteBufList.get(b1);

        assertEquals(1, b1.refCnt());
        list.release(); // triggers deallocate
        assertEquals(0, b1.refCnt());
    }

    @Test
    public void testDeallocateClearsList() {
        ByteBuf b1 = Unpooled.buffer();
        ByteBufList list = ByteBufList.get(b1);
        list.release();

        assertEquals(0, list.size());
    }
}
