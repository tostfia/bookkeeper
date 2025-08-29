package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Test;
import static org.junit.Assert.*;

public class RetainLlmTest {

    @Test
    public void testRetainIncreasesRefCount() {
        ByteBuf buf = Unpooled.buffer();
        ByteBufList list = ByteBufList.get(buf);
        int initial = list.refCnt();
        list.retain();
        assertEquals(initial + 1, list.refCnt());
    }
}
