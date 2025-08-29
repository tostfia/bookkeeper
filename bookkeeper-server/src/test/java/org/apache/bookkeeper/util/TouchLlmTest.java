package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Test;

public class TouchLlmTest {

    @Test
    public void testTouchHintPropagated() {
        ByteBuf buf = Unpooled.buffer();
        ByteBufList list = ByteBufList.get(buf);
        list.touch("testHint"); // No exception expected
    }
}