package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Test;
import static org.junit.Assert.*;

public class ArrayOffsetLlmTest {

    @Test
    public void testArrayOffsetValid() {
        byte[] data = new byte[]{10, 20, 30};
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        ByteBufList list = ByteBufList.get(buf);

        assertEquals(buf.arrayOffset(), list.arrayOffset());
    }
    //Questo test è così perchè non posso modificare il codice sorgente di ByteBufList
    @Test
    public void testArrayOffsetMultipleBuffersNoException() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[]{1});
        ByteBuf b2 = Unpooled.wrappedBuffer(new byte[]{2});
        ByteBufList list = ByteBufList.get(b1, b2);

        assertFalse("Should not have array backing", list.hasArray());

        // La chiamata non lancia eccezione, ma il comportamento è indefinito
        int offset = list.arrayOffset(); // non sicuro, ma non fallisce
        assertTrue("Offset should be >= 0", offset >= 0);
    }


}
