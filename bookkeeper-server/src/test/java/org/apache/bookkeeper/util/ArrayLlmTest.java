package org.apache.bookkeeper.util;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Test;
import static org.junit.Assert.*;

public class ArrayLlmTest {

    @Test
    public void testArrayAccess() {
        byte[] data = new byte[]{42, 43};
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        ByteBufList list = ByteBufList.get(buf);
        assertArrayEquals(data, list.array());
    }

    @Test
    public void testArrayAccessMultipleBuffers() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[]{1});
        ByteBuf b2 = Unpooled.wrappedBuffer(new byte[]{2});
        ByteBufList list = ByteBufList.get(b1, b2);

        // Verifica che non sia backed da un singolo array
        assertFalse("Should not be backed by a single array", list.hasArray());

        // La chiamata a array() non lancia eccezione, ma il comportamento è indefinito
        byte[] result = list.array(); // accede comunque al primo buffer
        assertArrayEquals(new byte[]{1}, result); // verifica che restituisca l'array del primo buffer
    }
}
