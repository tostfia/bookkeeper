package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class ArrayTest {

    private ByteBufList bufList;

    @Before
    public void setUp() {
        bufList = ByteBufList.get();
    }

    @After
    public void tearDown() {
        bufList.release();
    }

    /**
     * Caso: un singolo heap buffer.
     * hasArray() deve essere true e array() deve restituire il backing array originale.
     */
    @Test
    public void testArrayWithSingleHeapBuffer() {
        byte[] data = "hello".getBytes();
        bufList.add(Unpooled.wrappedBuffer(data));

        assertTrue(bufList.hasArray());
        assertSame(data, bufList.array()); // deve restituire lo stesso array, non una copia
    }



    /**
     * Caso: solo direct buffer.
     * hasArray() deve essere false e array() deve lanciare eccezione.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testArrayWithDirectBuffers() {
        ByteBuf buf1 = Unpooled.directBuffer().writeBytes("hello".getBytes());
        ByteBuf buf2 = Unpooled.directBuffer().writeBytes("world".getBytes());

        bufList.add(buf1);
        bufList.add(buf2);

        assertFalse(bufList.hasArray());
        bufList.array(); // deve lanciare
    }


}
