package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class ArrayOffsetTest {

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
     * hasArray() deve essere true e array() + arrayOffset() restituiscono i dati corretti.
     */
    @Test
    public void testSingleHeapBuffer() {
        byte[] data = "hello".getBytes();
        bufList.add(Unpooled.wrappedBuffer(data));

        assertTrue(bufList.hasArray());

        byte[] backing = bufList.array();
        int offset = bufList.arrayOffset();
        int length = bufList.readableBytes();

        byte[] actual = new byte[length];
        System.arraycopy(backing, offset, actual, 0, length);

        assertArrayEquals(data, actual);
        assertEquals(0, offset); // wrappedBuffer di solito ha offset 0
    }

    /**
     * Caso: più heap buffer.
     * hasArray() deve essere false, quindi non si chiama array()
     */
    @Test
    public void testMultipleHeapBuffers() {
        bufList.add(Unpooled.wrappedBuffer("hello".getBytes()));
        bufList.add(Unpooled.wrappedBuffer("world".getBytes()));

        assertFalse(bufList.hasArray());
        // Non chiamare bufList.array(), contrattualmente non valido
    }

    /**
     * Caso: solo direct buffer.
     * hasArray() deve essere false e array() / arrayOffset() lancia eccezione.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testDirectBuffers() {
        ByteBuf buf1 = Unpooled.directBuffer().writeBytes("hello".getBytes());
        bufList.add(buf1);

        assertFalse(bufList.hasArray());
        bufList.array();        // deve lanciare
        bufList.arrayOffset();  // deve lanciare
    }

    /**
     * Caso: heap + direct buffer misti.
     * hasArray() deve essere false, quindi non si chiama array()
     */
    @Test
    public void testMixedBuffers() {
        bufList.add(Unpooled.wrappedBuffer("hello".getBytes()));             // heap
        bufList.add(Unpooled.directBuffer().writeBytes("world".getBytes())); // direct

        assertFalse(bufList.hasArray());
        // Non chiamare array() / arrayOffset() perché non valido
    }
}

