package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class HasArrayTest {

    private ByteBufList bufList;

    @Before
    public void setup() {
        bufList = ByteBufList.get();
    }

    @After
    public void teardown() {
        bufList.release();
    }

    @Test
    public void testHasArrayWithHeapBuffers() {
        byte[] data1 = "hello".getBytes();
        byte[] data2 = "world".getBytes();

        bufList.add(Unpooled.wrappedBuffer(data1));
        bufList.add(Unpooled.wrappedBuffer(data2));


        assertFalse(bufList.hasArray());
    }

    @Test
    public void testHasArrayWithDirectBuffers() {
        ByteBuf buf1 = Unpooled.directBuffer();
        buf1.writeBytes("hello".getBytes());

        ByteBuf buf2 = Unpooled.directBuffer();
        buf2.writeBytes("world".getBytes());

        bufList.add(buf1);
        bufList.add(buf2);

        // direct buffer non ha backing array → deve essere false
        assertFalse(bufList.hasArray());
    }

    @Test
    public void testHasArrayMixedBuffers() {
        byte[] data = "hello".getBytes();
        bufList.add(Unpooled.wrappedBuffer(data)); // heap
        bufList.add(Unpooled.directBuffer().writeBytes("world".getBytes())); // direct

        assertFalse(bufList.hasArray());
    }

    @Test
    public void testHasArrayWithSingleHeapBuffer() {
        byte[] data = "hello".getBytes();
        bufList.add(Unpooled.wrappedBuffer(data));

        assertTrue(bufList.hasArray());
    }

}
