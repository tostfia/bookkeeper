package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertArrayEquals;

@RunWith(Parameterized.class)
public class ToArrayTest {

    private ByteBufList bufList;

    @Before
    public void setUp() {
        bufList = ByteBufList.get();
    }

    @After
    public void tearDown() {
        bufList.release();
    }

    @Parameterized.Parameters
    public static Object[][] data() {
        return new Object[][] {
                { new byte[][]{} },                // caso vuoto
                { new byte[][]{"hello".getBytes() }},          // caso singolo buffer
                { new byte[][]{"hello".getBytes(), "world".getBytes()} } // caso multiplo
        };
    }

    private final byte[][] input;

    public ToArrayTest(byte[][] input) {
        this.input = input;
    }

    @Test
    public void testToArray() {
        // aggiungo i buffer
        for (byte[] arr : input) {
            ByteBuf buf = Unpooled.wrappedBuffer(arr);
            bufList.add(buf);
        }

        // mi costruisco l’array atteso (concatenazione)
        int totalLen = 0;
        for (byte[] arr : input) {
            totalLen += arr.length;
        }
        byte[] expected = new byte[totalLen];
        int pos = 0;
        for (byte[] arr : input) {
            System.arraycopy(arr, 0, expected, pos, arr.length);
            pos += arr.length;
        }

        // chiamo toArray
        byte[] actual = bufList.toArray();

        // confronto
        assertArrayEquals(expected, actual);
    }
}
