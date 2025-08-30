package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RetainTest {

    //La classe aumenta solo un contatore di riferimenti

    private ByteBufList bufList;

    @Before
    public void setUp() {
        bufList = ByteBufList.get();
    }

    @After
    public void tearDown() {
        bufList.release();
    }

    @Test
    public void testRetain() {
        ByteBufList list = ByteBufList.get();
        int initial = list.refCnt();
        assertEquals(1, initial);

        // retain singolo
        ByteBufList returned = list.retain();
        assertEquals(list, returned);         // ritorna se stesso
        assertEquals(initial + 1, list.refCnt());

        // retain multiplo
        list.retain();
        assertEquals(initial + 2, list.refCnt());

        // release singolo
        list.release();
        assertEquals(initial + 1, list.refCnt());

        list.release();
        assertEquals(initial, list.refCnt());

        // release finale
        list.release();
        assertEquals(0, list.refCnt());
    }

}
