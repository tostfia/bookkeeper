package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import static org.junit.Assert.*;

public class ByteBufListRecycleTest {

    @Test
    public void testDeallocateReleasesAllBuffers() {
        // 1. Crea una nuova istanza
        ByteBufList list = ByteBufList.get();

        // 2. Aggiungi alcuni buffer
        ByteBuf b1 = Unpooled.buffer();
        ByteBuf b2 = Unpooled.buffer();
        list.add(b1);
        list.add(b2);

        // RefCnt iniziale deve essere 1
        assertEquals(1, b1.refCnt());
        assertEquals(1, b2.refCnt());

        // 3. Rilascia la lista (chiama deallocate)
        list.release();

        // 4. Verifica che i buffer interni siano stati rilasciati
        assertEquals("b1 non è stato rilasciato", 0, b1.refCnt());
        assertEquals("b2 non è stato rilasciato", 0, b2.refCnt());
    }

    @Test
    public void testDeallocateRecyclesInstance() {
        // 1. Ottieni la prima istanza
        ByteBufList firstInstance = ByteBufList.get();
        firstInstance.add(Unpooled.wrappedBuffer(new byte[]{10, 20, 30}));
        assertNotNull(firstInstance);

        // 2. Rilascia la prima istanza (invoca deallocate)
        firstInstance.release();

        // 3. Ottieni una seconda istanza
        ByteBufList secondInstance = ByteBufList.get();
        assertNotNull(secondInstance);



        // 4. Verifica che l’istanza sia stata riciclata (lista vuota e riutilizzabile)
        assertEquals("La lista riciclata dovrebbe essere vuota", 0, secondInstance.size());

        // 5. Pulizia
        secondInstance.release();
    }
}
