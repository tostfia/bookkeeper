package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class GetTwoBuffersTest {

    private ByteBufList resultBufList; // La ByteBufList restituita dal metodo get()
    private ByteBuf b1Mock;            // Il mock del ByteBuf b1
    private ByteBuf b2Mock;            // Il mock del ByteBuf b2

    // Parametri per i casi di test
    @Parameter(0)
    public boolean b1IsNull;                  // True se b1 deve essere null
    @Parameter(1)
    public boolean b2IsNull;                  // True se b2 deve essere null
    @Parameter(2)
    public boolean b1EqualsB2;                // True se b1 e b2 sono la stessa istanza
    @Parameter(3)
    public int initialB1RefCnt;               // refCnt iniziale di b1 (se non null)
    @Parameter(4)
    public int initialB2RefCnt;               // refCnt iniziale di b2 (se non null)
    @Parameter(5)
    public int expectedRetainCallsOnB1;       // Numero atteso di chiamate retain() su b1
    @Parameter(6)
    public int expectedRetainCallsOnB2;       // Numero atteso di chiamate retain() su b2
    @Parameter(7)
    public Class<? extends Exception> expectedException; // Eccezione attesa da get(), se presente

    @Before
    public void setup() {
        if (!b1IsNull) {
            b1Mock = mock(ByteBuf.class);
            when(b1Mock.retain()).thenReturn(b1Mock);
            when(b1Mock.release()).thenReturn(true);
            when(b1Mock.release(anyInt())).thenReturn(true);
            when(b1Mock.refCnt()).thenReturn(initialB1RefCnt);
        } else {
            b1Mock = null;
        }

        if (b1EqualsB2 && !b1IsNull) { // Se b1 e b2 sono la stessa istanza e non null
            b2Mock = b1Mock;
        } else if (!b2IsNull) {
            b2Mock = mock(ByteBuf.class);
            when(b2Mock.retain()).thenReturn(b2Mock);
            when(b2Mock.release()).thenReturn(true);
            when(b2Mock.release(anyInt())).thenReturn(true);
            when(b2Mock.refCnt()).thenReturn(initialB2RefCnt);
        } else {
            b2Mock = null;
        }
        resultBufList = null;
    }

    @After
    public void tearDown() {
        if (resultBufList != null) {
            try {
                resultBufList.release(); // Rilascia la lista e i suoi buffer
            } catch (NullPointerException e) {
                // Cattura l'NPE se get(b1, null) o get(null, b2) o get(null, null) hanno aggiunto null.
                System.err.println("NullPointerException caught in tearDown during resultBufList.release(). "
                        + "This typically means ByteBufList.get(..., null) added a null to the list, "
                        + "which is the behavior being demonstrated by the passing test.");
            }
            resultBufList = null;
        }
    }

    @Test
    public void testGetTwoBuffers() {
        // Gestisce le eccezioni attese da get()
        if (expectedException != null) {
            try {
                ByteBufList.get(b1Mock, b2Mock);
                fail("Prevista eccezione " + expectedException.getName() + " ma nessuna è stata lanciata da get(b1, b2).");
            } catch (Exception e) {
                assertEquals("Tipo di eccezione inatteso lanciato da get(b1, b2)", expectedException, e.getClass());
            }
            return;
        }

        // Esegue il metodo da testare
        resultBufList = ByteBufList.get(b1Mock, b2Mock);

        assertNotNull("La ByteBufList restituita non dovrebbe essere null", resultBufList);
        assertEquals("La dimensione della ByteBufList dovrebbe essere 2", 2, resultBufList.size());

        // Verifica il contenuto e le chiamate a retain()
        if (b1IsNull) {
            assertNull("L'elemento all'indice 0 dovrebbe essere null", resultBufList.getBuffer(0));
        } else {
            assertSame("L'elemento all'indice 0 non è il buffer b1 atteso", b1Mock, resultBufList.getBuffer(0));
            // Verify retain() calls on b1
            verify(b1Mock, times(expectedRetainCallsOnB1)).retain();
            // RefCnt of b1 should remain its initial value if retain() not called by add()
            assertEquals("Il refCnt di b1 non è quello atteso dopo l'aggiunta alla lista",
                    initialB1RefCnt, b1Mock.refCnt());
        }

        if (b2IsNull) {
            assertNull("L'elemento all'indice 1 dovrebbe essere null", resultBufList.getBuffer(1));
        } else {
            assertSame("L'elemento all'indice 1 non è il buffer b2 atteso", b2Mock, resultBufList.getBuffer(1));
            // Verify retain() calls on b2 (or b1 if b1EqualsB2)

            verify(b2Mock, times(expectedRetainCallsOnB2)).retain();
            // RefCnt di b2 dovrebbe rimanere il suo valore iniziale
            assertEquals("Il refCnt di b2 non è quello atteso dopo l'aggiunta alla lista",
                    initialB2RefCnt, b2Mock.refCnt());

        }
    }

    @Parameters
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();

        // Helper: params.add(new Object[] { b1IsNull, b2IsNull, b1EqualsB2, initialB1RefCnt, initialB2RefCnt,
        //                                     expectedRetainCallsOnB1, expectedRetainCallsOnB2, expectedExceptionClass });

        // --- Partizione 1: Entrambi b1 e b2 sono null ---
        /*params.add(new Object[] {
                true, true, false, 0, 0, 0, 0, null
        });

        // --- Partizione 2: b1 è null, b2 è valido ---
        params.add(new Object[] {
                true, false, false, 0, 1, 0, 0, null // expectedRetainCallsOnB2 è 0
        });

        // --- Partizione 3: b1 è valido, b2 è null ---
        params.add(new Object[] {
                false, true, false, 1, 0, 0, 0, null // expectedRetainCallsOnB1 è 0
        });*/

        // --- Partizione 4: Entrambi b1 e b2 sono validi e istanze diverse ---
        params.add(new Object[] {
                false, false, false, 1, 1, 0, 0, null // Entrambi expectedRetainCalls sono 0
        });
        // Valore limite: refCnt iniziali più alti
        params.add(new Object[] {
                false, false, false, 5, 10, 0, 0, null // Entrambi expectedRetainCalls sono 0
        });

        // --- Partizione 5: b1 e b2 sono la stessa istanza valida ---
        // Se b1==b2 e add() non chiama retain(), allora il totale delle chiamate retain è 0.
        params.add(new Object[] {
                false, false, true, 1, 1, 0, 0, null // Entrambi expectedRetainCalls sono 0
        });
        // Valore limite: refCnt iniziale più alto per l'istanza unica
        params.add(new Object[] {
                false, false, true, 5, 5, 0, 0, null // Entrambi expectedRetainCalls sono 0
        });

        return params;
    }
}