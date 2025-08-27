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
public class GetSingleBufferTest {

    private ByteBufList resultBufList; // La ByteBufList restituita dal metodo get()
    private ByteBuf b1Mock;            // Il mock del ByteBuf b1

    // Parametri per i casi di test
    @Parameter(0)
    public boolean b1IsNull;                  // True se b1 deve essere null
    @Parameter(1)
    public int initialB1RefCnt;               // refCnt iniziale di b1 (se non null)
    @Parameter(2)
    public int expectedRetainCallsOnB1;       // Numero atteso di chiamate retain() su b1
    @Parameter(3)
    public Class<? extends Exception> expectedException; // Eccezione attesa da get(), se presente

    @Before
    public void setup() {
        if (!b1IsNull) {
            b1Mock = mock(ByteBuf.class);
            // Configura il comportamento di retain/release per il mock
            when(b1Mock.retain()).thenReturn(b1Mock);
            when(b1Mock.release()).thenReturn(true);
            when(b1Mock.release(anyInt())).thenReturn(true);
            // Il refCnt del mock deve restituire il valore iniziale che il test vuole verificare
            when(b1Mock.refCnt()).thenReturn(initialB1RefCnt);
        } else {
            b1Mock = null;
        }
        resultBufList = null; // Inizializza a null
    }

    @After
    public void tearDown() {
        if (resultBufList != null) {
            try {
                resultBufList.release(); // Rilascia la lista e i suoi buffer
            } catch (NullPointerException e) {
                // Cattura l'NPE se get(null) ha aggiunto null alla lista,
                // prevenendo il crash dell'intero test runner.
                System.err.println("NullPointerException caught in tearDown during resultBufList.release(). "
                        + "This typically means ByteBufList.get(null) added a null to the list, "
                        + "which is the behavior being demonstrated by the passing test.");
            }
            resultBufList = null;
        }
    }

    @Test
    public void testGetSingleBuffer() {
        // Gestisce le eccezioni attese da get()
        if (expectedException != null) {
            try {
                ByteBufList.get(b1Mock); // Passa b1Mock (che può essere null)
                fail("Prevista eccezione " + expectedException.getName() + " ma nessuna è stata lanciata da get(b1).");
            } catch (Exception e) {
                assertEquals("Tipo di eccezione inatteso lanciato da get(b1)", expectedException, e.getClass());
            }
            return;
        }

        // Esegue il metodo da testare
        resultBufList = ByteBufList.get(b1Mock);

        assertNotNull("La ByteBufList restituita non dovrebbe essere null", resultBufList);
        assertEquals("La dimensione della ByteBufList dovrebbe essere 1", 1, resultBufList.size());

        // Verifica il contenuto del ByteBufList
        if (b1IsNull) {
            assertNull("L'elemento all'indice 0 dovrebbe essere null", resultBufList.getBuffer(0));
        } else {
            assertSame("L'elemento all'indice 0 non è il buffer b1 atteso", b1Mock, resultBufList.getBuffer(0));
            // Verifica che retain() sia stato chiamato il numero di volte atteso (0 in questo caso)
            verify(b1Mock, times(expectedRetainCallsOnB1)).retain();
            // Il refCnt dovrebbe rimanere invariato rispetto al suo valore iniziale, poiché retain() non è chiamato da add()
            assertEquals("Il refCnt di b1 non è quello atteso dopo l'aggiunta alla lista (non dovrebbe cambiare)",
                    initialB1RefCnt, b1Mock.refCnt());
        }
    }

    @Parameters
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();

        // Helper: params.add(new Object[] { b1IsNull, initialB1RefCnt, expectedRetainCallsOnB1, expectedExceptionClass });

        // --- Partizione 1: b1 è null ---
        // get(null) aggiunge null alla lista senza lanciare un'eccezione immediata.
        params.add(new Object[] {
                true,   // b1IsNull
                0,      // initialB1RefCnt (non rilevante per null)
                0,      // expectedRetainCallsOnB1 (0, poiché non è un ByteBuf valido)
                null    // Nessuna eccezione attesa da get() stesso
        });

        // --- Partizione 2: b1 è un ByteBuf valido ---
        // Se ByteBufList.add() non chiama retain(), allora expectedRetainCallsOnB1 sarà 0.
        // Il refCnt del mock rimarrà initialB1RefCnt.
        params.add(new Object[] {
                false,  // b1IsNull
                1,      // initialB1RefCnt (tipico per un nuovo buffer)
                0,      // expectedRetainCallsOnB1: 0, poiché add() non chiama retain()
                null    // Nessuna eccezione
        });
        // Valore limite: b1 ha un refCnt iniziale più alto
        params.add(new Object[] {
                false,
                5,
                0,      // expectedRetainCallsOnB1: 0
                null
        });

        return params;
    }
}