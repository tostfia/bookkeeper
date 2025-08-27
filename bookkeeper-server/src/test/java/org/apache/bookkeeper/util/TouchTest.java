package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*; // Importa i metodi statici di Mockito

@RunWith(Parameterized.class)
public class TouchTest {

    private ByteBufList bufList;
    private List<ByteBuf> mockBuffers; // Lista per tenere traccia dei mock ByteBuf creati

    // Parametri per i casi di test
    @Parameter(0)
    public List<Integer> bufferSizes; // Lista di dimensioni per creare mock ByteBuf (rappresenta i buffer interni)
    @Parameter(1)
    public Object hint;               // L'oggetto hint da passare a touch()
    @Parameter(2)
    public Class<? extends Exception> expectedException; // Eccezione attesa, se presente

    @Before
    public void setup() {
        bufList = ByteBufList.get(); // Ottiene un'istanza di ByteBufList
        mockBuffers = new ArrayList<>();

        if (bufferSizes != null) {
            for (Integer size : bufferSizes) {
                // Crea un mock di ByteBuf
                ByteBuf mockBuf = mock(ByteBuf.class);
                // Configura i metodi retain/release per i mock per evitare IllegalReferenceCountException
                when(mockBuf.retain()).thenReturn(mockBuf);
                when(mockBuf.release()).thenReturn(true);
                when(mockBuf.release(anyInt())).thenReturn(true);
                when(mockBuf.refCnt()).thenReturn(1); // Default ref count for mocks

                bufList.add(mockBuf); // Aggiunge il mock alla ByteBufList
                mockBuffers.add(mockBuf); // Aggiunge il mock alla nostra lista di riferimento
            }
        }
    }

    @After
    public void tearDown() {
        if (bufList != null) {
            // Rilascia la ByteBufList. Questo dovrebbe, in teoria, rilasciare anche i suoi buffer interni.
            // Poiché stiamo usando mock, il loro metodo release() è stato configurato per ritornare true.
            bufList.release();
            bufList = null;
        }
        mockBuffers.clear(); // Pulisce la lista dei mock
    }

    @Test
    public void testTouch() {
        // Gestisce i casi in cui ci si aspetta un'eccezione
        if (expectedException != null) {
            try {
                bufList.touch(hint);
                fail("Prevista eccezione " + expectedException.getName() + " ma nessuna è stata lanciata.");
            } catch (Exception e) {
                assertEquals("Tipo di eccezione inatteso", expectedException, e.getClass());
            }
            return;
        }

        // Invoca il metodo da testare
        ReferenceCounted returnedRef = bufList.touch(hint);

        // Verifica che il metodo ritorni l'istanza stessa di ByteBufList
        assertSame("L'oggetto restituito dovrebbe essere 'this' (l'istanza di ByteBufList)", bufList, returnedRef);

        // Verifica che touch(hint) sia stato chiamato su ciascun mock ByteBuf esattamente una volta
        for (ByteBuf mockBuf : mockBuffers) {
            verify(mockBuf, times(1)).touch(hint);
            // Si può anche verificare che non siano state chiamate altre varianti di touch()
            verify(mockBuf, never()).touch();
        }

        // Per una lista vuota, 'mockBuffers' è vuoto, quindi il ciclo precedente non viene eseguito,
        // il che è il comportamento corretto (nessun touch su nessun buffer).
    }

    @Parameters
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();

        // Helper: params.add(new Object[] { bufferSizes, hint, expectedExceptionClass });

        // --- Partizione: ByteBufList è vuota (buffers.size() == 0) ---
        // 1. Hint è null
        params.add(new Object[] {
                Collections.emptyList(), // bufferSizes: lista vuota, nessun mock creato
                null,                    // hint
                null                     // Nessuna eccezione attesa
        });
        // 2. Hint è un oggetto non null
        params.add(new Object[] {
                Collections.emptyList(),
                "test_hint_object",      // Un oggetto String come hint
                null
        });

        // --- Partizione: ByteBufList contiene un singolo ByteBuf (buffers.size() == 1) ---
        // 1. Hint è null
        params.add(new Object[] {
                Arrays.asList(10),       // bufferSizes: 1 mock ByteBuf (dimensione 10 arbitraria)
                null,
                null
        });
        // 2. Hint è un oggetto non null
        params.add(new Object[] {
                Arrays.asList(20),
                "single_buffer_hint",    // Un oggetto String come hint
                null
        });

        // --- Partizione: ByteBufList contiene più ByteBuf (buffers.size() > 1) ---
        // Useremo 3 buffer come rappresentazione di "più di uno"
        // 1. Hint è null
        params.add(new Object[] {
                Arrays.asList(10, 20, 30), // bufferSizes: 3 mock ByteBufs
                null,
                null
        });
        // 2. Hint è un oggetto non null
        params.add(new Object[] {
                Arrays.asList(5, 15, 25),
                new Object(),            // Un oggetto generico come hint
                null
        });

        return params;
    }
}
