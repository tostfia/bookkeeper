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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class CloneTest {

    // L'istanza di ByteBufList che verrà usata come input 'other' al metodo clone
    private ByteBufList originalBufList;
    // Lista per tenere traccia dei mock ByteBuf originali aggiunti a 'originalBufList'
    private List<ByteBuf> originalMocks;
    // Lista per tenere traccia dei mock ByteBuf che verranno restituiti da retainedDuplicate()
    private List<ByteBuf> duplicateMocks;

    // Parametri per i casi di test
    @Parameter(0)
    public int initialNumBuffers; // Numero di ByteBuf nella lista originale. Usato -1 per indicare 'other' null.
    @Parameter(1)
    public Class<? extends Exception> expectedException; // Eccezione attesa da clone(), se presente

    @Before
    public void setup() {
        originalMocks = new ArrayList<>();
        duplicateMocks = new ArrayList<>();

        if (initialNumBuffers != -1) { // Non creiamo originalBufList se il test è per clone(null)
            originalBufList = ByteBufList.get(); // Ottiene una vera istanza di ByteBufList
            for (int i = 0; i < initialNumBuffers; i++) {
                ByteBuf originalMockBuf = mock(ByteBuf.class);
                ByteBuf duplicateMockBuf = mock(ByteBuf.class); // Il mock che retainedDuplicate() dovrebbe restituire

                // Configura il comportamento per originalMockBuf
                when(originalMockBuf.retainedDuplicate()).thenReturn(duplicateMockBuf);
                // Configura retain/release di default per i mock originali
                when(originalMockBuf.retain()).thenReturn(originalMockBuf);
                when(originalMockBuf.release()).thenReturn(true);
                when(originalMockBuf.release(anyInt())).thenReturn(true);
                when(originalMockBuf.refCnt()).thenReturn(1); // Assumiamo refCnt iniziale 1

                // Configura il comportamento per duplicateMockBuf (il "duplicato")
                // Dovrebbe iniziare con refCnt 1 poiché è "retainedDuplicate"
                when(duplicateMockBuf.retain()).thenReturn(duplicateMockBuf);
                when(duplicateMockBuf.release()).thenReturn(true);
                when(duplicateMockBuf.release(anyInt())).thenReturn(true);
                when(duplicateMockBuf.refCnt()).thenReturn(1); // refCnt iniziale per il duplicato

                originalBufList.add(originalMockBuf); // Aggiunge il mock originale alla lista
                originalMocks.add(originalMockBuf);
                duplicateMocks.add(duplicateMockBuf);
            }
        } else {
            originalBufList = null; // Per il caso clone(null)
        }
    }

    @After
    public void tearDown() {
        if (originalBufList != null) {
            originalBufList.release(); // Rilascia la lista originale e i suoi mock
            originalBufList = null;
        }
        // Non è necessario rilasciare individualmente originalMocks o duplicateMocks qui,
        // poiché sono gestiti da originalBufList.release() o sono stati aggiunti alla clonedList.
        // Ho configurato i loro metodi .release() per restituire true.
        originalMocks.clear();
        duplicateMocks.clear();
    }

    @Test
    public void testClone() {
        // Gestisce le eccezioni attese
        if (expectedException != null) {
            try {
                // Passa null all'originalBufList solo se il test è per clone(null)
                ByteBufList.clone(originalBufList);
                fail("Prevista eccezione " + expectedException.getName() + " ma nessuna è stata lanciata.");
            } catch (Exception e) {
                assertEquals("Tipo di eccezione inatteso", expectedException, e.getClass());
            }
            return;
        }

        // --- Test dell'operazione di clonazione ---
        ByteBufList clonedList = ByteBufList.clone(originalBufList);

        assertNotNull("La ByteBufList clonata non dovrebbe essere null", clonedList);
        assertNotSame("La ByteBufList clonata dovrebbe essere una nuova istanza", originalBufList, clonedList);
        assertEquals("La dimensione della lista clonata dovrebbe corrispondere alla dimensione della lista originale",
                originalBufList.size(), clonedList.size());

        // Verifica che retainedDuplicate() sia stato chiamato su ogni buffer originale e
        // che la lista clonata contenga i duplicati restituiti.
        for (int i = 0; i < originalMocks.size(); i++) {
            ByteBuf originalMock = originalMocks.get(i);
            ByteBuf expectedDuplicateMock = duplicateMocks.get(i);
            ByteBuf actualClonedBuf = clonedList.getBuffer(i);

            // Verifica che retainedDuplicate sia stato chiamato sul buffer originale esattamente una volta
            verify(originalMock, times(1)).retainedDuplicate();

            // Verifica che il buffer nella lista clonata sia effettivamente il duplicato atteso
            assertSame("Il buffer all'indice " + i + " nella lista clonata non è il duplicato atteso",
                    expectedDuplicateMock, actualClonedBuf);
        }

        // Assicurati che non ci siano interazioni inaspettate con i mock originali (es. release premature)
        for (ByteBuf originalMock : originalMocks) {
            verify(originalMock, never()).release(); // clone() non dovrebbe rilasciare i buffer originali
            verify(originalMock, never()).release(anyInt());
            verify(originalMock, never()).duplicate(); // clone() dovrebbe usare retainedDuplicate, non duplicate
        }

        // Rilascia la lista clonata per evitare perdite di risorse nei test
        clonedList.release();
    }

    @Parameters
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();

        // Helper: params.add(new Object[] { initialNumBuffers, expectedExceptionClass });

        // --- Partizione 1: other è null ---
        // Usiamo -1 per initialNumBuffers per segnalare al setup che l'input 'other' sarà null.
        params.add(new Object[] {
                -1, // Segnala che ByteBufList.clone(null) dovrebbe essere chiamato
                NullPointerException.class
        });

        // --- Partizione 2: other è una ByteBufList vuota (buffers.size() == 0) ---
        params.add(new Object[] {
                0,  // initialNumBuffers: lista vuota
                null // Nessuna eccezione attesa
        });

        // --- Partizione 3: other contiene un singolo ByteBuf (buffers.size() == 1) ---
        params.add(new Object[] {
                1,  // initialNumBuffers: un singolo buffer
                null // Nessuna eccezione attesa
        });

        // --- Partizione 4: other contiene più ByteBuf (buffers.size() > 1) ---
        params.add(new Object[] {
                3,  // initialNumBuffers: più buffer
                null // Nessuna eccezione attesa
        });

        // Caso aggiuntivo: un numero maggiore di buffer
        params.add(new Object[] {
                10,
                null
        });

        return params;
    }
}
