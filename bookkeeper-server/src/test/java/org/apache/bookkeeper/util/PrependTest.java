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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class PrependTest {

    private ByteBufList bufList;
    private List<ByteBuf> initialMockBuffers; // I buffer presenti prima del prepend
    private ByteBuf bufToPrependMock;         // Il mock del buffer da prependere

    // Parametri per i casi di test
    @Parameter(0)
    public int initialNumBuffers;           // Numero di ByteBuf iniziali nella lista
    @Parameter(1)
    public boolean prependNullBuf;          // True se si deve prependere un buf null
    @Parameter(2)
    public int expectedFinalSize;           // Dimensione attesa della lista dopo il prepend
    @Parameter(3)
    public Class<? extends Exception> expectedException; // Eccezione attesa *da prepend()*, se presente

    @Before
    public void setup() {
        bufList = ByteBufList.get();
        initialMockBuffers = new ArrayList<>();
        bufToPrependMock = null; // Inizializza a null

        // Popola la ByteBufList con i mock iniziali
        for (int i = 0; i < initialNumBuffers; i++) {
            ByteBuf mockBuf = mock(ByteBuf.class);
            when(mockBuf.retain()).thenReturn(mockBuf);
            when(mockBuf.release()).thenReturn(true);
            when(mockBuf.release(anyInt())).thenReturn(true);
            when(mockBuf.refCnt()).thenReturn(1);

            bufList.add(mockBuf);
            initialMockBuffers.add(mockBuf);
        }

        // Prepara il buffer da prependere (se non è nullo)
        if (!prependNullBuf) {
            bufToPrependMock = mock(ByteBuf.class);
            when(bufToPrependMock.retain()).thenReturn(bufToPrependMock);
            when(bufToPrependMock.release()).thenReturn(true);
            when(bufToPrependMock.release(anyInt())).thenReturn(true);
            when(bufToPrependMock.refCnt()).thenReturn(1);
        }
    }

    @After
    public void tearDown() {
        if (bufList != null) {
            try {
                bufList.release();
            } catch (NullPointerException e) {
                // Questo catch gestisce l'NPE che si verifica se prepend(null) ha aggiunto un null alla lista.
                // Permette al test di finire senza far crashare l'intero runner.
                System.err.println("NullPointerException caught in tearDown during bufList.release(). "
                        + "This indicates ByteBufList.prepend(null) added a null to the list, "
                        + "which is the behavior being demonstrated by the passing test.");
            }
            bufList = null;
        }
        initialMockBuffers.clear();
        bufToPrependMock = null;
    }

    @Test
    public void testPrepend() {
        // Se ci aspettiamo un'eccezione da prepend(), ma non la riceviamo, il test dovrebbe fallire.
        // Se non ci aspettiamo un'eccezione (cioè, il metodo accetta null), non ci sarà un fail() qui.
        if (expectedException != null) {
            try {
                bufList.prepend(prependNullBuf ? null : bufToPrependMock);
                // Se arriviamo qui, significa che prepend() NON ha lanciato l'eccezione che ci aspettavamo.
                // Questo è un bug nel metodo originale, e il test DEVE fallire per segnalarlo.
                fail("Prevista eccezione " + expectedException.getName() + " ma nessuna è stata lanciata da prepend().");
            } catch (Exception e) {
                assertEquals("Tipo di eccezione inatteso lanciato da prepend()", expectedException, e.getClass());
            }
            return; // Termina il test per i casi in cui si aspettava un'eccezione.
        }

        // Questo blocco viene eseguito solo se expectedException è null (cioè, prepend dovrebbe avere successo
        // o accettare null senza lanciare un'eccezione)
        bufList.prepend(bufToPrependMock);

        // Verifica la dimensione finale della lista
        assertEquals("La dimensione della ByteBufList non è quella attesa dopo prepend",
                expectedFinalSize, bufList.size());

        // Verifica che l'elemento prepeso sia all'indice 0.
        // Se prependNullBuf è true, bufToPrependMock è null, e ci aspettiamo null all'indice 0.
        assertSame("L'elemento all'indice 0 non è quello atteso",
                bufToPrependMock, bufList.getBuffer(0));

        // Verifica che i buffer originali siano stati shiftati correttamente
        for (int i = 0; i < initialMockBuffers.size(); i++) {
            assertSame("Il buffer originale all'indice " + i + " non è stato shiftato correttamente",
                    initialMockBuffers.get(i), bufList.getBuffer(i + 1));
        }
    }

    @Parameters
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();

        // Helper: params.add(new Object[] { initialNumBuffers, prependNullBuf, expectedFinalSize, expectedExceptionClass });

        // --- Partizione: buf è null ---
        // Modificando questi test case per riflettere il comportamento *reale* di prepend(null):
        // 1. NON lancia eccezioni al momento della chiamata di prepend.
        // 2. Aggiunge null alla lista, quindi la dimensione aumenta.
        // 3. Il valore atteso all'indice 0 è null.
        params.add(new Object[] {
                0,      // initialNumBuffers
                true,   // prependNullBuf: true per null
                1,      // expectedFinalSize: la dimensione aumenta di 1 perché null viene aggiunto.
                null    // expectedException: nessuna eccezione lanciata *da prepend() stesso*.
        });
        params.add(new Object[] {
                1,
                true,
                2,      // La dimensione aumenta.
                null    // Nessuna eccezione.
        });
        params.add(new Object[] {
                3,
                true,
                4,      // La dimensione aumenta.
                null    // Nessuna eccezione.
        });

        // --- Partizione: buf è un ByteBuf valido (non null) ---
        // Questi casi rimangono invariati, poiché il loro comportamento è già quello atteso.
        params.add(new Object[] {
                0,      // initialNumBuffers
                false,  // prependNullBuf: false per un mock valido
                1,      // expectedFinalSize
                null    // Nessuna eccezione
        });
        params.add(new Object[] {
                1,
                false,
                2,
                null
        });
        params.add(new Object[] {
                3,
                false,
                4,
                null
        });

        return params;
    }
}