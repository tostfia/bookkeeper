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
import static org.junit.Assert.assertNull; // Per asserire se un ByteBuf null viene aggiunto
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class AddTest {

    private ByteBufList bufList;
    private List<ByteBuf> initialMockBuffers; // I buffer presenti prima dell'add
    private ByteBuf bufToAddMock;             // Il mock del buffer da aggiungere (potrebbe essere null)

    // Parametri per i casi di test
    @Parameter(0)
    public int initialNumBuffers;           // Numero di ByteBuf iniziali nella lista
    @Parameter(1)
    public boolean addNullBuf;              // True se si deve aggiungere un buf null
    @Parameter(2)
    public int expectedFinalSize;           // Dimensione attesa della lista dopo l'add
    // L'eccezione attesa da add(). Data la natura di ArrayList.add(), ci aspettiamo null qui per buf=null.
    @Parameter(3)
    public Class<? extends Exception> expectedException;

    @Before
    public void setup() {
        bufList = ByteBufList.get();
        initialMockBuffers = new ArrayList<>();
        bufToAddMock = null; // Inizializza a null

        // Popola la ByteBufList con i mock iniziali
        for (int i = 0; i < initialNumBuffers; i++) {
            ByteBuf mockBuf = mock(ByteBuf.class);
            // Configura i metodi retain/release per i mock per evitare IllegalReferenceCountException
            when(mockBuf.retain()).thenReturn(mockBuf);
            when(mockBuf.release()).thenReturn(true);
            when(mockBuf.release(anyInt())).thenReturn(true);
            when(mockBuf.refCnt()).thenReturn(1); // Assumiamo refCnt iniziale 1

            // Utilizziamo il metodo add della ByteBufList per popolare, assumendo che esista
            // e si comporti correttamente per la preparazione dei test.
            bufList.add(mockBuf);
            initialMockBuffers.add(mockBuf);
        }

        // Prepara il buffer da aggiungere (se non è nullo)
        if (!addNullBuf) {
            bufToAddMock = mock(ByteBuf.class);
            when(bufToAddMock.retain()).thenReturn(bufToAddMock);
            when(bufToAddMock.release()).thenReturn(true);
            when(bufToAddMock.release(anyInt())).thenReturn(true);
            when(bufToAddMock.refCnt()).thenReturn(1);
        }
    }

    @After
    public void tearDown() {
        initialMockBuffers.clear();
        bufToAddMock = null; // Rimuove il riferimento al mock
    }

    @Test
    public void testAdd() {
        // Gestisce i casi in cui ci si aspetta un'eccezione da add()
        if (expectedException != null) {
            try {
                bufList.add(addNullBuf ? null : bufToAddMock);
                // Se arriviamo qui, significa che add() NON ha lanciato l'eccezione che ci aspettavamo.
                // Questo indica un comportamento inatteso del metodo, quindi il test DEVE fallire.
                fail("Prevista eccezione " + expectedException.getName() + " ma nessuna è stata lanciata da add().");
            } catch (Exception e) {
                assertEquals("Tipo di eccezione inatteso lanciato da add()", expectedException, e.getClass());
            }
            return; // Termina il test per i casi in cui si aspettava un'eccezione.
        }

        // Questo blocco viene eseguito solo se expectedException è null (cioè, add() dovrebbe avere successo
        // o accettare null senza lanciare un'eccezione)
        bufList.add(bufToAddMock);

        // Verifica la dimensione finale della lista
        assertEquals("La dimensione della ByteBufList non è quella attesa dopo add",
                expectedFinalSize, bufList.size());

        // Verifica che il buffer aggiunto sia all'ultima posizione
        // L'indice dell'ultimo elemento è expectedFinalSize - 1
        if (addNullBuf) {
            assertNull("L'elemento aggiunto all'ultima posizione dovrebbe essere null", bufList.getBuffer(expectedFinalSize - 1));
        } else {
            assertSame("Il buffer aggiunto non è all'ultima posizione",
                    bufToAddMock, bufList.getBuffer(expectedFinalSize - 1));
        }

        // Verifica che i buffer originali siano rimasti nelle loro posizioni
        // Questo ciclo deve andare fino a initialMockBuffers.size() perché gli elementi sono all'inizio.
        for (int i = 0; i < initialMockBuffers.size(); i++) {
            assertSame("Il buffer originale all'indice " + i + " è stato modificato o spostato",
                    initialMockBuffers.get(i), bufList.getBuffer(i));
        }
    }

    @Parameters
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();

        // Helper: params.add(new Object[] { initialNumBuffers, addNullBuf, expectedFinalSize, expectedExceptionClass });

        // --- Partizione: buf è null ---
        params.add(new Object[] {
                0,      // initialNumBuffers (lista inizialmente vuota)
                true,   // addNullBuf: true per null
                1,      // expectedFinalSize: la dimensione aumenta di 1 perché null viene aggiunto.
                null    // expectedException: nessuna eccezione lanciata *da add() stesso*.
        });


        // --- Partizione: buf è un ByteBuf valido (non null) ---
        //  ByteBufList è inizialmente vuota (valore limite)
        params.add(new Object[] {
                0,      // initialNumBuffers
                false,  // addNullBuf: false per un mock valido
                1,      // expectedFinalSize
                null    // Nessuna eccezione
        });


        return params;
    }
}
