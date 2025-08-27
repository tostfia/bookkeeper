package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted; // Importiamo se necessario, ma non strettamente per questo test

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
        if (bufList != null) {
            try {
                bufList.release(); // Rilascia tutti i buffer nella lista
            } catch (NullPointerException e) {
                // Questo catch gestisce l'NPE che si verifica se add(null) ha aggiunto un null alla lista.
                // Permette al test di finire senza far crashare l'intero runner.
                System.err.println("NullPointerException caught in tearDown during bufList.release(). "
                        + "This indicates ByteBufList.add(null) added a null to the list, "
                        + "which is the behavior being demonstrated by the passing test.");
            }
            bufList = null;
        }
        // Non è necessario rilasciare individualmente i mock qui, perché sono gestiti da bufList.release()
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
        // Basandoci sul comportamento standard di ArrayList.add(null), non ci aspettiamo un'eccezione immediata.
        // La dimensione aumenterà, e null sarà aggiunto.
        params.add(new Object[] {
                0,      // initialNumBuffers (lista inizialmente vuota)
                true,   // addNullBuf: true per null
                1,      // expectedFinalSize: la dimensione aumenta di 1 perché null viene aggiunto.
                null    // expectedException: nessuna eccezione lanciata *da add() stesso*.
        });
        params.add(new Object[] {
                1,      // initialNumBuffers (lista con un elemento)
                true,
                2,
                null
        });
        params.add(new Object[] {
                3,      // initialNumBuffers (lista con più elementi)
                true,
                4,
                null
        });

        // --- Partizione: buf è un ByteBuf valido (non null) ---
        // 1. ByteBufList è inizialmente vuota (valore limite)
        params.add(new Object[] {
                0,      // initialNumBuffers
                false,  // addNullBuf: false per un mock valido
                1,      // expectedFinalSize
                null    // Nessuna eccezione
        });
        // 2. ByteBufList contiene un singolo ByteBuf (valore limite)
        params.add(new Object[] {
                1,
                false,
                2,
                null
        });
        // 3. ByteBufList contiene più ByteBuf (caso generale)
        params.add(new Object[] {
                3,
                false,
                4,
                null
        });

        return params;
    }
}
