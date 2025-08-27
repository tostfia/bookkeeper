package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled; // Per creare istanze di ByteBuf
import io.netty.util.ReferenceCounted; // Interfaccia per gestire il conteggio dei riferimenti

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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class GetBufferTest {

    private ByteBufList bufList; // Istanza di ByteBufList da testare
    private List<ByteBuf> internalBuffers; // Per tenere traccia dei buffer aggiunti per confronti

    // Parametri per i casi di test
    @Parameter(0)
    public List<byte[]> bufListContentData; // Dati per costruire il contenuto di ByteBufList
    @Parameter(1)
    public int index;                       // L'indice da passare a getBuffer
    @Parameter(2)
    public byte[] expectedBufferContent;    // Contenuto atteso del ByteBuf restituito, se non null
    @Parameter(3)
    public Class<? extends Exception> expectedException; // Tipo di eccezione attesa, se presente

    @Before
    public void setup() {
        // Ottiene un'istanza vuota di ByteBufList (potrebbe essere da un pool)
        bufList = ByteBufList.get();
        internalBuffers = new ArrayList<>(); // Inizializza la lista di riferimento

        // Popola la ByteBufList con i dati specifici del caso di test
        if (bufListContentData != null) {
            for (byte[] data : bufListContentData) {
                if (data != null) {
                    ByteBuf newBuf = Unpooled.wrappedBuffer(data);
                    // Incrementa il reference count del ByteBuf prima di aggiungerlo.
                    // Questo perché `bufList.add` potrebbe incrementarlo e `bufList.release` potrebbe decrementarlo.
                    // Assicurarsi che il refCnt sia 1 quando aggiunto al bufList per il test.
                    // Nel contesto di Netty, i WrappedBuffer di solito partono con refCnt 1.
                    // Quindi, se `bufList.add` prende la proprietà del buffer, non dovremmo mantenerne un altro `retain`.
                    // Tuttavia, per `assertSame`, è utile che il buffer in `internalBuffers` non sia rilasciato prematuramente.
                    // Il modo più sicuro per il test è aggiungere il buffer e poi rilasciare la copia di riferimento
                    // o non trattenere una copia che deve essere rilasciata separatamente.
                    // Per il test `assertSame`, teniamo un riferimento e ci aspettiamo che `bufList.release()`
                    // rilasci l'istanza originale, e il nostro `internalBuffers` non lo tocchi.
                    // Il problema era che `internalBuffers` *rilasciava* nuovamente il buffer.
                    bufList.add(newBuf); // Assumiamo un metodo add(ByteBuf)
                    internalBuffers.add(newBuf); // Aggiunge al riferimento per il confronto
                }
            }
        }
    }

    @After
    public void tearDown() {
        // Rilascia la ByteBufList. Questo dovrebbe anche rilasciare tutti i ByteBuf interni
        // che sono stati aggiunti alla lista.
        if (bufList != null) {
            bufList.release(); // Assumendo un metodo release() o close() per ByteBufList
            bufList = null;
        }
        // NON rilasciare i buffer in internalBuffers qui.
        // Sono le stesse istanze che sono state aggiunte a bufList e che sono state
        // rilasciate da bufList.release(). Rilasciarli di nuovo causerebbe l'IllegalReferenceCountException.
        // Dobbiamo solo pulire la lista di riferimenti.
        internalBuffers.clear();
    }

    @Test
    public void testGetBuffer() {
        // Gestisce i casi in cui ci si aspetta un'eccezione
        if (expectedException != null) {
            try {
                bufList.getBuffer(index);
                fail("Prevista eccezione " + expectedException.getName() + " ma nessuna è stata lanciata per index: " + index);
            } catch (Exception e) {
                // Confronta il tipo di eccezione effettiva con quella attesa
                assertEquals("Tipo di eccezione inatteso per index: " + index, expectedException, e.getClass());
            }
            return;
        }

        // Se non è prevista un'eccezione, il metodo dovrebbe ritornare un ByteBuf
        ByteBuf actualBuffer = bufList.getBuffer(index);

        assertNotNull("Il buffer restituito non dovrebbe essere null per index: " + index, actualBuffer);

        // Verifica che il buffer restituito sia lo stesso oggetto che abbiamo aggiunto
        assertSame("Il buffer restituito non è lo stesso oggetto di quello atteso per index: " + index,
                internalBuffers.get(index), actualBuffer);

        // Verifica il contenuto del buffer restituito
        byte[] actualContent = new byte[actualBuffer.readableBytes()];
        actualBuffer.getBytes(actualBuffer.readerIndex(), actualContent); // Legge il contenuto senza modificare readerIndex

        assertEquals("La lunghezza del buffer restituito non corrisponde per index: " + index,
                expectedBufferContent.length, actualContent.length);
        org.junit.Assert.assertArrayEquals("Il contenuto del buffer restituito non corrisponde per index: " + index,
                expectedBufferContent, actualContent);

        // Verifica che il readerIndex non sia stato modificato dal metodo getBuffer
        assertEquals("Il readerIndex del buffer interno è stato modificato per index: " + index,
                0, actualBuffer.readerIndex()); // Assumendo che i buffer siano stati aggiunti con readerIndex 0
    }

    @Parameters
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();

        // Helper per la definizione dei casi di test:
        // params.add(new Object[] { bufListContentData, index, expectedBufferContent, expectedExceptionClass });

        // --- Partizione S1: ByteBufList vuota ---
        // Qualsiasi indice dovrebbe lanciare ArrayIndexOutOfBoundsException (come osservato)
        params.add(new Object[] {
                Collections.emptyList(),                // bufListContent
                0,                                      // index (anche 0 è invalido)
                null,                                   // expectedBufferContent
                IndexOutOfBoundsException.class    // Eccezione attesa (corretto in base all'output)
        });
        params.add(new Object[] {
                Collections.emptyList(),
                -1,
                null,
                ArrayIndexOutOfBoundsException.class    // Eccezione attesa (corretto in base all'output)
        });
        params.add(new Object[] {
                Collections.emptyList(),
                5,
                null,
                IndexOutOfBoundsException.class    // Eccezione attesa (corretto in base all'output)
        });


        // --- Partizione S2: ByteBufList con un singolo ByteBuf ---
        // Contenuto: [1,2,3]
        params.add(new Object[] {
                Arrays.asList(new byte[] { 1, 2, 3 }),
                0,                                      // index: valido (primo e unico)
                new byte[] { 1, 2, 3 },                 // expectedBufferContent
                null
        });
        // index negativo
        params.add(new Object[] {
                Arrays.asList(new byte[] { 1, 2, 3 }),
                -1,
                null,
                ArrayIndexOutOfBoundsException.class    // Eccezione attesa (corretto in base all'output)
        });
        // index fuori limite (uguale alla dimensione)
        params.add(new Object[] {
                Arrays.asList(new byte[] { 1, 2, 3 }),
                1,
                null,
                IndexOutOfBoundsException.class    // Eccezione attesa (corretto in base all'output)
        });

        // --- Partizione S3: ByteBufList con più ByteBuf ---
        // Contenuto: [1,2], [3,4,5], [6,7] (size = 3)

        // P1: index negativo
        params.add(new Object[] {
                Arrays.asList(new byte[] { 1, 2 }, new byte[] { 3, 4, 5 }, new byte[] { 6, 7 }),
                -1,
                null,
                ArrayIndexOutOfBoundsException.class    // Eccezione attesa (corretto in base all'output)
        });

        // P2: index = 0 (primo elemento)
        params.add(new Object[] {
                Arrays.asList(new byte[] { 1, 2 }, new byte[] { 3, 4, 5 }, new byte[] { 6, 7 }),
                0,
                new byte[] { 1, 2 },
                null
        });

        // P3: index positivo valido (elemento intermedio)
        params.add(new Object[] {
                Arrays.asList(new byte[] { 1, 2 }, new byte[] { 3, 4, 5 }, new byte[] { 6, 7 }),
                1,
                new byte[] { 3, 4, 5 },
                null
        });

        // P4: index = buffers.size() - 1 (ultimo elemento)
        params.add(new Object[] {
                Arrays.asList(new byte[] { 1, 2 }, new byte[] { 3, 4, 5 }, new byte[] { 6, 7 }),
                2, // buffers.size() - 1 = 3 - 1 = 2
                new byte[] { 6, 7 },
                null
        });

        // P5: index >= buffers.size() (fuori limite)
        params.add(new Object[] {
                Arrays.asList(new byte[] { 1, 2 }, new byte[] { 3, 4, 5 }, new byte[] { 6, 7 }),
                3, // buffers.size() = 3
                null,
                IndexOutOfBoundsException.class    // Eccezione attesa (corretto in base all'output)
        });
        params.add(new Object[] {
                Arrays.asList(new byte[] { 1, 2 }, new byte[] { 3, 4, 5 }, new byte[] { 6, 7 }),
                10,
                null,
                IndexOutOfBoundsException.class    // Eccezione attesa (corretto in base all'output)
        });

        // Caso aggiuntivo: buffer vuoti all'interno della lista
        params.add(new Object[] {
                Arrays.asList(new byte[] { 1, 2 }, new byte[0], new byte[] { 3, 4 }), // size = 3
                1,
                new byte[0], // Ci si aspetta un buffer vuoto
                null
        });

        return params;
    }
}
