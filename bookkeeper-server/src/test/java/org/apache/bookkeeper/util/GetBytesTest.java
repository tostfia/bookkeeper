package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled; // Per creare istanze di ByteBuf

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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail; // Per catturare eccezioni inaspettate

@RunWith(Parameterized.class)
public class GetBytesTest {

    private ByteBufList bufList; // Istanza di ByteBufList da testare

    // Parametri per i casi di test
    @Parameter(0)
    public byte[] dstInput; // L'array di destinazione per getBytes
    @Parameter(1)
    public List<byte[]> bufListContentData; // Dati per costruire il contenuto di ByteBufList
    @Parameter(2)
    public int expectedCopied; // Numero atteso di byte copiati
    @Parameter(3)
    public byte[] expectedDstContent; // Contenuto atteso dell'array dst dopo la copia
    @Parameter(4)
    public Class<? extends Exception> expectedException; // Tipo di eccezione attesa, se presente

    @Before
    public void setup() {
        // Ottiene un'istanza vuota di ByteBufList (potrebbe essere da un pool)
        bufList = ByteBufList.get();
        // Popola la ByteBufList con i dati specifici del caso di test
        if (bufListContentData != null) {
            for (byte[] data : bufListContentData) {
                if (data != null) {
                    // Crea un ByteBuf da un array di byte e lo aggiunge alla lista
                    // Assumiamo che ByteBufList abbia un metodo add(ByteBuf)
                    bufList.add(Unpooled.wrappedBuffer(data));
                }
            }
        }
    }

    @After
    public void tearDown() {
        // Rilascia la ByteBufList e i suoi buffer interni (importante per Netty)
        if (bufList != null) {
            bufList.release(); // Assumendo un metodo release() o close()
            bufList = null;
        }
    }

    @Test
    public void testGetBytes() {
        // Gestisce i casi in cui ci si aspetta un'eccezione
        if (expectedException != null) {
            try {
                bufList.getBytes(dstInput);
                fail("Prevista eccezione " + expectedException.getName() + " ma nessuna è stata lanciata.");
            } catch (Exception e) {
                assertEquals("Tipo di eccezione inatteso", expectedException, e.getClass());
            }
            return;
        }

        // Se dstInput è null, questo caso dovrebbe essere gestito da expectedException
        // Altrimenti, crea una copia modificabile dell'array di destinazione
        byte[] actualDst = dstInput == null ? null : Arrays.copyOf(dstInput, dstInput.length);

        if (actualDst != null) {
            int actualCopied = bufList.getBytes(actualDst);

            assertEquals("Numero errato di byte copiati", expectedCopied, actualCopied);
            assertArrayEquals("Il contenuto dell'array di destinazione non corrisponde", expectedDstContent, actualDst);
        } else {
            fail("dstInput era null ma non era prevista alcuna NullPointerException.");
        }
    }

    @Parameters
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();

        // Helper per la definizione dei casi di test:
        // params.add(new Object[] { dstInput, bufListContentData, expectedCopied, expectedDstContent, expectedExceptionClass });

        // --- Partizione P1: dst è null ---
        // Ci si aspetta una NullPointerException
        params.add(new Object[] {
                null,                                   // dstInput
                Arrays.asList(new byte[] { 1, 2, 3 }),  // bufListContent (il contenuto non importa se dst è null)
                -1,                                     // Valore fittizio, non verrà raggiunto
                null,                                   // Valore fittizio
                NullPointerException.class              // Eccezione attesa
        });

        // --- Partizione P2: dst è un array vuoto ---
        params.add(new Object[] {
                new byte[0],                            // dstInput
                Arrays.asList(new byte[] { 1, 2, 3 }),  // bufListContent
                0,                                      // Expected copied
                new byte[0],                            // Expected dst content (rimane vuoto)
                null                                    // Nessuna eccezione
        });

        // --- Partizione S1 & P3.3: ByteBufList vuota, dst con lunghezza > 0 ---
        params.add(new Object[] {
                new byte[5],                            // dstInput (inizialmente tutti zeri)
                Collections.emptyList(),                // bufListContent (ByteBufList vuota)
                0,                                      // Expected copied
                new byte[5],                            // Expected dst content (rimane tutto zeri)
                null
        });

        // --- Partizione S2.2 & P3.1: Singolo ByteBuf, dst.length < totalReadableBytes ---
        // bufList: [1,2,3,4,5]
        // dst: [_,_]
        // Risultato atteso: [1,2], copied=2
        params.add(new Object[] {
                new byte[2],
                Arrays.asList(new byte[] { 1, 2, 3, 4, 5 }),
                2,
                new byte[] { 1, 2 },
                null
        });

        // --- Partizione S2.2 & P3.2: Singolo ByteBuf, dst.length == totalReadableBytes ---
        // bufList: [1,2,3,4,5]
        // dst: [_,_,_,_,_]
        // Risultato atteso: [1,2,3,4,5], copied=5
        params.add(new Object[] {
                new byte[5],
                Arrays.asList(new byte[] { 1, 2, 3, 4, 5 }),
                5,
                new byte[] { 1, 2, 3, 4, 5 },
                null
        });

        // --- Partizione S2.2 & P3.3: Singolo ByteBuf, dst.length > totalReadableBytes ---
        // bufList: [1,2,3,4,5]
        // dst: [_,_,_,_,_,_,_]
        // Risultato atteso: [1,2,3,4,5,0,0], copied=5 (i byte rimanenti di dst rimangono a zero)
        params.add(new Object[] {
                new byte[7],
                Arrays.asList(new byte[] { 1, 2, 3, 4, 5 }),
                5,
                new byte[] { 1, 2, 3, 4, 5, 0, 0 },
                null
        });

        // --- Partizione S3.2 & P3.1: Multipli ByteBuf, dst.length < totalReadableBytes ---
        // bufList: [1,2], [3,4,5], [6,7] (totale 7 byte)
        // dst: [_,_,_,_]
        // Risultato atteso: [1,2,3,4], copied=4
        params.add(new Object[] {
                new byte[4],
                Arrays.asList(new byte[] { 1, 2 }, new byte[] { 3, 4, 5 }, new byte[] { 6, 7 }),
                4,
                new byte[] { 1, 2, 3, 4 },
                null
        });

        // --- Partizione S3.2 & P3.2: Multipli ByteBuf, dst.length == totalReadableBytes ---
        // bufList: [1,2], [3,4,5], [6,7] (totale 7 byte)
        // dst: [_,_,_,_,_,_,_]
        // Risultato atteso: [1,2,3,4,5,6,7], copied=7
        params.add(new Object[] {
                new byte[7],
                Arrays.asList(new byte[] { 1, 2 }, new byte[] { 3, 4, 5 }, new byte[] { 6, 7 }),
                7,
                new byte[] { 1, 2, 3, 4, 5, 6, 7 },
                null
        });

        // --- Partizione S3.2 & P3.3: Multipli ByteBuf, dst.length > totalReadableBytes ---
        // bufList: [1,2], [3,4,5], [6,7] (totale 7 byte)
        // dst: [_,_,_,_,_,_,_,_,_,_]
        // Risultato atteso: [1,2,3,4,5,6,7,0,0,0], copied=7
        params.add(new Object[] {
                new byte[10],
                Arrays.asList(new byte[] { 1, 2 }, new byte[] { 3, 4, 5 }, new byte[] { 6, 7 }),
                7,
                new byte[] { 1, 2, 3, 4, 5, 6, 7, 0, 0, 0 },
                null
        });

        // --- Partizione S3.1: Multipli ByteBuf, con alcuni buffer vuoti intermedi ---
        // bufList: [1,2], [], [3,4] (totale 4 byte leggibili)
        // dst: [_,_,_,_]
        // Risultato atteso: [1,2,3,4], copied=4
        params.add(new Object[] {
                new byte[4],
                Arrays.asList(new byte[] { 1, 2 }, new byte[0], new byte[] { 3, 4 }),
                4,
                new byte[] { 1, 2, 3, 4 },
                null
        });

        return params;
    }
}


