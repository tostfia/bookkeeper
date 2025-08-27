package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled; // Per creare istanze di ByteBuf reali

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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class CoalesceTest {

    private ByteBufList inputList; // L'istanza di ByteBufList da passare a coalesce
    private ByteBuf coalescedBuffer; // Il ByteBuf restituito dal metodo coalesce

    // Parametri per i casi di test
    @Parameter(0)
    public List<byte[]> bufferContents; // Contenuti per i ByteBuf all'interno di inputList. Usare null per inputList=null.
    @Parameter(1)
    public byte[] expectedCoalescedContent; // Contenuto atteso del ByteBuf risultante
    @Parameter(2)
    public Class<? extends Exception> expectedException; // Eccezione attesa, se presente

    @Before
    public void setup() {
        if (bufferContents == null) { // Caso: inputList è null
            inputList = null;
        } else {
            inputList = ByteBufList.get(); // Ottiene una vera istanza di ByteBufList
            for (byte[] content : bufferContents) {
                // Creiamo ByteBuf reali da Netty con i contenuti specificati.
                // retainedDuplicate() non è necessario qui, perché i buffer sono nuovi e il ciclo di vita
                // sarà gestito da inputList.release() in tearDown.
                ByteBuf buf = Unpooled.wrappedBuffer(content);
                inputList.add(buf); // Aggiunge il ByteBuf reale alla lista
            }
        }
        coalescedBuffer = null; // Inizializza a null
    }

    @After
    public void tearDown() {
        if (inputList != null) {
            inputList.release(); // Rilascia la lista inputList e tutti i suoi ByteBuf interni
            inputList = null;
        }
        if (coalescedBuffer != null) {
            coalescedBuffer.release(); // Rilascia anche il ByteBuf risultato di coalesce
            coalescedBuffer = null;
        }
    }

    @Test
    public void testCoalesce() {
        // Gestisce le eccezioni attese
        if (expectedException != null) {
            try {
                ByteBufList.coalesce(inputList);
                fail("Prevista eccezione " + expectedException.getName() + " ma nessuna è stata lanciata.");
            } catch (Exception e) {
                assertEquals("Tipo di eccezione inatteso", expectedException, e.getClass());
            }
            return;
        }

        // --- Test dell'operazione di coalescing ---
        coalescedBuffer = ByteBufList.coalesce(inputList);

        assertNotNull("Il ByteBuf coalesced non dovrebbe essere null", coalescedBuffer);

        // Verifica la quantità di byte leggibili nel buffer coalesced
        assertEquals("La quantità di byte leggibili nel buffer coalesced non corrisponde",
                expectedCoalescedContent.length, coalescedBuffer.readableBytes());

        // Verifica il contenuto del buffer coalesced
        byte[] actualCoalescedContent = new byte[coalescedBuffer.readableBytes()];
        coalescedBuffer.readBytes(actualCoalescedContent); // Legge il contenuto nel byte array

        assertArrayEquals("Il contenuto del buffer coalesced non corrisponde",
                expectedCoalescedContent, actualCoalescedContent);

        // Verifica che i readerIndex e writerIndex dei buffer originali non siano stati modificati
        if (inputList != null ) {
            for (int i = 0; i < bufferContents.size(); i++) {
                ByteBuf originalBuf = inputList.getBuffer(i);
                // I ByteBuf reali creati da Unpooled.wrappedBuffer(content) iniziano con readerIndex 0 e writerIndex = content.length
                assertEquals("ReaderIndex del buffer originale all'indice " + i + " è stato modificato",
                        0, originalBuf.readerIndex());
                assertEquals("WriterIndex del buffer originale all'indice " + i + " è stato modificato",
                        bufferContents.get(i).length, originalBuf.writerIndex());
            }
        }
    }

    @Parameters
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();

        // Helper: params.add(new Object[] { bufferContents, expectedCoalescedContent, expectedExceptionClass });

        // --- Partizione 1: list è null ---
        params.add(new Object[] {
                null,                     // bufferContents: null per indicare che inputList sarà null
                null,                     // expectedCoalescedContent: non rilevante per eccezione
                NullPointerException.class
        });

        // --- Partizione 2: list è una ByteBufList vuota (0 readable bytes) ---
        params.add(new Object[] {
                Collections.emptyList(),  // bufferContents: lista vuota
                new byte[0],              // expectedCoalescedContent: un array di byte vuoto
                null
        });
        // Caso: list contiene solo buffer vuoti
        params.add(new Object[] {
                Arrays.asList(new byte[0], new byte[0]),
                new byte[0],
                null
        });

        // --- Partizione 3: list contiene un singolo ByteBuf ---
        // 1. Singolo ByteBuf con contenuto
        params.add(new Object[] {
                Collections.singletonList(new byte[] { 1, 2, 3 }), // bufferContents
                new byte[] { 1, 2, 3 },                          // expectedCoalescedContent
                null
        });
        // 2. Singolo ByteBuf con più dati
        params.add(new Object[] {
                Collections.singletonList(new byte[] { 'h', 'e', 'l', 'l', 'o' }),
                new byte[] { 'h', 'e', 'l', 'l', 'o' },
                null
        });

        // --- Partizione 4: list contiene più ByteBuf ---
        // 1. Tutti i ByteBuf hanno contenuto
        params.add(new Object[] {
                Arrays.asList(new byte[] { 1, 2 }, new byte[] { 3, 4, 5 }, new byte[] { 6 }),
                new byte[] { 1, 2, 3, 4, 5, 6 },
                null
        });
        // 2. Alcuni ByteBuf sono vuoti (test intermedio)
        params.add(new Object[] {
                Arrays.asList(new byte[] { 'A', 'B' }, new byte[0], new byte[] { 'C', 'D' }),
                new byte[] { 'A', 'B', 'C', 'D' },
                null
        });
        // 3. Buffer con diversi tipi di dati
        params.add(new Object[] {
                Arrays.asList(new byte[] { (byte) 0xFF, 0x01 }, new byte[] { 0x7F, (byte) 0x80 }),
                new byte[] { (byte) 0xFF, 0x01, 0x7F, (byte) 0x80 },
                null
        });

        // Caso di test con una quantità maggiore di dati per verificare la gestione della capacità
        List<byte[]> largeContentList = new ArrayList<>();
        byte[] chunk = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 }; // 10 bytes
        for (int i = 0; i < 100; i++) { // 100 blocchi * 10 byte = 1000 byte totali
            largeContentList.add(chunk.clone()); // Aggiungi una copia per evitare modifiche accidentali
        }
        byte[] expectedLargeContent = new byte[1000];
        int offset = 0;
        for (int i = 0; i < 100; i++) {
            System.arraycopy(chunk, 0, expectedLargeContent, offset, chunk.length);
            offset += chunk.length;
        }
        params.add(new Object[] {
                largeContentList,
                expectedLargeContent,
                null
        });

        return params;
    }
}
