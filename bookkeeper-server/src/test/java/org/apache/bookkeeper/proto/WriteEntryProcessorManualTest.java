package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.net.BookieId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class WriteEntryProcessorManualTest {

    enum TestMethod { CREATE, WRITE_COMPLETE }
    enum ExpectedResult { SUCCESS, FAILURE }

    @Parameterized.Parameter(0)
    public TestMethod method;

    @Parameterized.Parameter(1)
    public Object[] params;

    @Parameterized.Parameter(2)
    public ExpectedResult expected;

    private WriteEntryProcessor processor;

    @Mock private BookieRequestHandler requestHandler;
    @Mock private BookieRequestProcessor requestProcessor;
    @Mock private BookieProtocol.ParsedAddRequest request;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // In black-box possiamo solo creare un processor con parametri passati
        try {
            if (method == TestMethod.CREATE) {
                processor = WriteEntryProcessor.create(
                        (BookieProtocol.ParsedAddRequest) params[0],
                        (BookieRequestHandler) params[1],
                        (BookieRequestProcessor) params[2]
                );
            } else {
                processor = WriteEntryProcessor.create(
                        request,
                        requestHandler,
                        requestProcessor
                );
            }
        } catch (Exception e) {
            processor = null; // Fallimento previsto
        }
    }

    @After
    public void tearDown() {
        processor = null; // Black-box: non possiamo fare cleanup interno
    }

    @Parameterized.Parameters(name = "{index}: method={0}, expected={2}")
    public static Object[][] data() {
        return new Object[][]{
                // --- CREATE: tutti i casi senza conoscenza interna falliscono ---
                { TestMethod.CREATE, new Object[]{null, null, null}, ExpectedResult.FAILURE },
                { TestMethod.CREATE, new Object[]{mock(BookieProtocol.ParsedAddRequest.class), mock(BookieRequestHandler.class), mock(BookieRequestProcessor.class)}, ExpectedResult.FAILURE },
                {TestMethod.CREATE, new Object[]{null,mock(BookieProtocol.ParsedAddRequest.class),mock(BookieRequestHandler.class)},ExpectedResult.FAILURE},
                {TestMethod.CREATE, new Object[]{mock(BookieRequestHandler.class),null,mock(BookieRequestProcessor.class)},ExpectedResult.FAILURE},
                {TestMethod.CREATE, new Object[]{mock(BookieRequestProcessor.class),mock(BookieRequestHandler.class),null},ExpectedResult.FAILURE},


                // --- WRITE_COMPLETE: parametri generici → FAIL ---
                { TestMethod.WRITE_COMPLETE, new Object[]{0, 1L, 1L, mock(BookieId.class), new Object()}, ExpectedResult.FAILURE },
                { TestMethod.WRITE_COMPLETE, new Object[]{1, 1L, 1L, mock(BookieId.class), new Object()}, ExpectedResult.FAILURE },
                { TestMethod.WRITE_COMPLETE, new Object[]{0, -1L, -1L, null, null}, ExpectedResult.FAILURE },
                {TestMethod.WRITE_COMPLETE, new Object[]{0, 1L, 1L, null, null}, ExpectedResult.FAILURE },
                {TestMethod.WRITE_COMPLETE, new Object[]{-1, -1L, -1L, mock(BookieId.class),"test"}, ExpectedResult.FAILURE },
                {TestMethod.WRITE_COMPLETE, new Object[]{-1, 1L, 1L, mock(BookieId.class), "test"}, ExpectedResult.FAILURE },
                {TestMethod.WRITE_COMPLETE, new Object[]{1, -1L, -1L,mock(BookieId.class), 123}, ExpectedResult.FAILURE },
                {TestMethod.WRITE_COMPLETE, new Object[]{123, 1L, 1L, mock(BookieId.class), 123}, ExpectedResult.FAILURE },
                {TestMethod.WRITE_COMPLETE,new Object[]{Integer.MAX_VALUE, 1L, 1L}}

        };
    }

    @Test
    public void testCreate() {
        if (method != TestMethod.CREATE) return;

        try {
            WriteEntryProcessor.create(
                    (BookieProtocol.ParsedAddRequest) params[0],
                    (BookieRequestHandler) params[1],
                    (BookieRequestProcessor) params[2]
            );
            assertEquals(ExpectedResult.SUCCESS, expected);
        } catch (Exception e) {
            assertEquals(ExpectedResult.FAILURE, expected);
        }
    }

    @Test
    public void testWriteComplete() {
        if (method != TestMethod.WRITE_COMPLETE) return;

        try {
            processor.writeComplete(
                    (Integer) params[0],
                    (Long) params[1],
                    (Long) params[2],
                    (BookieId) params[3],
                    params[4]
            );
            assertEquals(ExpectedResult.SUCCESS, expected); // mai SUCCESS in black-box
        } catch (Exception e) {
            assertEquals(ExpectedResult.FAILURE, expected);
        }
    }
}

