package org.apache.bookkeeper.bookie;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class LogMarkConstructorTest {

    private final Object input;
    private final Long expectedFileId;
    private final Long expectedOffset;
    private final Class<? extends Throwable> expectedException;
    private final String description;

    public LogMarkConstructorTest(Object input,
                                  Long expectedFileId,
                                  Long expectedOffset,
                                  Class<? extends Throwable> expectedException,
                                  String description) {
        this.input = input;
        this.expectedFileId = expectedFileId;
        this.expectedOffset = expectedOffset;
        this.expectedException = expectedException;
        this.description = description;
    }

    @Parameterized.Parameters(name = "{index}: {4}")
    public static Object[][] data() {
        LogMark sample = new LogMark(10L, 20L);
        LogMark extreme = new LogMark(Long.MAX_VALUE, Long.MIN_VALUE);

        return new Object[][] {
                // ---- Costruttore con LogMark ----
                {sample, 10L, 20L, null, "Costruttore copy con valori normali"},
                {extreme, Long.MAX_VALUE, Long.MIN_VALUE, null, "Costruttore copy con estremi long"},
                {new LogMark(-5L, -50L), -5L, -50L, null, "Costruttore copy con valori negativi"},
                {null, null, null, AssertionError.class, "Costruttore copy con null -> eccezione"},

                // ---- Costruttore con long,long ----
                {new long[]{10L, 20L}, 10L, 20L, null, "Costruttore con valori normali"},
                {new long[]{0L, 0L}, 0L, 0L, null, "Costruttore con zeri"},
                {new long[]{-5L, -50L}, -5L, -50L, null, "Costruttore con negativi"},
                {new long[]{Long.MAX_VALUE, Long.MIN_VALUE}, Long.MAX_VALUE, Long.MIN_VALUE, null, "Costruttore con estremi long"}
        };
    }

    @Test
    public void testConstructors() {
        try {
            LogMark mark;
            if (input instanceof LogMark) {
                // Test del costruttore copy
                mark = new LogMark((LogMark) input);
            } else if (input instanceof long[]) {
                long[] vals = (long[]) input;
                mark = new LogMark(vals[0], vals[1]);
            } else {
                throw new AssertionError("Input non supportato: " + input);
            }

            if (expectedException != null) {
                throw new AssertionError("Attesa eccezione " + expectedException.getSimpleName() +
                        " ma il costruttore ha creato un LogMark. Caso: " + description);
            }

            // Verifica valori attesi
            assertEquals("logFileId errato - " + description, expectedFileId.longValue(), mark.getLogFileId());
            assertEquals("logFileOffset errato - " + description, expectedOffset.longValue(), mark.getLogFileOffset());

        } catch (Throwable t) {
            if (expectedException == null || !expectedException.isInstance(t)) {
                throw new AssertionError("Eccezione inattesa nel caso '" + description + "': " + t, t);
            }
        }
    }
}
