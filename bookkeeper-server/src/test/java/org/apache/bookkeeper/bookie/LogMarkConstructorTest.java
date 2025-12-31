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
    private final String description;

    public LogMarkConstructorTest(Object input,
                                  Long expectedFileId,
                                  Long expectedOffset,
                                  String description) {
        this.input = input;
        this.expectedFileId = expectedFileId;
        this.expectedOffset = expectedOffset;
        this.description = description;
    }

    @Parameterized.Parameters
    public static Object[][] data() {
        LogMark sample = new LogMark(10L, 20L);
        LogMark extreme = new LogMark(Long.MAX_VALUE, Long.MIN_VALUE);

        return new Object[][] {
                // ---- Copy constructor ----
                {sample, 10L, 20L, "Copy constructor con valori normali"},
                {extreme, Long.MAX_VALUE, Long.MIN_VALUE, "Copy constructor con estremi long"},
                {new LogMark(-5L, -50L), -5L, -50L, "Copy constructor con valori negativi"},



                // ---- Costruttore con long,long ----
                {new long[]{10L, 20L}, 10L, 20L, "Costruttore con valori normali"},
                {new long[]{0L, 0L}, 0L, 0L, "Costruttore con zeri"},
                {new long[]{-5L, -50L}, -5L, -50L, "Costruttore con valori negativi"},
                { new long[]{Long.MAX_VALUE, Long.MAX_VALUE}, Long.MAX_VALUE, Long.MAX_VALUE, "Costruttore con estremi massimi" },
                { new long[]{Long.MIN_VALUE, Long.MIN_VALUE}, Long.MIN_VALUE, Long.MIN_VALUE, "Costruttore con estremi minimi" },
                { new long[]{Long.MAX_VALUE, Long.MIN_VALUE}, Long.MAX_VALUE, Long.MIN_VALUE, "Costruttore con valori misti" },
                { new long[]{1L, 1L}, 1L, 1L, "Costruttore con primi valori positivi" },



                // ---- Default constructor ----
                {null, 0L, 0L, "Default constructor -> valori 0"}
        };
    }

    @Test
    public void testConstructors() {
        LogMark mark;

        if (input instanceof LogMark) {
            mark = new LogMark((LogMark) input);  // Copy constructor
        } else if (input instanceof long[]) {
            long[] vals = (long[]) input;
            mark = new LogMark(vals[0], vals[1]); // Costruttore long,long
        } else {
            mark = new LogMark();                 // Default constructor
        }

        // Verifica valori attesi
        assertEquals("logFileId errato - " + description, expectedFileId.longValue(), mark.getLogFileId());
        assertEquals("logFileOffset errato - " + description, expectedOffset.longValue(), mark.getLogFileOffset());
    }


}
