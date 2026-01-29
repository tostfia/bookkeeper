package org.apache.bookkeeper.bookie;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class LogMarkConstructorTest {

    private enum CaseType {
        COPY,
        LONGS,
        DEFAULT,
        COPY_NULL
    }

    private final CaseType caseType;
    private final Object input;
    private final Long expectedFileId;
    private final Long expectedOffset;
    private final Class<? extends Throwable> expectedException;
    private final String description;

    public LogMarkConstructorTest(CaseType caseType,
                                  Object input,
                                  Long expectedFileId,
                                  Long expectedOffset,
                                  Class<? extends Throwable> expectedException,
                                  String description) {
        this.caseType = caseType;
        this.input = input;
        this.expectedFileId = expectedFileId;
        this.expectedOffset = expectedOffset;
        this.expectedException = expectedException;
        this.description = description;
    }

    @Parameterized.Parameters(name = "{index}: {5}")
    public static Collection<Object[]> data() {
        LogMark sample = new LogMark(10L, 20L);
        LogMark extreme = new LogMark(Long.MAX_VALUE, Long.MIN_VALUE);

        return Arrays.asList(new Object[][]{
                // ---- Copy constructor (valid) ----
                {CaseType.COPY, sample, 10L, 20L, null, "Copy constructor con valori normali"},
                {CaseType.COPY, extreme, Long.MAX_VALUE, Long.MAX_VALUE, null, "Copy constructor con estremi long"},
                //{CaseType.COPY, new LogMark(-5L, -50L), -5L, -50L, IllegalArgumentException.class, "Copy constructor con valori negativi"},

                // ---- Copy constructor (null -> NPE) ----
                {CaseType.COPY_NULL, null, null, null, NullPointerException.class, "Copy constructor con null -> NPE"},

                // ---- Costruttore con long,long (valid) ----
                {CaseType.LONGS, new long[]{10L, 20L}, 10L, 20L, null, "Costruttore con valori normali"},
                {CaseType.LONGS, new long[]{0L, 0L}, 0L, 0L, null, "Costruttore con zeri"},
                //{CaseType.LONGS, new long[]{-5L, -50L}, -5L, -50L, null, "Costruttore con valori negativi"},
                {CaseType.LONGS, new long[]{Long.MAX_VALUE, Long.MAX_VALUE}, Long.MAX_VALUE, Long.MAX_VALUE, null, "Costruttore con estremi massimi"},
                //{CaseType.LONGS, new long[]{Long.MIN_VALUE, Long.MIN_VALUE}, Long.MIN_VALUE, Long.MIN_VALUE, null, "Costruttore con estremi minimi"},
                {CaseType.LONGS, new long[]{1L, 1L}, 1L, 1L, null, "Costruttore con primi valori positivi"},


                // ---- Default constructor ----
                {CaseType.DEFAULT, null, 0L, 0L, null, "Default constructor -> valori 0"},


        });
    }

    @Test
    public void testConstructorsAndIO() {
        try {
            LogMark mark = null;

            switch (caseType) {
                case COPY:
                    mark = new LogMark((LogMark) input);
                    break;

                case LONGS:
                    long[] vals = (long[]) input;
                    mark = new LogMark(vals[0], vals[1]);
                    break;

                case DEFAULT:
                    mark = new LogMark();
                    break;

                case COPY_NULL:
                    // deve lanciare NullPointerException
                    new LogMark((LogMark) null);
                    break;


            }

            if (expectedException != null) {
                fail("Attesa eccezione: " + expectedException.getSimpleName() + " ma non è stata lanciata. Caso: " + description);
            }

            // Verifica valori attesi per i casi non-eccezione
            if (mark != null) {
                assertEquals("logFileId errato - " + description, expectedFileId.longValue(), mark.getLogFileId());
                assertEquals("logFileOffset errato - " + description, expectedOffset.longValue(), mark.getLogFileOffset());
            }

        } catch (Throwable t) {
            if (expectedException == null || !expectedException.isInstance(t)) {
                throw new AssertionError("Eccezione inattesa nel caso '" + description + "': " + t, t);
            }
            // eccezione attesa -> test passa
        }
    }
}