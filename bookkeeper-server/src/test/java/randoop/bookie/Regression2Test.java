package randoop.bookie;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class Regression2Test {

    public static boolean debug = false;

    public void assertBooleanArrayEquals(boolean[] expectedArray, boolean[] actualArray) {
        if (expectedArray.length != actualArray.length) {
            throw new AssertionError("Array lengths differ: " + expectedArray.length + " != " + actualArray.length);
        }
        for (int i = 0; i < expectedArray.length; i++) {
            if (expectedArray[i] != actualArray[i]) {
                throw new AssertionError("Arrays differ at index " + i + ": " + expectedArray[i] + " != " + actualArray[i]);
            }
        }
    }

    @Test
    public void test0501() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0501");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark8.compare(logMark11);
        logMark8.setLogMark((long) (short) 10, 10L);
        long long16 = logMark8.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int23 = logMark19.compare(logMark22);
        logMark19.setLogMark((long) (short) 10, 10L);
        long long27 = logMark19.getLogFileId();
        int int28 = logMark8.compare(logMark19);
        java.lang.String str29 = logMark19.toString();
        int int30 = logMark5.compare(logMark19);
        long long31 = logMark19.getLogFileId();
        long long32 = logMark19.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark35 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark38 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int39 = logMark35.compare(logMark38);
        logMark35.setLogMark((long) (short) 10, 10L);
        long long43 = logMark35.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark49 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int50 = logMark46.compare(logMark49);
        logMark46.setLogMark((long) (short) 10, 10L);
        long long54 = logMark46.getLogFileId();
        int int55 = logMark35.compare(logMark46);
        long long56 = logMark35.getLogFileId();
        long long57 = logMark35.getLogFileOffset();
        long long58 = logMark35.getLogFileId();
        int int59 = logMark19.compare(logMark35);
        org.apache.bookkeeper.bookie.LogMark logMark60 = new org.apache.bookkeeper.bookie.LogMark(logMark19);
        org.apache.bookkeeper.bookie.LogMark logMark61 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long62 = logMark61.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark63 = new org.apache.bookkeeper.bookie.LogMark(logMark61);
        org.apache.bookkeeper.bookie.LogMark logMark64 = new org.apache.bookkeeper.bookie.LogMark(logMark61);
        java.lang.String str65 = logMark64.toString();
        logMark64.setLogMark(0L, (long) (byte) 0);
        logMark64.setLogMark((long) '4', (long) (byte) -1);
        long long72 = logMark64.getLogFileId();
        int int73 = logMark19.compare(logMark64);
        int int74 = logMark2.compare(logMark64);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 10L + "'", long16 == 10L);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 0 + "'", int23 == 0);
        org.junit.Assert.assertTrue("'" + long27 + "' != '" + 10L + "'", long27 == 10L);
        org.junit.Assert.assertTrue("'" + int28 + "' != '" + 0 + "'", int28 == 0);
        org.junit.Assert.assertEquals("'" + str29 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str29, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + (-1) + "'", int30 == (-1));
        org.junit.Assert.assertTrue("'" + long31 + "' != '" + 10L + "'", long31 == 10L);
        org.junit.Assert.assertTrue("'" + long32 + "' != '" + 10L + "'", long32 == 10L);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + 0 + "'", int39 == 0);
        org.junit.Assert.assertTrue("'" + long43 + "' != '" + 10L + "'", long43 == 10L);
        org.junit.Assert.assertTrue("'" + int50 + "' != '" + 0 + "'", int50 == 0);
        org.junit.Assert.assertTrue("'" + long54 + "' != '" + 10L + "'", long54 == 10L);
        org.junit.Assert.assertTrue("'" + int55 + "' != '" + 0 + "'", int55 == 0);
        org.junit.Assert.assertTrue("'" + long56 + "' != '" + 10L + "'", long56 == 10L);
        org.junit.Assert.assertTrue("'" + long57 + "' != '" + 10L + "'", long57 == 10L);
        org.junit.Assert.assertTrue("'" + long58 + "' != '" + 10L + "'", long58 == 10L);
        org.junit.Assert.assertTrue("'" + int59 + "' != '" + 0 + "'", int59 == 0);
        org.junit.Assert.assertNotNull(logMark61);
// flaky "1) test0501(Regression2Test)":         org.junit.Assert.assertTrue("'" + long62 + "' != '" + 32L + "'", long62 == 32L);
// flaky "1) test0501(Regression2Test)":         org.junit.Assert.assertEquals("'" + str65 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 32" + "'", str65, "LogMark: logFileId - 100 , logFileOffset - 32");
        org.junit.Assert.assertTrue("'" + long72 + "' != '" + 52L + "'", long72 == 52L);
        org.junit.Assert.assertTrue("'" + int73 + "' != '" + (-1) + "'", int73 == (-1));
        org.junit.Assert.assertTrue("'" + int74 + "' != '" + 1 + "'", int74 == 1);
    }

    @Test
    public void test0502() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0502");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        java.lang.String str11 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass13 = logMark12.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str11, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0503() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0503");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        logMark0.setLogMark((long) ' ', (long) ' ');
        long long6 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.Class<?> wildcardClass8 = logMark0.getClass();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "2) test0503(Regression2Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 32L + "'", long6 == 32L);
        org.junit.Assert.assertNotNull(wildcardClass8);
    }

    @Test
    public void test0504() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0504");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.lang.String str28 = logMark27.toString();
        logMark27.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark32 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        int int34 = logMark27.compare(logMark32);
        int int35 = logMark13.compare(logMark32);
        java.lang.String str36 = logMark32.toString();
        org.apache.bookkeeper.bookie.LogMark logMark37 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        long long38 = logMark37.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str36, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertTrue("'" + long38 + "' != '" + 32L + "'", long38 == 32L);
    }

    @Test
    public void test0505() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0505");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long6 = logMark5.getLogFileId();
        logMark5.setLogMark(35L, 10L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 97L + "'", long6 == 97L);
    }

    @Test
    public void test0506() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0506");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        logMark2.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test0507() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0507");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        java.lang.String str11 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int19 = logMark15.compare(logMark18);
        logMark15.setLogMark((long) (short) 10, 10L);
        long long23 = logMark15.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int30 = logMark26.compare(logMark29);
        logMark26.setLogMark((long) (short) 10, 10L);
        long long34 = logMark26.getLogFileId();
        int int35 = logMark15.compare(logMark26);
        java.lang.String str36 = logMark26.toString();
        int int37 = logMark2.compare(logMark26);
        long long38 = logMark26.getLogFileOffset();
        long long39 = logMark26.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str11, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 10L + "'", long34 == 10L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 0 + "'", int35 == 0);
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str36, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + 0 + "'", int37 == 0);
        org.junit.Assert.assertTrue("'" + long38 + "' != '" + 10L + "'", long38 == 10L);
        org.junit.Assert.assertTrue("'" + long39 + "' != '" + 10L + "'", long39 == 10L);
    }

    @Test
    public void test0508() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0508");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long8 = logMark2.getLogFileOffset();
        logMark2.setLogMark((long) (byte) 1, (long) (short) 10);
        long long12 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 1L + "'", long12 == 1L);
    }

    @Test
    public void test0509() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0509");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, (long) (short) 0);
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0510() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0510");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int14 = logMark10.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        int int16 = logMark6.compare(logMark10);
        long long17 = logMark10.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        java.lang.String str19 = logMark18.toString();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 0L + "'", long17 == 0L);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str19, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0511() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0511");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        long long7 = logMark5.getLogFileId();
        long long8 = logMark5.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
    }

    @Test
    public void test0512() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0512");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        logMark1.setLogMark(97L, (long) '4');
        logMark1.setLogMark((long) 'a', (long) 1);
        logMark1.setLogMark((long) (byte) -1, (long) '#');
        org.junit.Assert.assertNotNull(logMark0);
// flaky "3) test0512(Regression2Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
    }

    @Test
    public void test0513() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0513");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        java.lang.String str11 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int19 = logMark15.compare(logMark18);
        logMark15.setLogMark((long) (short) 10, 10L);
        long long23 = logMark15.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int30 = logMark26.compare(logMark29);
        logMark26.setLogMark((long) (short) 10, 10L);
        long long34 = logMark26.getLogFileId();
        int int35 = logMark15.compare(logMark26);
        java.lang.String str36 = logMark26.toString();
        int int37 = logMark2.compare(logMark26);
        long long38 = logMark26.getLogFileId();
        long long39 = logMark26.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer40 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark26.readLogMark(byteBuffer40);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str11, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 10L + "'", long34 == 10L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 0 + "'", int35 == 0);
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str36, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + 0 + "'", int37 == 0);
        org.junit.Assert.assertTrue("'" + long38 + "' != '" + 10L + "'", long38 == 10L);
        org.junit.Assert.assertTrue("'" + long39 + "' != '" + 10L + "'", long39 == 10L);
    }

    @Test
    public void test0514() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0514");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int10 = logMark3.compare(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        int int18 = logMark3.compare(logMark16);
        java.lang.String str19 = logMark16.toString();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + (-1) + "'", int18 == (-1));
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str19, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0515() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0515");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        logMark2.setLogMark((long) 0, (long) '#');
        java.lang.String str13 = logMark2.toString();
        long long14 = logMark2.getLogFileOffset();
        java.lang.Class<?> wildcardClass15 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
// flaky "4) test0515(Regression2Test)":         org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str13, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 35L + "'", long14 == 35L);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0516() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0516");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - -1" + "'", str4, "LogMark: logFileId - 52 , logFileOffset - -1");
    }

    @Test
    public void test0517() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0517");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test0518() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0518");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        java.lang.String str3 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str3, "LogMark: logFileId - 1 , logFileOffset - 35");
    }

    @Test
    public void test0519() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0519");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        logMark8.setLogMark(10L, 0L);
        logMark8.setLogMark((long) 100, (long) 'a');
        java.lang.String str17 = logMark8.toString();
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
// flaky "5) test0519(Regression2Test)":         org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
// flaky "2) test0519(Regression2Test)":         org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str10, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str17 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str17, "LogMark: logFileId - 100 , logFileOffset - 97");
    }

    @Test
    public void test0520() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0520");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 100, 35L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int9 = logMark5.compare(logMark8);
        logMark5.setLogMark((long) (short) 10, 10L);
        long long13 = logMark5.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int20 = logMark16.compare(logMark19);
        logMark16.setLogMark((long) (short) 10, 10L);
        long long24 = logMark16.getLogFileId();
        int int25 = logMark5.compare(logMark16);
        java.lang.String str26 = logMark16.toString();
        int int27 = logMark2.compare(logMark16);
        java.lang.String str28 = logMark2.toString();
        long long29 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 35" + "'", str28, "LogMark: logFileId - 100 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 35L + "'", long29 == 35L);
    }

    @Test
    public void test0521() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0521");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long4 = logMark0.getLogFileId();
        logMark0.setLogMark(52L, (long) 100);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.readLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "6) test0521(Regression2Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
// flaky "3) test0521(Regression2Test)":         org.junit.Assert.assertTrue("'" + long4 + "' != '" + 100L + "'", long4 == 100L);
    }

    @Test
    public void test0522() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0522");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), 10L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark12 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long13 = logMark12.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        java.lang.String str15 = logMark14.toString();
        int int16 = logMark11.compare(logMark14);
        int int17 = logMark6.compare(logMark14);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int24 = logMark20.compare(logMark23);
        logMark20.setLogMark((long) (short) 10, 10L);
        long long28 = logMark20.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark34 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int35 = logMark31.compare(logMark34);
        logMark31.setLogMark((long) (short) 10, 10L);
        long long39 = logMark31.getLogFileId();
        int int40 = logMark20.compare(logMark31);
        java.lang.String str41 = logMark31.toString();
        java.lang.String str42 = logMark31.toString();
        int int43 = logMark6.compare(logMark31);
        int int44 = logMark2.compare(logMark31);
        java.lang.String str45 = logMark31.toString();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 10L + "'", long3 == 10L);
        org.junit.Assert.assertNotNull(logMark12);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 100L + "'", long13 == 100L);
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str15, "LogMark: logFileId - 52 , logFileOffset - 100");
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + (-1) + "'", int17 == (-1));
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 0 + "'", int35 == 0);
        org.junit.Assert.assertTrue("'" + long39 + "' != '" + 10L + "'", long39 == 10L);
        org.junit.Assert.assertTrue("'" + int40 + "' != '" + 0 + "'", int40 == 0);
        org.junit.Assert.assertEquals("'" + str41 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str41, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str42 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str42, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int43 + "' != '" + 1 + "'", int43 == 1);
        org.junit.Assert.assertTrue("'" + int44 + "' != '" + (-1) + "'", int44 == (-1));
        org.junit.Assert.assertEquals("'" + str45 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str45, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test0523() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0523");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        java.lang.String str9 = logMark8.toString();
        logMark8.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark13 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        int int15 = logMark8.compare(logMark13);
        int int16 = logMark3.compare(logMark13);
        int int17 = logMark1.compare(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        java.lang.String str23 = logMark22.toString();
        logMark22.setLogMark((long) (-1), (long) (byte) 100);
        long long27 = logMark22.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark22);
        java.lang.String str29 = logMark22.toString();
        int int30 = logMark18.compare(logMark22);
        java.lang.Class<?> wildcardClass31 = logMark22.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertNotNull(logMark2);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str4, "LogMark: logFileId - 52 , logFileOffset - 100");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark13);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + (-1) + "'", int15 == (-1));
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 0 + "'", int16 == 0);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long27 + "' != '" + (-1L) + "'", long27 == (-1L));
        org.junit.Assert.assertEquals("'" + str29 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 100" + "'", str29, "LogMark: logFileId - -1 , logFileOffset - 100");
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 1 + "'", int30 == 1);
        org.junit.Assert.assertNotNull(wildcardClass31);
    }

    @Test
    public void test0524() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0524");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        logMark2.setLogMark((long) 0, (long) '#');
        long long13 = logMark2.getLogFileOffset();
        java.lang.String str14 = logMark2.toString();
        java.lang.String str15 = logMark2.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 35L + "'", long13 == 35L);
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str14, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str15, "LogMark: logFileId - 0 , logFileOffset - 35");
    }

    @Test
    public void test0525() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0525");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long7 = logMark6.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark6.writeLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - -1" + "'", str4, "LogMark: logFileId - 52 , logFileOffset - -1");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + (-1L) + "'", long7 == (-1L));
    }

    @Test
    public void test0526() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0526");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass4 = logMark3.getClass();
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test0527() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0527");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(35L, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark8.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        logMark13.setLogMark((long) (short) 100, 35L);
        logMark13.setLogMark(32L, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark23 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long24 = logMark23.getLogFileOffset();
        long long25 = logMark23.getLogFileOffset();
        int int26 = logMark22.compare(logMark23);
        logMark22.setLogMark((long) (byte) 1, (long) 'a');
        int int30 = logMark13.compare(logMark22);
        int int31 = logMark5.compare(logMark13);
        int int32 = logMark2.compare(logMark5);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
        org.junit.Assert.assertNotNull(logMark23);
// flaky "7) test0527(Regression2Test)":         org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
// flaky "4) test0527(Regression2Test)":         org.junit.Assert.assertTrue("'" + long25 + "' != '" + 10L + "'", long25 == 10L);
// flaky "1) test0527(Regression2Test)":         org.junit.Assert.assertTrue("'" + int26 + "' != '" + 1 + "'", int26 == 1);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 1 + "'", int30 == 1);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 1 + "'", int31 == 1);
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 1 + "'", int32 == 1);
    }

    @Test
    public void test0528() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0528");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark((long) 100, (long) 'a');
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long7 = logMark6.getLogFileOffset();
        int int8 = logMark0.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        java.lang.String str10 = logMark6.toString();
        long long11 = logMark6.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "8) test0528(Regression2Test)":         org.junit.Assert.assertTrue("'" + long2 + "' != '" + 10L + "'", long2 == 10L);
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str10, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
    }

    @Test
    public void test0529() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0529");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.Class<?> wildcardClass5 = logMark4.getClass();
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0530() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0530");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        java.lang.String str11 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int19 = logMark15.compare(logMark18);
        logMark15.setLogMark((long) (short) 10, 10L);
        long long23 = logMark15.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int30 = logMark26.compare(logMark29);
        logMark26.setLogMark((long) (short) 10, 10L);
        long long34 = logMark26.getLogFileId();
        int int35 = logMark15.compare(logMark26);
        java.lang.String str36 = logMark26.toString();
        int int37 = logMark2.compare(logMark26);
        java.lang.String str38 = logMark26.toString();
        java.nio.ByteBuffer byteBuffer39 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark26.writeLogMark(byteBuffer39);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str11, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 10L + "'", long34 == 10L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 0 + "'", int35 == 0);
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str36, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + 0 + "'", int37 == 0);
        org.junit.Assert.assertEquals("'" + str38 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str38, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test0531() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0531");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        logMark2.setLogMark((long) (short) 0, (long) '#');
        long long6 = logMark2.getLogFileOffset();
        java.lang.Class<?> wildcardClass7 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 35L + "'", long6 == 35L);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0532() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0532");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        java.lang.String str8 = logMark7.toString();
        logMark7.setLogMark((long) (-1), (long) (byte) 100);
        long long12 = logMark7.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int15 = logMark2.compare(logMark7);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str8, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + (-1L) + "'", long12 == (-1L));
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + 1 + "'", int15 == 1);
    }

    @Test
    public void test0533() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0533");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        java.lang.String str2 = logMark0.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        long long6 = logMark5.getLogFileId();
        int int7 = logMark0.compare(logMark5);
        long long8 = logMark5.getLogFileOffset();
        logMark5.setLogMark((long) '#', 52L);
        long long12 = logMark5.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 97L + "'", long8 == 97L);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 52L + "'", long12 == 52L);
    }

    @Test
    public void test0534() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0534");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), 10L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark12 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long13 = logMark12.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        java.lang.String str15 = logMark14.toString();
        int int16 = logMark11.compare(logMark14);
        int int17 = logMark6.compare(logMark14);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int24 = logMark20.compare(logMark23);
        logMark20.setLogMark((long) (short) 10, 10L);
        long long28 = logMark20.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark34 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int35 = logMark31.compare(logMark34);
        logMark31.setLogMark((long) (short) 10, 10L);
        long long39 = logMark31.getLogFileId();
        int int40 = logMark20.compare(logMark31);
        java.lang.String str41 = logMark31.toString();
        java.lang.String str42 = logMark31.toString();
        int int43 = logMark6.compare(logMark31);
        int int44 = logMark2.compare(logMark31);
        java.nio.ByteBuffer byteBuffer45 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer45);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 10L + "'", long3 == 10L);
        org.junit.Assert.assertNotNull(logMark12);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 97L + "'", long13 == 97L);
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str15, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + (-1) + "'", int17 == (-1));
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 0 + "'", int35 == 0);
        org.junit.Assert.assertTrue("'" + long39 + "' != '" + 10L + "'", long39 == 10L);
        org.junit.Assert.assertTrue("'" + int40 + "' != '" + 0 + "'", int40 == 0);
        org.junit.Assert.assertEquals("'" + str41 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str41, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str42 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str42, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int43 + "' != '" + 1 + "'", int43 == 1);
        org.junit.Assert.assertTrue("'" + int44 + "' != '" + (-1) + "'", int44 == (-1));
    }

    @Test
    public void test0535() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0535");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        long long6 = logMark5.getLogFileId();
        long long7 = logMark5.getLogFileId();
        int int8 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass10 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 0L + "'", long7 == 0L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 1 + "'", int8 == 1);
        org.junit.Assert.assertNotNull(wildcardClass10);
    }

    @Test
    public void test0536() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0536");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark8.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int13 = logMark2.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        java.nio.ByteBuffer byteBuffer15 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark12.writeLogMark(byteBuffer15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
    }

    @Test
    public void test0537() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0537");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(35L, 1L);
        long long3 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 1L + "'", long3 == 1L);
    }

    @Test
    public void test0538() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0538");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long5 = logMark4.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        java.lang.String str7 = logMark6.toString();
        int int8 = logMark3.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        long long13 = logMark11.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark17.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark17);
        int int22 = logMark11.compare(logMark21);
        int int23 = logMark3.compare(logMark11);
        java.nio.ByteBuffer byteBuffer24 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark11.readLogMark(byteBuffer24);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 97L + "'", long5 == 97L);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str7, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 0L + "'", long13 == 0L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 0 + "'", int23 == 0);
    }

    @Test
    public void test0539() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0539");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        java.nio.ByteBuffer byteBuffer17 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark15.writeLogMark(byteBuffer17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
    }

    @Test
    public void test0540() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0540");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        java.lang.String str5 = logMark2.toString();
        java.lang.String str6 = logMark2.toString();
        logMark2.setLogMark(35L, (long) (short) 100);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark10.setLogMark((long) (byte) 10, 35L);
        java.nio.ByteBuffer byteBuffer14 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark10.writeLogMark(byteBuffer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str5, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str6, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0541() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0541");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, (-1L));
        long long3 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 1L + "'", long3 == 1L);
    }

    @Test
    public void test0542() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0542");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), 0L);
        logMark2.setLogMark((long) (byte) -1, (long) 'a');
    }

    @Test
    public void test0543() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0543");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(10L, 0L);
        logMark2.setLogMark((long) (short) -1, (long) (short) 1);
    }

    @Test
    public void test0544() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0544");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        long long11 = logMark9.getLogFileId();
        int int12 = logMark3.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long14 = logMark3.getLogFileId();
        long long15 = logMark3.getLogFileId();
        logMark3.setLogMark(1L, (long) (short) -1);
        java.nio.ByteBuffer byteBuffer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 0L + "'", long14 == 0L);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 0L + "'", long15 == 0L);
    }

    @Test
    public void test0545() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0545");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        logMark2.setLogMark((long) 0, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass14 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass14);
    }

    @Test
    public void test0546() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0546");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long4 = logMark0.getLogFileId();
        logMark0.setLogMark(52L, (long) 100);
        long long8 = logMark0.getLogFileId();
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.writeLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 100L + "'", long4 == 100L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 52L + "'", long8 == 52L);
    }

    @Test
    public void test0547() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0547");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark8.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int13 = logMark2.compare(logMark12);
        long long14 = logMark12.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark((-1L), (long) '4');
        int int18 = logMark12.compare(logMark17);
        long long19 = logMark12.getLogFileOffset();
        long long20 = logMark12.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer21 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark12.readLogMark(byteBuffer21);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 1L + "'", long14 == 1L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 35L + "'", long19 == 35L);
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 35L + "'", long20 == 35L);
    }

    @Test
    public void test0548() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0548");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark8.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int13 = logMark2.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        long long15 = logMark14.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 35L + "'", long15 == 35L);
    }

    @Test
    public void test0549() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0549");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(9223372036854775807L, (-1L));
        java.lang.String str3 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 9223372036854775807 , logFileOffset - -1" + "'", str3, "LogMark: logFileId - 9223372036854775807 , logFileOffset - -1");
    }

    @Test
    public void test0550() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0550");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        logMark2.setLogMark((long) 0, (long) '#');
        java.lang.String str13 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark2.setLogMark((long) (byte) -1, 0L);
        long long18 = logMark2.getLogFileOffset();
        java.lang.Class<?> wildcardClass19 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str13, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 0L + "'", long18 == 0L);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0551() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0551");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        java.lang.String str9 = logMark8.toString();
        logMark8.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark13 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        int int15 = logMark8.compare(logMark13);
        int int16 = logMark3.compare(logMark13);
        int int17 = logMark1.compare(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int24 = logMark20.compare(logMark23);
        org.apache.bookkeeper.bookie.LogMark logMark25 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark25);
        int int27 = logMark20.compare(logMark26);
        logMark20.setLogMark((long) 0, (long) '#');
        java.lang.String str31 = logMark20.toString();
        int int32 = logMark3.compare(logMark20);
        org.apache.bookkeeper.bookie.LogMark logMark35 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark36 = new org.apache.bookkeeper.bookie.LogMark(logMark35);
        org.apache.bookkeeper.bookie.LogMark logMark39 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark42 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int43 = logMark39.compare(logMark42);
        long long44 = logMark42.getLogFileId();
        int int45 = logMark36.compare(logMark42);
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark(logMark36);
        long long47 = logMark36.getLogFileId();
        long long48 = logMark36.getLogFileId();
        int int49 = logMark20.compare(logMark36);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertNotNull(logMark2);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str4, "LogMark: logFileId - 52 , logFileOffset - 100");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark13);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + (-1) + "'", int15 == (-1));
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 0 + "'", int16 == 0);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertNotNull(logMark25);
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
        org.junit.Assert.assertEquals("'" + str31 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str31, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 1 + "'", int32 == 1);
        org.junit.Assert.assertTrue("'" + int43 + "' != '" + 0 + "'", int43 == 0);
        org.junit.Assert.assertTrue("'" + long44 + "' != '" + 97L + "'", long44 == 97L);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + (-1) + "'", int45 == (-1));
        org.junit.Assert.assertTrue("'" + long47 + "' != '" + 0L + "'", long47 == 0L);
        org.junit.Assert.assertTrue("'" + long48 + "' != '" + 0L + "'", long48 == 0L);
        org.junit.Assert.assertTrue("'" + int49 + "' != '" + 0 + "'", int49 == 0);
    }

    @Test
    public void test0552() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0552");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(10L, (long) (short) 0);
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0553() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0553");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', 0L);
        long long3 = logMark2.getLogFileId();
        java.lang.Class<?> wildcardClass4 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test0554() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0554");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        long long15 = logMark1.getLogFileId();
        logMark1.setLogMark((long) 100, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) (short) 10);
        int int22 = logMark1.compare(logMark21);
        java.lang.String str23 = logMark21.toString();
        java.lang.String str24 = logMark21.toString();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str2, "LogMark: logFileId - 52 , logFileOffset - 100");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 52L + "'", long15 == 52L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 10" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 10" + "'", str24, "LogMark: logFileId - 97 , logFileOffset - 10");
    }

    @Test
    public void test0555() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0555");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        logMark2.setLogMark((long) (-1), 9223372036854775807L);
        java.lang.String str6 = logMark2.toString();
        logMark2.setLogMark((long) (byte) -1, 35L);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807" + "'", str6, "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807");
    }

    @Test
    public void test0556() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0556");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        java.lang.String str3 = logMark2.toString();
        logMark2.setLogMark((long) 1, (long) (short) -1);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 0");
    }

    @Test
    public void test0557() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0557");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        long long11 = logMark9.getLogFileId();
        int int12 = logMark3.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long14 = logMark3.getLogFileId();
        long long15 = logMark3.getLogFileId();
        java.nio.ByteBuffer byteBuffer16 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 0L + "'", long14 == 0L);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 0L + "'", long15 == 0L);
    }

    @Test
    public void test0558() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0558");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        long long4 = logMark2.getLogFileId();
        java.lang.String str5 = logMark2.toString();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 100L + "'", long1 == 100L);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str3, "LogMark: logFileId - 52 , logFileOffset - 100");
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 52L + "'", long4 == 52L);
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str5, "LogMark: logFileId - 52 , logFileOffset - 100");
    }

    @Test
    public void test0559() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0559");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long6 = logMark5.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int13 = logMark9.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark14 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        int int16 = logMark9.compare(logMark15);
        int int17 = logMark5.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark18);
        long long20 = logMark19.getLogFileOffset();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertNotNull(logMark14);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 0L + "'", long20 == 0L);
    }

    @Test
    public void test0560() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0560");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str6 = logMark5.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str6, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0561() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0561");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        long long11 = logMark9.getLogFileId();
        int int12 = logMark3.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.String str14 = logMark13.toString();
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str14, "LogMark: logFileId - 0 , logFileOffset - 35");
    }

    @Test
    public void test0562() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0562");
        org.apache.bookkeeper.bookie.LogMark logMark0 = new org.apache.bookkeeper.bookie.LogMark();
        java.lang.String str1 = logMark0.toString();
        long long2 = logMark0.getLogFileId();
        logMark0.setLogMark((long) '4', (long) '#');
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str1, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 0L + "'", long2 == 0L);
    }

    @Test
    public void test0563() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0563");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 100, 52L);
        long long3 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 52L + "'", long3 == 52L);
    }

    @Test
    public void test0564() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0564");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int10 = logMark3.compare(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        int int18 = logMark3.compare(logMark16);
        long long19 = logMark3.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.String str21 = logMark20.toString();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
// flaky "9) test0564(Regression2Test)":         org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + (-1) + "'", int18 == (-1));
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + (-1L) + "'", long19 == (-1L));
        org.junit.Assert.assertEquals("'" + str21 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str21, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test0565() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0565");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, (long) (short) 100);
    }

    @Test
    public void test0566() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0566");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark7.setLogMark((long) 10, (long) '4');
        logMark7.setLogMark(10L, (long) 10);
        java.lang.String str14 = logMark7.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str14, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test0567() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0567");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        long long15 = logMark1.getLogFileId();
        logMark1.setLogMark((long) 100, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        java.lang.String str22 = logMark21.toString();
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark(logMark23);
        int int25 = logMark1.compare(logMark24);
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark24);
        java.lang.Class<?> wildcardClass27 = logMark24.getClass();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "10) test0567(Regression2Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
// flaky "5) test0567(Regression2Test)":         org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
// flaky "2) test0567(Regression2Test)":         org.junit.Assert.assertTrue("'" + long15 + "' != '" + (-1L) + "'", long15 == (-1L));
        org.junit.Assert.assertEquals("'" + str22 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str22, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 1 + "'", int25 == 1);
        org.junit.Assert.assertNotNull(wildcardClass27);
    }

    @Test
    public void test0568() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0568");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        java.lang.String str2 = logMark0.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        long long6 = logMark5.getLogFileId();
        int int7 = logMark0.compare(logMark5);
        long long8 = logMark5.getLogFileOffset();
        logMark5.setLogMark((long) '#', 52L);
        java.nio.ByteBuffer byteBuffer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "11) test0568(Regression2Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
// flaky "6) test0568(Regression2Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark3);
// flaky "3) test0568(Regression2Test)":         org.junit.Assert.assertTrue("'" + long6 + "' != '" + (-1L) + "'", long6 == (-1L));
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
// flaky "1) test0568(Regression2Test)":         org.junit.Assert.assertTrue("'" + long8 + "' != '" + 10L + "'", long8 == 10L);
    }

    @Test
    public void test0569() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0569");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int14 = logMark10.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark15 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        int int17 = logMark10.compare(logMark16);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int24 = logMark20.compare(logMark23);
        logMark20.setLogMark((long) (short) 10, 10L);
        long long28 = logMark20.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark34 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int35 = logMark31.compare(logMark34);
        logMark31.setLogMark((long) (short) 10, 10L);
        long long39 = logMark31.getLogFileId();
        int int40 = logMark20.compare(logMark31);
        java.lang.String str41 = logMark31.toString();
        java.lang.String str42 = logMark31.toString();
        logMark31.setLogMark((long) (-1), 100L);
        int int46 = logMark10.compare(logMark31);
        int int47 = logMark6.compare(logMark31);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertNotNull(logMark15);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 1 + "'", int17 == 1);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 0 + "'", int35 == 0);
        org.junit.Assert.assertTrue("'" + long39 + "' != '" + 10L + "'", long39 == 10L);
        org.junit.Assert.assertTrue("'" + int40 + "' != '" + 0 + "'", int40 == 0);
        org.junit.Assert.assertEquals("'" + str41 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str41, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str42 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str42, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int46 + "' != '" + 1 + "'", int46 == 1);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 1 + "'", int47 == 1);
    }

    @Test
    public void test0570() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0570");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long9 = logMark8.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        java.lang.String str11 = logMark10.toString();
        int int12 = logMark7.compare(logMark10);
        int int13 = logMark2.compare(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int20 = logMark16.compare(logMark19);
        logMark16.setLogMark((long) (short) 10, 10L);
        long long24 = logMark16.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark30 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int31 = logMark27.compare(logMark30);
        logMark27.setLogMark((long) (short) 10, 10L);
        long long35 = logMark27.getLogFileId();
        int int36 = logMark16.compare(logMark27);
        long long37 = logMark27.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark40 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark(logMark40);
        java.lang.String str42 = logMark41.toString();
        logMark41.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark46 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark47 = new org.apache.bookkeeper.bookie.LogMark(logMark46);
        int int48 = logMark41.compare(logMark46);
        int int49 = logMark27.compare(logMark46);
        org.apache.bookkeeper.bookie.LogMark logMark52 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark55 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int56 = logMark52.compare(logMark55);
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark(logMark52);
        org.apache.bookkeeper.bookie.LogMark logMark58 = new org.apache.bookkeeper.bookie.LogMark(logMark57);
        int int59 = logMark27.compare(logMark58);
        long long60 = logMark58.getLogFileId();
        int int61 = logMark2.compare(logMark58);
        java.nio.ByteBuffer byteBuffer62 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark58.writeLogMark(byteBuffer62);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark8);
// flaky "12) test0570(Regression2Test)":         org.junit.Assert.assertTrue("'" + long9 + "' != '" + 10L + "'", long9 == 10L);
// flaky "7) test0570(Regression2Test)":         org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str11, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 1 + "'", int12 == 1);
// flaky "4) test0570(Regression2Test)":         org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + int36 + "' != '" + 0 + "'", int36 == 0);
        org.junit.Assert.assertTrue("'" + long37 + "' != '" + 10L + "'", long37 == 10L);
        org.junit.Assert.assertEquals("'" + str42 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str42, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark46);
// flaky "2) test0570(Regression2Test)":         org.junit.Assert.assertTrue("'" + int48 + "' != '" + 0 + "'", int48 == 0);
// flaky "1) test0570(Regression2Test)":         org.junit.Assert.assertTrue("'" + int49 + "' != '" + 1 + "'", int49 == 1);
        org.junit.Assert.assertTrue("'" + int56 + "' != '" + 0 + "'", int56 == 0);
        org.junit.Assert.assertTrue("'" + int59 + "' != '" + (-1) + "'", int59 == (-1));
        org.junit.Assert.assertTrue("'" + long60 + "' != '" + 97L + "'", long60 == 97L);
        org.junit.Assert.assertTrue("'" + int61 + "' != '" + (-1) + "'", int61 == (-1));
    }

    @Test
    public void test0571() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0571");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long9 = logMark8.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        java.lang.String str11 = logMark10.toString();
        int int12 = logMark7.compare(logMark10);
        int int13 = logMark2.compare(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int20 = logMark16.compare(logMark19);
        logMark16.setLogMark((long) (short) 10, 10L);
        long long24 = logMark16.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark30 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int31 = logMark27.compare(logMark30);
        logMark27.setLogMark((long) (short) 10, 10L);
        long long35 = logMark27.getLogFileId();
        int int36 = logMark16.compare(logMark27);
        long long37 = logMark27.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark40 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark(logMark40);
        java.lang.String str42 = logMark41.toString();
        logMark41.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark46 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark47 = new org.apache.bookkeeper.bookie.LogMark(logMark46);
        int int48 = logMark41.compare(logMark46);
        int int49 = logMark27.compare(logMark46);
        org.apache.bookkeeper.bookie.LogMark logMark52 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark55 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int56 = logMark52.compare(logMark55);
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark(logMark52);
        org.apache.bookkeeper.bookie.LogMark logMark58 = new org.apache.bookkeeper.bookie.LogMark(logMark57);
        int int59 = logMark27.compare(logMark58);
        long long60 = logMark58.getLogFileId();
        int int61 = logMark2.compare(logMark58);
        long long62 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark63 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long64 = logMark2.getLogFileId();
        org.junit.Assert.assertNotNull(logMark8);
// flaky "13) test0571(Regression2Test)":         org.junit.Assert.assertTrue("'" + long9 + "' != '" + 10L + "'", long9 == 10L);
// flaky "8) test0571(Regression2Test)":         org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str11, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 1 + "'", int12 == 1);
// flaky "5) test0571(Regression2Test)":         org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + int36 + "' != '" + 0 + "'", int36 == 0);
        org.junit.Assert.assertTrue("'" + long37 + "' != '" + 10L + "'", long37 == 10L);
        org.junit.Assert.assertEquals("'" + str42 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str42, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark46);
// flaky "3) test0571(Regression2Test)":         org.junit.Assert.assertTrue("'" + int48 + "' != '" + 0 + "'", int48 == 0);
// flaky "2) test0571(Regression2Test)":         org.junit.Assert.assertTrue("'" + int49 + "' != '" + 1 + "'", int49 == 1);
        org.junit.Assert.assertTrue("'" + int56 + "' != '" + 0 + "'", int56 == 0);
        org.junit.Assert.assertTrue("'" + int59 + "' != '" + (-1) + "'", int59 == (-1));
        org.junit.Assert.assertTrue("'" + long60 + "' != '" + 97L + "'", long60 == 97L);
        org.junit.Assert.assertTrue("'" + int61 + "' != '" + (-1) + "'", int61 == (-1));
        org.junit.Assert.assertTrue("'" + long62 + "' != '" + (-1L) + "'", long62 == (-1L));
        org.junit.Assert.assertTrue("'" + long64 + "' != '" + 52L + "'", long64 == 52L);
    }

    @Test
    public void test0572() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0572");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0573() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0573");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark8.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int13 = logMark2.compare(logMark12);
        long long14 = logMark2.getLogFileOffset();
        long long15 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 0L + "'", long14 == 0L);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 97L + "'", long15 == 97L);
    }

    @Test
    public void test0574() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0574");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        java.lang.String str13 = logMark8.toString();
        long long14 = logMark8.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
// flaky "14) test0574(Regression2Test)":         org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str10, "LogMark: logFileId - -1 , logFileOffset - 10");
// flaky "9) test0574(Regression2Test)":         org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str13, "LogMark: logFileId - -1 , logFileOffset - 10");
// flaky "6) test0574(Regression2Test)":         org.junit.Assert.assertTrue("'" + long14 + "' != '" + (-1L) + "'", long14 == (-1L));
    }

    @Test
    public void test0575() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0575");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) (short) 10);
        logMark2.setLogMark((long) (byte) 1, (long) 10);
        java.lang.String str6 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 10" + "'", str6, "LogMark: logFileId - 1 , logFileOffset - 10");
    }

    @Test
    public void test0576() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0576");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        java.lang.String str6 = logMark5.toString();
        int int7 = logMark3.compare(logMark5);
        java.lang.String str8 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        long long13 = logMark11.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        int int16 = logMark11.compare(logMark15);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int23 = logMark19.compare(logMark22);
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark(logMark19);
        int int25 = logMark15.compare(logMark19);
        long long26 = logMark19.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark19);
        logMark19.setLogMark((long) 10, (long) (byte) -1);
        logMark19.setLogMark((long) ' ', (long) (byte) -1);
        long long34 = logMark19.getLogFileId();
        java.lang.String str35 = logMark19.toString();
        long long36 = logMark19.getLogFileOffset();
        int int37 = logMark3.compare(logMark19);
        logMark19.setLogMark((long) (byte) 10, (long) '4');
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 35L + "'", long4 == 35L);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str6, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str8, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 0L + "'", long13 == 0L);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 0 + "'", int23 == 0);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + (-1) + "'", int25 == (-1));
        org.junit.Assert.assertTrue("'" + long26 + "' != '" + 0L + "'", long26 == 0L);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 32L + "'", long34 == 32L);
        org.junit.Assert.assertEquals("'" + str35 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - -1" + "'", str35, "LogMark: logFileId - 32 , logFileOffset - -1");
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + (-1L) + "'", long36 == (-1L));
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + (-1) + "'", int37 == (-1));
    }

    @Test
    public void test0577() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0577");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        logMark8.setLogMark(10L, 0L);
        logMark8.setLogMark((long) 100, (long) 'a');
        java.lang.String str17 = logMark8.toString();
        java.lang.Class<?> wildcardClass18 = logMark8.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
// flaky "15) test0577(Regression2Test)":         org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str10, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str17 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str17, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(wildcardClass18);
    }

    @Test
    public void test0578() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0578");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long7 = logMark6.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark6.readLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "16) test0578(Regression2Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
// flaky "10) test0578(Regression2Test)":         org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str4, "LogMark: logFileId - -1 , logFileOffset - 10");
// flaky "7) test0578(Regression2Test)":         org.junit.Assert.assertTrue("'" + long7 + "' != '" + 10L + "'", long7 == 10L);
    }

    @Test
    public void test0579() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0579");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.lang.String str28 = logMark27.toString();
        logMark27.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark32 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        int int34 = logMark27.compare(logMark32);
        int int35 = logMark13.compare(logMark32);
        org.apache.bookkeeper.bookie.LogMark logMark38 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int42 = logMark38.compare(logMark41);
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark(logMark38);
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark(logMark43);
        int int45 = logMark13.compare(logMark44);
        long long46 = logMark13.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark49 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark50 = new org.apache.bookkeeper.bookie.LogMark(logMark49);
        java.lang.String str51 = logMark49.toString();
        int int52 = logMark13.compare(logMark49);
        long long53 = logMark49.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
// flaky "17) test0579(Regression2Test)":         org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
// flaky "11) test0579(Regression2Test)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + 1 + "'", int35 == 1);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + (-1) + "'", int45 == (-1));
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
        org.junit.Assert.assertEquals("'" + str51 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str51, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 1 + "'", int52 == 1);
        org.junit.Assert.assertTrue("'" + long53 + "' != '" + 0L + "'", long53 == 0L);
    }

    @Test
    public void test0580() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0580");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, 35L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
    }

    @Test
    public void test0581() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0581");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        long long15 = logMark1.getLogFileId();
        logMark1.setLogMark((long) 100, (long) '#');
        long long19 = logMark1.getLogFileOffset();
        logMark1.setLogMark((long) 0, 32L);
        java.lang.String str23 = logMark1.toString();
        long long24 = logMark1.getLogFileId();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "18) test0581(Regression2Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
// flaky "12) test0581(Regression2Test)":         org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
// flaky "8) test0581(Regression2Test)":         org.junit.Assert.assertTrue("'" + long15 + "' != '" + (-1L) + "'", long15 == (-1L));
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 35L + "'", long19 == 35L);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 32" + "'", str23, "LogMark: logFileId - 0 , logFileOffset - 32");
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 0L + "'", long24 == 0L);
    }

    @Test
    public void test0582() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0582");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long6 = logMark5.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int13 = logMark9.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark14 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        int int16 = logMark9.compare(logMark15);
        int int17 = logMark5.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        java.lang.String str23 = logMark22.toString();
        logMark22.setLogMark((long) (short) -1, 10L);
        int int27 = logMark18.compare(logMark22);
        long long28 = logMark22.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark(logMark22);
        java.lang.Class<?> wildcardClass30 = logMark29.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertNotNull(logMark14);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertNotNull(wildcardClass30);
    }

    @Test
    public void test0583() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0583");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        logMark2.setLogMark((long) (-1), 9223372036854775807L);
        java.lang.String str6 = logMark2.toString();
        logMark2.setLogMark(35L, (long) (byte) 10);
        logMark2.setLogMark((long) (byte) 10, (long) (byte) 0);
        logMark2.setLogMark((long) 'a', 1L);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807" + "'", str6, "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807");
    }

    @Test
    public void test0584() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0584");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int14 = logMark10.compare(logMark13);
        long long15 = logMark13.getLogFileId();
        int int16 = logMark7.compare(logMark13);
        java.lang.String str17 = logMark13.toString();
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark18);
        int int20 = logMark2.compare(logMark19);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 97L + "'", long15 == 97L);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertEquals("'" + str17 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str17, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + (-1) + "'", int20 == (-1));
    }

    @Test
    public void test0585() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0585");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        logMark2.setLogMark((long) 1, (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int19 = logMark15.compare(logMark18);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        int int21 = logMark2.compare(logMark15);
        java.lang.String str22 = logMark2.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + (-1) + "'", int21 == (-1));
        org.junit.Assert.assertEquals("'" + str22 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 0" + "'", str22, "LogMark: logFileId - 1 , logFileOffset - 0");
    }

    @Test
    public void test0586() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0586");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        long long11 = logMark9.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark15.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        int int20 = logMark9.compare(logMark19);
        long long21 = logMark19.getLogFileId();
        int int22 = logMark5.compare(logMark19);
        java.nio.ByteBuffer byteBuffer23 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.writeLogMark(byteBuffer23);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 0L + "'", long11 == 0L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 1 + "'", int20 == 1);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 1L + "'", long21 == 1L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
    }

    @Test
    public void test0587() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0587");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        long long11 = logMark9.getLogFileId();
        int int12 = logMark3.compare(logMark9);
        java.lang.String str13 = logMark9.toString();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        java.nio.ByteBuffer byteBuffer15 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark9.writeLogMark(byteBuffer15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str13, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0588() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0588");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        long long24 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(10L, 1L);
        int int28 = logMark13.compare(logMark27);
        java.nio.ByteBuffer byteBuffer29 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark27.readLogMark(byteBuffer29);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int28 + "' != '" + 1 + "'", int28 == 1);
    }

    @Test
    public void test0589() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0589");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 100, 52L);
        long long3 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
    }

    @Test
    public void test0590() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0590");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long6 = logMark5.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int13 = logMark9.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark14 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        int int16 = logMark9.compare(logMark15);
        int int17 = logMark5.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        java.lang.String str23 = logMark22.toString();
        logMark22.setLogMark((long) (short) -1, 10L);
        int int27 = logMark18.compare(logMark22);
        long long28 = logMark22.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark(logMark22);
        long long30 = logMark22.getLogFileOffset();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertNotNull(logMark14);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 10L + "'", long30 == 10L);
    }

    @Test
    public void test0591() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0591");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        long long3 = logMark2.getLogFileId();
        long long4 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
    }

    @Test
    public void test0592() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0592");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        java.lang.String str23 = logMark13.toString();
        java.lang.String str24 = logMark13.toString();
        org.apache.bookkeeper.bookie.LogMark logMark25 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark25);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        long long28 = logMark27.getLogFileId();
        int int29 = logMark13.compare(logMark27);
        long long30 = logMark27.getLogFileId();
        java.nio.ByteBuffer byteBuffer31 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark27.readLogMark(byteBuffer31);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str23, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str24, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark25);
// flaky "19) test0592(Regression2Test)":         org.junit.Assert.assertTrue("'" + long28 + "' != '" + (-1L) + "'", long28 == (-1L));
// flaky "13) test0592(Regression2Test)":         org.junit.Assert.assertTrue("'" + int29 + "' != '" + 1 + "'", int29 == 1);
// flaky "9) test0592(Regression2Test)":         org.junit.Assert.assertTrue("'" + long30 + "' != '" + (-1L) + "'", long30 == (-1L));
    }

    @Test
    public void test0593() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0593");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test0594() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0594");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        java.lang.String str9 = logMark8.toString();
        logMark8.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark13 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        int int15 = logMark8.compare(logMark13);
        int int16 = logMark3.compare(logMark13);
        int int17 = logMark1.compare(logMark3);
        java.lang.String str18 = logMark1.toString();
        java.nio.ByteBuffer byteBuffer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark1.writeLogMark(byteBuffer19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertNotNull(logMark2);
// flaky "20) test0594(Regression2Test)":         org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str4, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark13);
// flaky "14) test0594(Regression2Test)":         org.junit.Assert.assertTrue("'" + int15 + "' != '" + 0 + "'", int15 == 0);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 0 + "'", int16 == 0);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
// flaky "10) test0594(Regression2Test)":         org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str18, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test0595() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0595");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        java.lang.String str9 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        java.lang.String str14 = logMark12.toString();
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark17.setLogMark((long) (byte) 100, (long) (short) 0);
        int int21 = logMark12.compare(logMark17);
        int int22 = logMark2.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark25);
        java.lang.String str27 = logMark26.toString();
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        long long29 = logMark28.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark35 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int36 = logMark32.compare(logMark35);
        org.apache.bookkeeper.bookie.LogMark logMark37 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark38 = new org.apache.bookkeeper.bookie.LogMark(logMark37);
        int int39 = logMark32.compare(logMark38);
        int int40 = logMark28.compare(logMark32);
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark(logMark28);
        org.apache.bookkeeper.bookie.LogMark logMark42 = new org.apache.bookkeeper.bookie.LogMark(logMark41);
        int int43 = logMark12.compare(logMark42);
        java.lang.Class<?> wildcardClass44 = logMark12.getClass();
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str9, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str14, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + (-1) + "'", int21 == (-1));
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str27, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 0L + "'", long29 == 0L);
        org.junit.Assert.assertTrue("'" + int36 + "' != '" + 0 + "'", int36 == 0);
        org.junit.Assert.assertNotNull(logMark37);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + 1 + "'", int39 == 1);
        org.junit.Assert.assertTrue("'" + int40 + "' != '" + 0 + "'", int40 == 0);
        org.junit.Assert.assertTrue("'" + int43 + "' != '" + (-1) + "'", int43 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass44);
    }

    @Test
    public void test0596() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0596");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) -1, (long) (short) 100);
    }

    @Test
    public void test0597() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0597");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(35L, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int9 = logMark5.compare(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        logMark10.setLogMark((long) (short) 100, 35L);
        logMark10.setLogMark(32L, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark20 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long21 = logMark20.getLogFileOffset();
        long long22 = logMark20.getLogFileOffset();
        int int23 = logMark19.compare(logMark20);
        logMark19.setLogMark((long) (byte) 1, (long) 'a');
        int int27 = logMark10.compare(logMark19);
        int int28 = logMark2.compare(logMark10);
        long long29 = logMark10.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertNotNull(logMark20);
// flaky "21) test0597(Regression2Test)":         org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
// flaky "15) test0597(Regression2Test)":         org.junit.Assert.assertTrue("'" + long22 + "' != '" + 10L + "'", long22 == 10L);
// flaky "11) test0597(Regression2Test)":         org.junit.Assert.assertTrue("'" + int23 + "' != '" + 1 + "'", int23 == 1);
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
        org.junit.Assert.assertTrue("'" + int28 + "' != '" + 1 + "'", int28 == 1);
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 1L + "'", long29 == 1L);
    }

    @Test
    public void test0598() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0598");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        logMark2.setLogMark((long) (short) 0, (long) '#');
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0599() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0599");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(1L, 35L);
    }

    @Test
    public void test0600() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0600");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', 10L);
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0601() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0601");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark(100L, (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.String str8 = logMark7.toString();
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 52" + "'", str8, "LogMark: logFileId - 100 , logFileOffset - 52");
    }

    @Test
    public void test0602() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0602");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) 0, 9223372036854775807L);
        java.lang.String str8 = logMark3.toString();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 9223372036854775807" + "'", str8, "LogMark: logFileId - 0 , logFileOffset - 9223372036854775807");
    }

    @Test
    public void test0603() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0603");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0604() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0604");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.lang.String str28 = logMark27.toString();
        logMark27.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark32 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        int int34 = logMark27.compare(logMark32);
        int int35 = logMark13.compare(logMark32);
        org.apache.bookkeeper.bookie.LogMark logMark38 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int42 = logMark38.compare(logMark41);
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark(logMark38);
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark(logMark43);
        int int45 = logMark13.compare(logMark44);
        logMark44.setLogMark((long) (-1), (long) (byte) 1);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + (-1) + "'", int45 == (-1));
    }

    @Test
    public void test0605() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0605");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 9223372036854775807L);
        long long3 = logMark2.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 9223372036854775807L + "'", long3 == 9223372036854775807L);
    }

    @Test
    public void test0606() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0606");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark7.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        long long14 = logMark13.getLogFileId();
        logMark13.setLogMark(52L, 35L);
        logMark13.setLogMark((-1L), (long) (byte) 0);
        java.lang.String str21 = logMark13.toString();
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 97L + "'", long14 == 97L);
        org.junit.Assert.assertEquals("'" + str21 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str21, "LogMark: logFileId - -1 , logFileOffset - 0");
    }

    @Test
    public void test0607() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0607");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark(100L, (long) '4');
        long long7 = logMark3.getLogFileOffset();
        long long8 = logMark3.getLogFileId();
        java.lang.String str9 = logMark3.toString();
        java.lang.Class<?> wildcardClass10 = logMark3.getClass();
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 52L + "'", long7 == 52L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 100L + "'", long8 == 100L);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 52" + "'", str9, "LogMark: logFileId - 100 , logFileOffset - 52");
        org.junit.Assert.assertNotNull(wildcardClass10);
    }

    @Test
    public void test0608() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0608");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long9 = logMark8.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        java.lang.String str11 = logMark10.toString();
        int int12 = logMark7.compare(logMark10);
        int int13 = logMark2.compare(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int20 = logMark16.compare(logMark19);
        logMark16.setLogMark((long) (short) 10, 10L);
        long long24 = logMark16.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark30 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int31 = logMark27.compare(logMark30);
        logMark27.setLogMark((long) (short) 10, 10L);
        long long35 = logMark27.getLogFileId();
        int int36 = logMark16.compare(logMark27);
        java.lang.String str37 = logMark27.toString();
        java.lang.String str38 = logMark27.toString();
        int int39 = logMark2.compare(logMark27);
        long long40 = logMark2.getLogFileId();
        long long41 = logMark2.getLogFileId();
        org.junit.Assert.assertNotNull(logMark8);
// flaky "22) test0608(Regression2Test)":         org.junit.Assert.assertTrue("'" + long9 + "' != '" + 97L + "'", long9 == 97L);
// flaky "16) test0608(Regression2Test)":         org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str11, "LogMark: logFileId - 100 , logFileOffset - 97");
// flaky "12) test0608(Regression2Test)":         org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + int36 + "' != '" + 0 + "'", int36 == 0);
        org.junit.Assert.assertEquals("'" + str37 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str37, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str38 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str38, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + 1 + "'", int39 == 1);
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + 52L + "'", long40 == 52L);
        org.junit.Assert.assertTrue("'" + long41 + "' != '" + 52L + "'", long41 == 52L);
    }

    @Test
    public void test0609() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0609");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        long long4 = logMark2.getLogFileOffset();
        logMark2.setLogMark(52L, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long9 = logMark2.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "23) test0609(Regression2Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 32L + "'", long1 == 32L);
// flaky "17) test0609(Regression2Test)":         org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str3, "LogMark: logFileId - 32 , logFileOffset - 32");
// flaky "13) test0609(Regression2Test)":         org.junit.Assert.assertTrue("'" + long4 + "' != '" + 32L + "'", long4 == 32L);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 10L + "'", long9 == 10L);
    }

    @Test
    public void test0610() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0610");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        java.lang.String str9 = logMark8.toString();
        logMark8.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark13 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        int int15 = logMark8.compare(logMark13);
        int int16 = logMark3.compare(logMark13);
        int int17 = logMark1.compare(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int24 = logMark20.compare(logMark23);
        org.apache.bookkeeper.bookie.LogMark logMark25 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark25);
        int int27 = logMark20.compare(logMark26);
        logMark20.setLogMark((long) 0, (long) '#');
        java.lang.String str31 = logMark20.toString();
        int int32 = logMark3.compare(logMark20);
        java.nio.ByteBuffer byteBuffer33 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer33);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertNotNull(logMark2);
// flaky "24) test0610(Regression2Test)":         org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str4, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark13);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + (-1) + "'", int15 == (-1));
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 0 + "'", int16 == 0);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertNotNull(logMark25);
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
        org.junit.Assert.assertEquals("'" + str31 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str31, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 1 + "'", int32 == 1);
    }

    @Test
    public void test0611() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0611");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark2.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark6.setLogMark((long) '4', 9223372036854775807L);
    }

    @Test
    public void test0612() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0612");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long5 = logMark3.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 10L + "'", long5 == 10L);
    }

    @Test
    public void test0613() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0613");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(52L, (long) (short) 1);
    }

    @Test
    public void test0614() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0614");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        java.nio.ByteBuffer byteBuffer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark12.writeLogMark(byteBuffer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
// flaky "25) test0614(Regression2Test)":         org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str10, "LogMark: logFileId - 32 , logFileOffset - 32");
    }

    @Test
    public void test0615() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0615");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        long long5 = logMark1.getLogFileId();
        logMark1.setLogMark((long) ' ', (long) (byte) -1);
        org.junit.Assert.assertNotNull(logMark0);
// flaky "26) test0615(Regression2Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str2, "LogMark: logFileId - 32 , logFileOffset - 32");
// flaky "18) test0615(Regression2Test)":         org.junit.Assert.assertTrue("'" + long5 + "' != '" + 32L + "'", long5 == 32L);
    }

    @Test
    public void test0616() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0616");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        long long24 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark25 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark25);
        java.lang.String str27 = logMark26.toString();
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        org.apache.bookkeeper.bookie.LogMark logMark29 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long30 = logMark29.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark(logMark29);
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark(logMark29);
        int int33 = logMark28.compare(logMark32);
        int int34 = logMark13.compare(logMark32);
        long long35 = logMark13.getLogFileId();
        long long36 = logMark13.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertNotNull(logMark25);
// flaky "27) test0616(Regression2Test)":         org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str27, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertNotNull(logMark29);
// flaky "19) test0616(Regression2Test)":         org.junit.Assert.assertTrue("'" + long30 + "' != '" + 32L + "'", long30 == 32L);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 0 + "'", int33 == 0);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 10L + "'", long36 == 10L);
    }

    @Test
    public void test0617() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0617");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark7.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        logMark13.setLogMark((long) (short) 100, (-1L));
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
    }

    @Test
    public void test0618() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0618");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark18);
        java.lang.String str20 = logMark18.toString();
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark23.setLogMark((long) (byte) 100, (long) (short) 0);
        int int27 = logMark18.compare(logMark23);
        long long28 = logMark23.getLogFileId();
        int int29 = logMark11.compare(logMark23);
        java.lang.Class<?> wildcardClass30 = logMark11.getClass();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "28) test0618(Regression2Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str2, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertEquals("'" + str20 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str20, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 100L + "'", long28 == 100L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass30);
    }

    @Test
    public void test0619() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0619");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.lang.String str28 = logMark27.toString();
        logMark27.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark32 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        int int34 = logMark27.compare(logMark32);
        int int35 = logMark13.compare(logMark32);
        org.apache.bookkeeper.bookie.LogMark logMark36 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long37 = logMark36.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark38 = new org.apache.bookkeeper.bookie.LogMark(logMark36);
        org.apache.bookkeeper.bookie.LogMark logMark39 = new org.apache.bookkeeper.bookie.LogMark(logMark36);
        java.lang.String str40 = logMark39.toString();
        logMark39.setLogMark(0L, (long) (byte) 0);
        logMark39.setLogMark(100L, (long) 1);
        int int47 = logMark32.compare(logMark39);
        logMark39.setLogMark(32L, (long) (short) 100);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
        org.junit.Assert.assertNotNull(logMark36);
// flaky "29) test0619(Regression2Test)":         org.junit.Assert.assertTrue("'" + long37 + "' != '" + 32L + "'", long37 == 32L);
// flaky "20) test0619(Regression2Test)":         org.junit.Assert.assertEquals("'" + str40 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str40, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + (-1) + "'", int47 == (-1));
    }

    @Test
    public void test0620() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0620");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
    }

    @Test
    public void test0621() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0621");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) -1, (long) 0);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0622() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0622");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long6 = logMark5.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int13 = logMark9.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark14 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        int int16 = logMark9.compare(logMark15);
        int int17 = logMark5.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        java.lang.String str23 = logMark22.toString();
        logMark22.setLogMark((long) (short) -1, 10L);
        int int27 = logMark18.compare(logMark22);
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark18);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertNotNull(logMark14);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
    }

    @Test
    public void test0623() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0623");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark16 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark16);
        java.lang.String str18 = logMark17.toString();
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark17);
        org.apache.bookkeeper.bookie.LogMark logMark20 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long21 = logMark20.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark20);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark(logMark20);
        int int24 = logMark19.compare(logMark23);
        int int25 = logMark15.compare(logMark23);
        long long26 = logMark23.getLogFileId();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "30) test0623(Regression2Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str2, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertNotNull(logMark16);
// flaky "21) test0623(Regression2Test)":         org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str18, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertNotNull(logMark20);
// flaky "14) test0623(Regression2Test)":         org.junit.Assert.assertTrue("'" + long21 + "' != '" + 32L + "'", long21 == 32L);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
// flaky "4) test0623(Regression2Test)":         org.junit.Assert.assertTrue("'" + long26 + "' != '" + 32L + "'", long26 == 32L);
    }

    @Test
    public void test0624() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0624");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 1L);
    }

    @Test
    public void test0625() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0625");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.lang.String str28 = logMark27.toString();
        logMark27.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark32 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        int int34 = logMark27.compare(logMark32);
        int int35 = logMark13.compare(logMark32);
        logMark32.setLogMark((long) (byte) 0, (long) (-1));
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
    }

    @Test
    public void test0626() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0626");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark8.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int13 = logMark2.compare(logMark12);
        java.nio.ByteBuffer byteBuffer14 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
    }

    @Test
    public void test0627() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0627");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        java.lang.String str11 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int19 = logMark15.compare(logMark18);
        logMark15.setLogMark((long) (short) 10, 10L);
        long long23 = logMark15.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int30 = logMark26.compare(logMark29);
        logMark26.setLogMark((long) (short) 10, 10L);
        long long34 = logMark26.getLogFileId();
        int int35 = logMark15.compare(logMark26);
        java.lang.String str36 = logMark26.toString();
        int int37 = logMark2.compare(logMark26);
        long long38 = logMark26.getLogFileId();
        long long39 = logMark26.getLogFileId();
        java.nio.ByteBuffer byteBuffer40 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark26.readLogMark(byteBuffer40);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str11, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 10L + "'", long34 == 10L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 0 + "'", int35 == 0);
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str36, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + 0 + "'", int37 == 0);
        org.junit.Assert.assertTrue("'" + long38 + "' != '" + 10L + "'", long38 == 10L);
        org.junit.Assert.assertTrue("'" + long39 + "' != '" + 10L + "'", long39 == 10L);
    }

    @Test
    public void test0628() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0628");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.lang.String str28 = logMark27.toString();
        logMark27.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark32 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        int int34 = logMark27.compare(logMark32);
        int int35 = logMark13.compare(logMark32);
        org.apache.bookkeeper.bookie.LogMark logMark36 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long37 = logMark36.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark38 = new org.apache.bookkeeper.bookie.LogMark(logMark36);
        org.apache.bookkeeper.bookie.LogMark logMark39 = new org.apache.bookkeeper.bookie.LogMark(logMark36);
        java.lang.String str40 = logMark39.toString();
        logMark39.setLogMark(0L, (long) (byte) 0);
        logMark39.setLogMark(100L, (long) 1);
        int int47 = logMark32.compare(logMark39);
        java.nio.ByteBuffer byteBuffer48 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark32.readLogMark(byteBuffer48);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 1 + "'", int35 == 1);
        org.junit.Assert.assertNotNull(logMark36);
        org.junit.Assert.assertTrue("'" + long37 + "' != '" + (-1L) + "'", long37 == (-1L));
        org.junit.Assert.assertEquals("'" + str40 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - -1" + "'", str40, "LogMark: logFileId - 0 , logFileOffset - -1");
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + (-1) + "'", int47 == (-1));
    }

    @Test
    public void test0629() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0629");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        logMark1.setLogMark(97L, (long) '4');
        logMark1.setLogMark((long) 'a', (long) 1);
        logMark1.setLogMark((-1L), 1L);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - -1" + "'", str2, "LogMark: logFileId - 0 , logFileOffset - -1");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
    }

    @Test
    public void test0630() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0630");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        long long3 = logMark2.getLogFileOffset();
        long long4 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 97L + "'", long4 == 97L);
    }

    @Test
    public void test0631() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0631");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) (short) 10);
        logMark2.setLogMark((long) (byte) 1, (long) 10);
        long long6 = logMark2.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 10L + "'", long6 == 10L);
    }

    @Test
    public void test0632() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0632");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        logMark2.setLogMark((long) (short) 0, (long) '#');
        java.lang.String str6 = logMark2.toString();
        java.lang.Class<?> wildcardClass7 = logMark2.getClass();
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str6, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0633() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0633");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        long long15 = logMark1.getLogFileId();
        logMark1.setLogMark((long) 100, (long) '#');
        long long19 = logMark1.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark(logMark22);
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        int int28 = logMark22.compare(logMark27);
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int32 = logMark27.compare(logMark31);
        logMark31.setLogMark(97L, 97L);
        long long36 = logMark31.getLogFileOffset();
        logMark31.setLogMark((long) (short) 0, (long) (byte) 100);
        int int40 = logMark1.compare(logMark31);
        java.nio.ByteBuffer byteBuffer41 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark1.readLogMark(byteBuffer41);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - -1" + "'", str2, "LogMark: logFileId - 0 , logFileOffset - -1");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 0L + "'", long15 == 0L);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 35L + "'", long19 == 35L);
        org.junit.Assert.assertTrue("'" + int28 + "' != '" + (-1) + "'", int28 == (-1));
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 0 + "'", int32 == 0);
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 97L + "'", long36 == 97L);
        org.junit.Assert.assertTrue("'" + int40 + "' != '" + 1 + "'", int40 == 1);
    }

    @Test
    public void test0634() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0634");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        logMark2.setLogMark(32L, 9223372036854775807L);
        long long7 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 100, (long) (short) 1);
        int int11 = logMark2.compare(logMark10);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + (-1L) + "'", long1 == (-1L));
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - -1" + "'", str3, "LogMark: logFileId - 0 , logFileOffset - -1");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 9223372036854775807L + "'", long7 == 9223372036854775807L);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + (-1) + "'", int11 == (-1));
    }

    @Test
    public void test0635() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0635");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), 10L);
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark6);
        java.lang.Class<?> wildcardClass9 = logMark6.getClass();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + (-1L) + "'", long3 == (-1L));
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 1 + "'", int8 == 1);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0636() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0636");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.lang.String str28 = logMark27.toString();
        logMark27.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark32 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        int int34 = logMark27.compare(logMark32);
        int int35 = logMark13.compare(logMark32);
        org.apache.bookkeeper.bookie.LogMark logMark38 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int42 = logMark38.compare(logMark41);
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark(logMark38);
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark(logMark43);
        int int45 = logMark13.compare(logMark44);
        long long46 = logMark13.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark49 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark50 = new org.apache.bookkeeper.bookie.LogMark(logMark49);
        java.lang.String str51 = logMark49.toString();
        int int52 = logMark13.compare(logMark49);
        java.lang.String str53 = logMark49.toString();
        long long54 = logMark49.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark58 = new org.apache.bookkeeper.bookie.LogMark(logMark57);
        java.lang.String str59 = logMark58.toString();
        logMark58.setLogMark((long) (-1), (long) (byte) 100);
        org.apache.bookkeeper.bookie.LogMark logMark63 = new org.apache.bookkeeper.bookie.LogMark(logMark58);
        int int64 = logMark49.compare(logMark63);
        java.nio.ByteBuffer byteBuffer65 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark63.readLogMark(byteBuffer65);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 1 + "'", int35 == 1);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + (-1) + "'", int45 == (-1));
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
        org.junit.Assert.assertEquals("'" + str51 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str51, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 1 + "'", int52 == 1);
        org.junit.Assert.assertEquals("'" + str53 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str53, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long54 + "' != '" + 0L + "'", long54 == 0L);
        org.junit.Assert.assertEquals("'" + str59 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str59, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int64 + "' != '" + 1 + "'", int64 == 1);
    }

    @Test
    public void test0637() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0637");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        java.lang.String str11 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int19 = logMark15.compare(logMark18);
        logMark15.setLogMark((long) (short) 10, 10L);
        long long23 = logMark15.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int30 = logMark26.compare(logMark29);
        logMark26.setLogMark((long) (short) 10, 10L);
        long long34 = logMark26.getLogFileId();
        int int35 = logMark15.compare(logMark26);
        java.lang.String str36 = logMark26.toString();
        int int37 = logMark2.compare(logMark26);
        java.lang.String str38 = logMark26.toString();
        org.apache.bookkeeper.bookie.LogMark logMark39 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.lang.Class<?> wildcardClass40 = logMark26.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str11, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 10L + "'", long34 == 10L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 0 + "'", int35 == 0);
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str36, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + 0 + "'", int37 == 0);
        org.junit.Assert.assertEquals("'" + str38 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str38, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(wildcardClass40);
    }

    @Test
    public void test0638() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0638");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), 10L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 10L + "'", long3 == 10L);
    }

    @Test
    public void test0639() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0639");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0640() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0640");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark5.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
    }

    @Test
    public void test0641() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0641");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark8.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int13 = logMark2.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        java.nio.ByteBuffer byteBuffer15 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark14.readLogMark(byteBuffer15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
    }

    @Test
    public void test0642() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0642");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        long long15 = logMark1.getLogFileId();
        logMark1.setLogMark((long) 100, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        java.lang.String str22 = logMark21.toString();
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark(logMark23);
        int int25 = logMark1.compare(logMark24);
        java.nio.ByteBuffer byteBuffer26 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark24.readLogMark(byteBuffer26);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - -1" + "'", str2, "LogMark: logFileId - 0 , logFileOffset - -1");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 0L + "'", long15 == 0L);
        org.junit.Assert.assertEquals("'" + str22 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str22, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 1 + "'", int25 == 1);
    }

    @Test
    public void test0643() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0643");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        logMark6.setLogMark((long) (short) 10, 10L);
        long long14 = logMark6.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int21 = logMark17.compare(logMark20);
        logMark17.setLogMark((long) (short) 10, 10L);
        long long25 = logMark17.getLogFileId();
        int int26 = logMark6.compare(logMark17);
        java.lang.String str27 = logMark17.toString();
        java.lang.String str28 = logMark17.toString();
        logMark17.setLogMark(0L, (long) '#');
        int int32 = logMark2.compare(logMark17);
        long long33 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 10L + "'", long14 == 10L);
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + 0 + "'", int21 == 0);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 10L + "'", long25 == 10L);
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + 0 + "'", int26 == 0);
        org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str27, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str28, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 1 + "'", int32 == 1);
        org.junit.Assert.assertTrue("'" + long33 + "' != '" + 32L + "'", long33 == 32L);
    }

    @Test
    public void test0644() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0644");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), (long) (byte) 10);
    }

    @Test
    public void test0645() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0645");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        java.lang.String str8 = logMark5.toString();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        long long10 = logMark9.getLogFileId();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str8, "LogMark: logFileId - -1 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + (-1L) + "'", long10 == (-1L));
    }

    @Test
    public void test0646() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0646");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark2.getLogFileId();
        long long24 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str26 = logMark2.toString();
        logMark2.setLogMark((long) 1, 52L);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test0647() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0647");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        logMark8.setLogMark(10L, 1L);
        java.nio.ByteBuffer byteBuffer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.writeLogMark(byteBuffer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0648() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0648");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 100, 35L);
        long long3 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
    }

    @Test
    public void test0649() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0649");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int14 = logMark10.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        int int16 = logMark6.compare(logMark10);
        long long17 = logMark10.getLogFileId();
        java.lang.String str18 = logMark10.toString();
        logMark10.setLogMark(0L, (long) (short) -1);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 97L + "'", long17 == 97L);
        org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str18, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0650() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0650");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int9 = logMark5.compare(logMark8);
        logMark5.setLogMark((long) (short) 10, 10L);
        long long13 = logMark5.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int20 = logMark16.compare(logMark19);
        logMark16.setLogMark((long) (short) 10, 10L);
        long long24 = logMark16.getLogFileId();
        int int25 = logMark5.compare(logMark16);
        java.lang.String str26 = logMark16.toString();
        int int27 = logMark2.compare(logMark16);
        logMark16.setLogMark(32L, 0L);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
    }

    @Test
    public void test0651() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0651");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (short) 10);
    }

    @Test
    public void test0652() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0652");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark16 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark16);
        java.lang.String str18 = logMark17.toString();
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark17);
        org.apache.bookkeeper.bookie.LogMark logMark20 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long21 = logMark20.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark20);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark(logMark20);
        int int24 = logMark19.compare(logMark23);
        int int25 = logMark15.compare(logMark23);
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - -1" + "'", str2, "LogMark: logFileId - 0 , logFileOffset - -1");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertNotNull(logMark16);
        org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - -1" + "'", str18, "LogMark: logFileId - 0 , logFileOffset - -1");
        org.junit.Assert.assertNotNull(logMark20);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + (-1L) + "'", long21 == (-1L));
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
    }

    @Test
    public void test0653() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0653");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int18 = logMark14.compare(logMark17);
        java.lang.String str19 = logMark17.toString();
        int int20 = logMark2.compare(logMark17);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 0 + "'", int18 == 0);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str19, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + (-1) + "'", int20 == (-1));
    }

    @Test
    public void test0654() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0654");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long9 = logMark3.getLogFileId();
        java.lang.Class<?> wildcardClass10 = logMark3.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + (-1L) + "'", long9 == (-1L));
        org.junit.Assert.assertNotNull(wildcardClass10);
    }

    @Test
    public void test0655() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0655");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, 0L);
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long5 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 97L + "'", long3 == 97L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 0L + "'", long5 == 0L);
    }

    @Test
    public void test0656() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0656");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 0, (long) (byte) 0);
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0657() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0657");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int9 = logMark5.compare(logMark8);
        logMark5.setLogMark((long) (short) 10, 10L);
        long long13 = logMark5.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int20 = logMark16.compare(logMark19);
        logMark16.setLogMark((long) (short) 10, 10L);
        long long24 = logMark16.getLogFileId();
        int int25 = logMark5.compare(logMark16);
        java.lang.String str26 = logMark16.toString();
        int int27 = logMark2.compare(logMark16);
        long long28 = logMark16.getLogFileId();
        long long29 = logMark16.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark35 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int36 = logMark32.compare(logMark35);
        logMark32.setLogMark((long) (short) 10, 10L);
        long long40 = logMark32.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int47 = logMark43.compare(logMark46);
        logMark43.setLogMark((long) (short) 10, 10L);
        long long51 = logMark43.getLogFileId();
        int int52 = logMark32.compare(logMark43);
        long long53 = logMark32.getLogFileId();
        long long54 = logMark32.getLogFileOffset();
        long long55 = logMark32.getLogFileId();
        int int56 = logMark16.compare(logMark32);
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark(logMark16);
        org.apache.bookkeeper.bookie.LogMark logMark60 = new org.apache.bookkeeper.bookie.LogMark(10L, (long) 100);
        java.lang.String str61 = logMark60.toString();
        int int62 = logMark16.compare(logMark60);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 10L + "'", long29 == 10L);
        org.junit.Assert.assertTrue("'" + int36 + "' != '" + 0 + "'", int36 == 0);
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + 10L + "'", long40 == 10L);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long51 + "' != '" + 10L + "'", long51 == 10L);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 0 + "'", int52 == 0);
        org.junit.Assert.assertTrue("'" + long53 + "' != '" + 10L + "'", long53 == 10L);
        org.junit.Assert.assertTrue("'" + long54 + "' != '" + 10L + "'", long54 == 10L);
        org.junit.Assert.assertTrue("'" + long55 + "' != '" + 10L + "'", long55 == 10L);
        org.junit.Assert.assertTrue("'" + int56 + "' != '" + 0 + "'", int56 == 0);
        org.junit.Assert.assertEquals("'" + str61 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 100" + "'", str61, "LogMark: logFileId - 10 , logFileOffset - 100");
        org.junit.Assert.assertTrue("'" + int62 + "' != '" + (-1) + "'", int62 == (-1));
    }

    @Test
    public void test0658() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0658");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) (byte) 10);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileId();
        long long5 = logMark3.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 97L + "'", long4 == 97L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 10L + "'", long5 == 10L);
    }

    @Test
    public void test0659() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0659");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.lang.String str28 = logMark27.toString();
        logMark27.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark32 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        int int34 = logMark27.compare(logMark32);
        int int35 = logMark13.compare(logMark32);
        java.nio.ByteBuffer byteBuffer36 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark13.writeLogMark(byteBuffer36);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 1 + "'", int35 == 1);
    }

    @Test
    public void test0660() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0660");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) -1, (long) (short) 1);
        java.lang.String str3 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 1" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 1");
    }

    @Test
    public void test0661() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0661");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + (-1L) + "'", long1 == (-1L));
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - -1" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - -1");
    }

    @Test
    public void test0662() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0662");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 10, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
    }

    @Test
    public void test0663() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0663");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        logMark2.setLogMark(1L, (long) (byte) 1);
        logMark2.setLogMark((long) (byte) 0, 0L);
        logMark2.setLogMark((long) 10, (long) (byte) 10);
        java.lang.String str12 = logMark2.toString();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str12, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test0664() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0664");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileId();
        long long24 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
    }

    @Test
    public void test0665() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0665");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str4 = logMark3.toString();
        java.lang.String str5 = logMark3.toString();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "31) test0665(Regression2Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
// flaky "22) test0665(Regression2Test)":         org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 10" + "'", str4, "LogMark: logFileId - 35 , logFileOffset - 10");
// flaky "15) test0665(Regression2Test)":         org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 10" + "'", str5, "LogMark: logFileId - 35 , logFileOffset - 10");
    }

    @Test
    public void test0666() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0666");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.lang.String str28 = logMark27.toString();
        logMark27.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark32 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        int int34 = logMark27.compare(logMark32);
        int int35 = logMark13.compare(logMark32);
        org.apache.bookkeeper.bookie.LogMark logMark36 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        java.lang.String str37 = logMark32.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
// flaky "32) test0666(Regression2Test)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
// flaky "23) test0666(Regression2Test)":         org.junit.Assert.assertEquals("'" + str37 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 10" + "'", str37, "LogMark: logFileId - 35 , logFileOffset - 10");
    }

    @Test
    public void test0667() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0667");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        java.lang.String str3 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str3, "LogMark: logFileId - 1 , logFileOffset - 35");
    }

    @Test
    public void test0668() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0668");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        long long10 = logMark8.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
// flaky "33) test0668(Regression2Test)":         org.junit.Assert.assertTrue("'" + long10 + "' != '" + 35L + "'", long10 == 35L);
    }

    @Test
    public void test0669() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0669");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 100);
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0670() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0670");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        java.lang.String str9 = logMark8.toString();
        java.lang.String str10 = logMark8.toString();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int28 = logMark24.compare(logMark27);
        logMark24.setLogMark((long) (short) 10, 10L);
        long long32 = logMark24.getLogFileId();
        int int33 = logMark13.compare(logMark24);
        long long34 = logMark24.getLogFileOffset();
        long long35 = logMark24.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark38 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int42 = logMark38.compare(logMark41);
        logMark38.setLogMark((long) (short) 10, 10L);
        long long46 = logMark38.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark49 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark52 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int53 = logMark49.compare(logMark52);
        logMark49.setLogMark((long) (short) 10, 10L);
        long long57 = logMark49.getLogFileId();
        int int58 = logMark38.compare(logMark49);
        long long59 = logMark38.getLogFileId();
        long long60 = logMark38.getLogFileOffset();
        long long61 = logMark38.getLogFileId();
        int int62 = logMark24.compare(logMark38);
        org.apache.bookkeeper.bookie.LogMark logMark63 = new org.apache.bookkeeper.bookie.LogMark(logMark38);
        int int64 = logMark8.compare(logMark63);
        org.apache.bookkeeper.bookie.LogMark logMark65 = new org.apache.bookkeeper.bookie.LogMark(logMark63);
        long long66 = logMark63.getLogFileId();
        long long67 = logMark63.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str10, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int28 + "' != '" + 0 + "'", int28 == 0);
        org.junit.Assert.assertTrue("'" + long32 + "' != '" + 10L + "'", long32 == 10L);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 0 + "'", int33 == 0);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 10L + "'", long34 == 10L);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
        org.junit.Assert.assertTrue("'" + int53 + "' != '" + 0 + "'", int53 == 0);
        org.junit.Assert.assertTrue("'" + long57 + "' != '" + 10L + "'", long57 == 10L);
        org.junit.Assert.assertTrue("'" + int58 + "' != '" + 0 + "'", int58 == 0);
        org.junit.Assert.assertTrue("'" + long59 + "' != '" + 10L + "'", long59 == 10L);
        org.junit.Assert.assertTrue("'" + long60 + "' != '" + 10L + "'", long60 == 10L);
        org.junit.Assert.assertTrue("'" + long61 + "' != '" + 10L + "'", long61 == 10L);
        org.junit.Assert.assertTrue("'" + int62 + "' != '" + 0 + "'", int62 == 0);
        org.junit.Assert.assertTrue("'" + int64 + "' != '" + 1 + "'", int64 == 1);
        org.junit.Assert.assertTrue("'" + long66 + "' != '" + 10L + "'", long66 == 10L);
        org.junit.Assert.assertTrue("'" + long67 + "' != '" + 10L + "'", long67 == 10L);
    }

    @Test
    public void test0671() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0671");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        long long4 = logMark3.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "34) test0671(Regression2Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - 35 , logFileOffset - 10");
// flaky "24) test0671(Regression2Test)":         org.junit.Assert.assertTrue("'" + long4 + "' != '" + 10L + "'", long4 == 10L);
    }

    @Test
    public void test0672() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0672");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.lang.String str28 = logMark27.toString();
        logMark27.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark32 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        int int34 = logMark27.compare(logMark32);
        int int35 = logMark13.compare(logMark32);
        org.apache.bookkeeper.bookie.LogMark logMark38 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int42 = logMark38.compare(logMark41);
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark(logMark38);
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark(logMark43);
        int int45 = logMark13.compare(logMark44);
        java.nio.ByteBuffer byteBuffer46 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark44.writeLogMark(byteBuffer46);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
// flaky "35) test0672(Regression2Test)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + (-1) + "'", int45 == (-1));
    }

    @Test
    public void test0673() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0673");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long4 = logMark3.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark(0L, (long) (byte) 0);
        logMark6.setLogMark(100L, (long) 1);
        int int14 = logMark2.compare(logMark6);
        java.nio.ByteBuffer byteBuffer15 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark3);
// flaky "36) test0673(Regression2Test)":         org.junit.Assert.assertTrue("'" + long4 + "' != '" + 10L + "'", long4 == 10L);
// flaky "25) test0673(Regression2Test)":         org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 10" + "'", str7, "LogMark: logFileId - 35 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + (-1) + "'", int14 == (-1));
    }

    @Test
    public void test0674() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0674");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark7.compare(logMark11);
        logMark11.setLogMark(97L, 97L);
        long long16 = logMark11.getLogFileOffset();
        logMark11.setLogMark(10L, (-1L));
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 97L + "'", long16 == 97L);
    }

    @Test
    public void test0675() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0675");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark2.getLogFileId();
        long long24 = logMark2.getLogFileOffset();
        logMark2.setLogMark((long) '4', (long) (byte) 100);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
    }

    @Test
    public void test0676() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0676");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 0, (long) (byte) 0);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0677() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0677");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(10L, (long) 100);
        java.lang.String str3 = logMark2.toString();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 100" + "'", str3, "LogMark: logFileId - 10 , logFileOffset - 100");
    }

    @Test
    public void test0678() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0678");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        long long3 = logMark2.getLogFileOffset();
        long long4 = logMark2.getLogFileId();
        long long5 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 1L + "'", long4 == 1L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 35L + "'", long5 == 35L);
    }

    @Test
    public void test0679() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0679");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - -1" + "'", str4, "LogMark: logFileId - 52 , logFileOffset - -1");
    }

    @Test
    public void test0680() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0680");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        int int11 = logMark5.compare(logMark10);
        java.lang.String str12 = logMark5.toString();
        logMark5.setLogMark((long) 10, 35L);
        int int16 = logMark0.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str18 = logMark17.toString();
        java.lang.Class<?> wildcardClass19 = logMark17.getClass();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "37) test0680(Regression2Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 52L + "'", long1 == 52L);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + (-1) + "'", int11 == (-1));
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str12, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 35" + "'", str18, "LogMark: logFileId - 10 , logFileOffset - 35");
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0681() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0681");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 'a');
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0682() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0682");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int9 = logMark5.compare(logMark8);
        logMark5.setLogMark((long) (short) 10, 10L);
        long long13 = logMark5.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int20 = logMark16.compare(logMark19);
        logMark16.setLogMark((long) (short) 10, 10L);
        long long24 = logMark16.getLogFileId();
        int int25 = logMark5.compare(logMark16);
        java.lang.String str26 = logMark16.toString();
        int int27 = logMark2.compare(logMark16);
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark(logMark31);
        java.lang.String str33 = logMark32.toString();
        logMark32.setLogMark((long) 0, 9223372036854775807L);
        int int37 = logMark2.compare(logMark32);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertEquals("'" + str33 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str33, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + (-1) + "'", int37 == (-1));
    }

    @Test
    public void test0683() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0683");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        java.lang.String str2 = logMark0.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        long long6 = logMark5.getLogFileId();
        int int7 = logMark0.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long9 = logMark8.getLogFileOffset();
        int int10 = logMark0.compare(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.nio.ByteBuffer byteBuffer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.readLogMark(byteBuffer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "38) test0683(Regression2Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 52L + "'", long1 == 52L);
// flaky "26) test0683(Regression2Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 52" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 52");
        org.junit.Assert.assertNotNull(logMark3);
// flaky "16) test0683(Regression2Test)":         org.junit.Assert.assertTrue("'" + long6 + "' != '" + (-1L) + "'", long6 == (-1L));
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
        org.junit.Assert.assertNotNull(logMark8);
// flaky "5) test0683(Regression2Test)":         org.junit.Assert.assertTrue("'" + long9 + "' != '" + 52L + "'", long9 == 52L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
    }

    @Test
    public void test0684() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0684");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark5.setLogMark(1L, (long) (byte) 1);
        long long10 = logMark5.getLogFileId();
        long long11 = logMark5.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        logMark12.setLogMark(35L, (long) '4');
        java.lang.String str16 = logMark12.toString();
        java.lang.Class<?> wildcardClass17 = logMark12.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 1L + "'", long10 == 1L);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 1L + "'", long11 == 1L);
        org.junit.Assert.assertEquals("'" + str16 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 52" + "'", str16, "LogMark: logFileId - 35 , logFileOffset - 52");
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0685() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0685");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        java.lang.String str2 = logMark0.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        long long6 = logMark5.getLogFileId();
        int int7 = logMark0.compare(logMark5);
        logMark5.setLogMark((long) '4', 52L);
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "39) test0685(Regression2Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 52L + "'", long1 == 52L);
// flaky "27) test0685(Regression2Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 52" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 52");
        org.junit.Assert.assertNotNull(logMark3);
// flaky "17) test0685(Regression2Test)":         org.junit.Assert.assertTrue("'" + long6 + "' != '" + (-1L) + "'", long6 == (-1L));
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
    }

    @Test
    public void test0686() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0686");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long4 = logMark0.getLogFileOffset();
        java.lang.String str5 = logMark0.toString();
        logMark0.setLogMark((long) '#', 10L);
        long long9 = logMark0.getLogFileId();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "40) test0686(Regression2Test)":         org.junit.Assert.assertTrue("'" + long2 + "' != '" + 52L + "'", long2 == 52L);
// flaky "28) test0686(Regression2Test)":         org.junit.Assert.assertTrue("'" + long4 + "' != '" + 52L + "'", long4 == 52L);
// flaky "18) test0686(Regression2Test)":         org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 52" + "'", str5, "LogMark: logFileId - -1 , logFileOffset - 52");
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 35L + "'", long9 == 35L);
    }

    @Test
    public void test0687() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0687");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        logMark2.setLogMark((long) (-1), 9223372036854775807L);
        java.lang.String str6 = logMark2.toString();
        logMark2.setLogMark(35L, (long) (byte) 10);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807" + "'", str6, "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807");
    }

    @Test
    public void test0688() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0688");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        logMark2.setLogMark(32L, 9223372036854775807L);
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 10" + "'", str3, "LogMark: logFileId - 35 , logFileOffset - 10");
    }

    @Test
    public void test0689() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0689");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 0);
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        logMark6.setLogMark((long) (short) 10, 10L);
        long long14 = logMark6.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int21 = logMark17.compare(logMark20);
        logMark17.setLogMark((long) (short) 10, 10L);
        long long25 = logMark17.getLogFileId();
        int int26 = logMark6.compare(logMark17);
        java.lang.String str27 = logMark17.toString();
        java.lang.String str28 = logMark17.toString();
        logMark17.setLogMark(0L, (long) '#');
        int int32 = logMark2.compare(logMark17);
        logMark2.setLogMark((long) 10, (long) (short) -1);
        java.lang.String str36 = logMark2.toString();
        logMark2.setLogMark((long) (byte) 1, 32L);
        org.apache.bookkeeper.bookie.LogMark logMark40 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 10L + "'", long14 == 10L);
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + 0 + "'", int21 == 0);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 10L + "'", long25 == 10L);
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + 0 + "'", int26 == 0);
        org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str27, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str28, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 1 + "'", int32 == 1);
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - -1" + "'", str36, "LogMark: logFileId - 10 , logFileOffset - -1");
    }

    @Test
    public void test0690() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0690");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long5 = logMark4.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        int int8 = logMark3.compare(logMark7);
        logMark3.setLogMark((long) (short) 10, (long) 100);
        java.nio.ByteBuffer byteBuffer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - 35 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 10L + "'", long5 == 10L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
    }

    @Test
    public void test0691() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0691");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.Class<?> wildcardClass4 = logMark0.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test0692() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0692");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
    }

    @Test
    public void test0693() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0693");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        java.lang.String str11 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int19 = logMark15.compare(logMark18);
        logMark15.setLogMark((long) (short) 10, 10L);
        long long23 = logMark15.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int30 = logMark26.compare(logMark29);
        logMark26.setLogMark((long) (short) 10, 10L);
        long long34 = logMark26.getLogFileId();
        int int35 = logMark15.compare(logMark26);
        java.lang.String str36 = logMark26.toString();
        int int37 = logMark2.compare(logMark26);
        long long38 = logMark26.getLogFileId();
        long long39 = logMark26.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark42 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark45 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int46 = logMark42.compare(logMark45);
        org.apache.bookkeeper.bookie.LogMark logMark47 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark48 = new org.apache.bookkeeper.bookie.LogMark(logMark47);
        int int49 = logMark42.compare(logMark48);
        java.lang.String str50 = logMark48.toString();
        org.apache.bookkeeper.bookie.LogMark logMark51 = new org.apache.bookkeeper.bookie.LogMark(logMark48);
        org.apache.bookkeeper.bookie.LogMark logMark52 = new org.apache.bookkeeper.bookie.LogMark(logMark48);
        java.lang.String str53 = logMark48.toString();
        int int54 = logMark26.compare(logMark48);
        java.nio.ByteBuffer byteBuffer55 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark48.writeLogMark(byteBuffer55);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str11, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 10L + "'", long34 == 10L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 0 + "'", int35 == 0);
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str36, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + 0 + "'", int37 == 0);
        org.junit.Assert.assertTrue("'" + long38 + "' != '" + 10L + "'", long38 == 10L);
        org.junit.Assert.assertTrue("'" + long39 + "' != '" + 10L + "'", long39 == 10L);
        org.junit.Assert.assertTrue("'" + int46 + "' != '" + 0 + "'", int46 == 0);
        org.junit.Assert.assertNotNull(logMark47);
        org.junit.Assert.assertTrue("'" + int49 + "' != '" + 1 + "'", int49 == 1);
        org.junit.Assert.assertEquals("'" + str50 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 10" + "'", str50, "LogMark: logFileId - 35 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str53 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 10" + "'", str53, "LogMark: logFileId - 35 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int54 + "' != '" + (-1) + "'", int54 == (-1));
    }

    @Test
    public void test0694() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0694");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, (long) 1);
        logMark2.setLogMark(10L, 1L);
    }

    @Test
    public void test0695() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0695");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        long long15 = logMark1.getLogFileId();
        logMark1.setLogMark((long) 100, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        java.lang.String str22 = logMark21.toString();
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark(logMark23);
        int int25 = logMark1.compare(logMark24);
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark24);
        java.lang.Class<?> wildcardClass27 = logMark26.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - 35 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 35L + "'", long15 == 35L);
        org.junit.Assert.assertEquals("'" + str22 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str22, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 1 + "'", int25 == 1);
        org.junit.Assert.assertNotNull(wildcardClass27);
    }

    @Test
    public void test0696() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0696");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        long long24 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark25 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark25);
        java.lang.String str27 = logMark26.toString();
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        org.apache.bookkeeper.bookie.LogMark logMark29 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long30 = logMark29.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark(logMark29);
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark(logMark29);
        int int33 = logMark28.compare(logMark32);
        int int34 = logMark13.compare(logMark32);
        org.apache.bookkeeper.bookie.LogMark logMark35 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        long long36 = logMark32.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertNotNull(logMark25);
        org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 10" + "'", str27, "LogMark: logFileId - 35 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark29);
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 10L + "'", long30 == 10L);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 0 + "'", int33 == 0);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 35L + "'", long36 == 35L);
    }

    @Test
    public void test0697() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0697");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int10 = logMark3.compare(logMark8);
        long long11 = logMark8.getLogFileOffset();
        java.lang.Class<?> wildcardClass12 = logMark8.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 10L + "'", long11 == 10L);
        org.junit.Assert.assertNotNull(wildcardClass12);
    }

    @Test
    public void test0698() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0698");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark7.compare(logMark11);
        long long13 = logMark7.getLogFileOffset();
        java.lang.Class<?> wildcardClass14 = logMark7.getClass();
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 0L + "'", long13 == 0L);
        org.junit.Assert.assertNotNull(wildcardClass14);
    }

    @Test
    public void test0699() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0699");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        logMark8.setLogMark((long) 10, (long) (-1));
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) (short) 10);
        int int15 = logMark8.compare(logMark14);
        logMark8.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        int int23 = logMark8.compare(logMark21);
        long long24 = logMark21.getLogFileId();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + (-1) + "'", int15 == (-1));
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 1 + "'", int23 == 1);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 0L + "'", long24 == 0L);
    }

    @Test
    public void test0700() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0700");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.lang.String str28 = logMark27.toString();
        logMark27.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark32 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        int int34 = logMark27.compare(logMark32);
        int int35 = logMark13.compare(logMark32);
        java.lang.String str36 = logMark32.toString();
        org.apache.bookkeeper.bookie.LogMark logMark37 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        logMark32.setLogMark((long) (short) -1, (long) 10);
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int47 = logMark43.compare(logMark46);
        logMark46.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark51 = new org.apache.bookkeeper.bookie.LogMark();
        int int52 = logMark46.compare(logMark51);
        int int53 = logMark32.compare(logMark46);
        java.lang.Class<?> wildcardClass54 = logMark32.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 10" + "'", str36, "LogMark: logFileId - 35 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 1 + "'", int52 == 1);
        org.junit.Assert.assertTrue("'" + int53 + "' != '" + (-1) + "'", int53 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass54);
    }

    @Test
    public void test0701() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0701");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark7.compare(logMark11);
        long long13 = logMark7.getLogFileOffset();
        long long14 = logMark7.getLogFileId();
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 0L + "'", long13 == 0L);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 97L + "'", long14 == 97L);
    }

    @Test
    public void test0702() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0702");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int9 = logMark5.compare(logMark8);
        logMark5.setLogMark((long) (short) 10, 10L);
        long long13 = logMark5.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int20 = logMark16.compare(logMark19);
        logMark16.setLogMark((long) (short) 10, 10L);
        long long24 = logMark16.getLogFileId();
        int int25 = logMark5.compare(logMark16);
        java.lang.String str26 = logMark16.toString();
        int int27 = logMark2.compare(logMark16);
        long long28 = logMark16.getLogFileId();
        long long29 = logMark16.getLogFileId();
        logMark16.setLogMark(1L, (long) (short) -1);
        logMark16.setLogMark(1L, 10L);
        java.nio.ByteBuffer byteBuffer36 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark16.readLogMark(byteBuffer36);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 10L + "'", long29 == 10L);
    }

    @Test
    public void test0703() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0703");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int9 = logMark5.compare(logMark8);
        logMark5.setLogMark((long) (short) 10, 10L);
        long long13 = logMark5.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int20 = logMark16.compare(logMark19);
        logMark16.setLogMark((long) (short) 10, 10L);
        long long24 = logMark16.getLogFileId();
        int int25 = logMark5.compare(logMark16);
        java.lang.String str26 = logMark16.toString();
        int int27 = logMark2.compare(logMark16);
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long29 = logMark28.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer30 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark28.writeLogMark(byteBuffer30);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 0L + "'", long29 == 0L);
    }

    @Test
    public void test0704() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0704");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        java.lang.String str6 = logMark5.toString();
        int int7 = logMark3.compare(logMark5);
        logMark3.setLogMark((long) 100, 0L);
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 35L + "'", long4 == 35L);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str6, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
    }

    @Test
    public void test0705() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0705");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        java.lang.String str9 = logMark8.toString();
        logMark8.setLogMark((long) (-1), (long) (byte) 100);
        long long13 = logMark8.getLogFileId();
        logMark8.setLogMark((long) '4', (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int18 = logMark3.compare(logMark8);
        java.nio.ByteBuffer byteBuffer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + (-1L) + "'", long13 == (-1L));
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
    }

    @Test
    public void test0706() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0706");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(52L, (long) (byte) 100);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        long long7 = logMark5.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        long long9 = logMark8.getLogFileId();
        int int10 = logMark2.compare(logMark8);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 0L + "'", long7 == 0L);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 97L + "'", long9 == 97L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
    }

    @Test
    public void test0707() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0707");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long14 = logMark13.getLogFileOffset();
        int int15 = logMark10.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        long long17 = logMark16.getLogFileId();
        int int18 = logMark2.compare(logMark16);
        long long19 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 0L + "'", long14 == 0L);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + 1 + "'", int15 == 1);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + (-1L) + "'", long17 == (-1L));
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 97L + "'", long19 == 97L);
    }

    @Test
    public void test0708() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0708");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        long long17 = logMark16.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        logMark20.setLogMark((long) (-1), 9223372036854775807L);
        java.lang.String str24 = logMark20.toString();
        long long25 = logMark20.getLogFileId();
        int int26 = logMark16.compare(logMark20);
        java.nio.ByteBuffer byteBuffer27 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark16.writeLogMark(byteBuffer27);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + (-1L) + "'", long17 == (-1L));
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807" + "'", str24, "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807");
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + (-1L) + "'", long25 == (-1L));
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + (-1) + "'", int26 == (-1));
    }

    @Test
    public void test0709() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0709");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        long long4 = logMark2.getLogFileOffset();
        logMark2.setLogMark(52L, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark8.setLogMark((long) (short) 100, (long) (short) 100);
        long long12 = logMark8.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 10L + "'", long4 == 10L);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 100L + "'", long12 == 100L);
    }

    @Test
    public void test0710() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0710");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 100, (long) (short) 100);
    }

    @Test
    public void test0711() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0711");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long5 = logMark4.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        int int8 = logMark3.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long10 = logMark9.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 10L + "'", long5 == 10L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
    }

    @Test
    public void test0712() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0712");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.Class<?> wildcardClass7 = logMark6.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0713() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0713");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '4');
    }

    @Test
    public void test0714() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0714");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int9 = logMark5.compare(logMark8);
        logMark5.setLogMark((long) (short) 10, 10L);
        long long13 = logMark5.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int20 = logMark16.compare(logMark19);
        logMark16.setLogMark((long) (short) 10, 10L);
        long long24 = logMark16.getLogFileId();
        int int25 = logMark5.compare(logMark16);
        java.lang.String str26 = logMark16.toString();
        int int27 = logMark2.compare(logMark16);
        long long28 = logMark16.getLogFileId();
        long long29 = logMark16.getLogFileId();
        logMark16.setLogMark(1L, (long) (short) -1);
        logMark16.setLogMark(1L, 10L);
        logMark16.setLogMark((long) (short) -1, (long) (byte) 1);
        java.lang.String str39 = logMark16.toString();
        java.lang.String str40 = logMark16.toString();
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 10L + "'", long29 == 10L);
        org.junit.Assert.assertEquals("'" + str39 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 1" + "'", str39, "LogMark: logFileId - -1 , logFileOffset - 1");
        org.junit.Assert.assertEquals("'" + str40 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 1" + "'", str40, "LogMark: logFileId - -1 , logFileOffset - 1");
    }

    @Test
    public void test0715() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0715");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 10, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.String str5 = logMark4.toString();
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        java.lang.String str10 = logMark9.toString();
        logMark9.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark14 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        int int16 = logMark9.compare(logMark14);
        int int17 = logMark4.compare(logMark14);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        org.apache.bookkeeper.bookie.LogMark logMark19 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark19);
        java.lang.String str21 = logMark20.toString();
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark(logMark24);
        java.lang.String str26 = logMark25.toString();
        logMark25.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark30 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark(logMark30);
        int int32 = logMark25.compare(logMark30);
        int int33 = logMark20.compare(logMark30);
        int int34 = logMark18.compare(logMark30);
        org.apache.bookkeeper.bookie.LogMark logMark35 = new org.apache.bookkeeper.bookie.LogMark(logMark18);
        long long36 = logMark35.getLogFileOffset();
        int int37 = logMark2.compare(logMark35);
        java.lang.String str38 = logMark2.toString();
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str5, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str10, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark14);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 0 + "'", int16 == 0);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertNotNull(logMark19);
        org.junit.Assert.assertEquals("'" + str21 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str21, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str26, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark30);
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 0 + "'", int32 == 0);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 0 + "'", int33 == 0);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 10L + "'", long36 == 10L);
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + 1 + "'", int37 == 1);
        org.junit.Assert.assertEquals("'" + str38 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 1" + "'", str38, "LogMark: logFileId - 10 , logFileOffset - 1");
    }

    @Test
    public void test0716() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0716");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass10 = logMark9.getClass();
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass10);
    }

    @Test
    public void test0717() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0717");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, (long) 100);
        long long3 = logMark2.getLogFileOffset();
        long long4 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + (-1L) + "'", long4 == (-1L));
    }

    @Test
    public void test0718() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0718");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long5 = logMark4.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        java.lang.String str7 = logMark6.toString();
        int int8 = logMark3.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        long long13 = logMark11.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark17.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark17);
        int int22 = logMark11.compare(logMark21);
        int int23 = logMark3.compare(logMark11);
        long long24 = logMark11.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer25 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark11.writeLogMark(byteBuffer25);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 10L + "'", long5 == 10L);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str7, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 1 + "'", int8 == 1);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 0L + "'", long13 == 0L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 0 + "'", int23 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 0L + "'", long24 == 0L);
    }

    @Test
    public void test0719() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0719");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 100, 35L);
        logMark2.setLogMark((long) (-1), 35L);
    }

    @Test
    public void test0720() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0720");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long3 = logMark2.getLogFileId();
        logMark2.setLogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long8 = logMark7.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int11 = logMark2.compare(logMark7);
        logMark7.setLogMark((long) (short) -1, 52L);
        long long15 = logMark7.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', 0L);
        org.apache.bookkeeper.bookie.LogMark logMark19 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark19);
        logMark20.setLogMark((long) (short) -1, 0L);
        long long24 = logMark20.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark27);
        long long29 = logMark27.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark30 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark(logMark30);
        int int32 = logMark27.compare(logMark31);
        org.apache.bookkeeper.bookie.LogMark logMark35 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark38 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int39 = logMark35.compare(logMark38);
        org.apache.bookkeeper.bookie.LogMark logMark40 = new org.apache.bookkeeper.bookie.LogMark(logMark35);
        int int41 = logMark31.compare(logMark35);
        int int42 = logMark20.compare(logMark35);
        int int43 = logMark18.compare(logMark20);
        int int44 = logMark7.compare(logMark20);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 10L + "'", long8 == 10L);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + 1 + "'", int11 == 1);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + (-1L) + "'", long15 == (-1L));
        org.junit.Assert.assertNotNull(logMark19);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + (-1L) + "'", long24 == (-1L));
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 0L + "'", long29 == 0L);
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 1 + "'", int32 == 1);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + 0 + "'", int39 == 0);
        org.junit.Assert.assertTrue("'" + int41 + "' != '" + (-1) + "'", int41 == (-1));
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + (-1) + "'", int42 == (-1));
        org.junit.Assert.assertTrue("'" + int43 + "' != '" + 1 + "'", int43 == 1);
        org.junit.Assert.assertTrue("'" + int44 + "' != '" + 1 + "'", int44 == 1);
    }

    @Test
    public void test0721() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0721");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(35L, 97L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
    }

    @Test
    public void test0722() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0722");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(9223372036854775807L, (long) 0);
    }

    @Test
    public void test0723() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0723");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int9 = logMark5.compare(logMark8);
        logMark5.setLogMark((long) (short) 10, 10L);
        long long13 = logMark5.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int20 = logMark16.compare(logMark19);
        logMark16.setLogMark((long) (short) 10, 10L);
        long long24 = logMark16.getLogFileId();
        int int25 = logMark5.compare(logMark16);
        java.lang.String str26 = logMark16.toString();
        int int27 = logMark2.compare(logMark16);
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long29 = logMark2.getLogFileOffset();
        logMark2.setLogMark((long) (short) -1, (long) (short) 10);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 0L + "'", long29 == 0L);
    }

    @Test
    public void test0724() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0724");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str9 = logMark5.toString();
        long long10 = logMark5.getLogFileId();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - -1 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + (-1L) + "'", long10 == (-1L));
    }

    @Test
    public void test0725() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0725");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        int int9 = logMark2.compare(logMark8);
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
    }

    @Test
    public void test0726() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0726");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        java.lang.Class<?> wildcardClass5 = logMark3.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 52" + "'", str4, "LogMark: logFileId - -1 , logFileOffset - 52");
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0727() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0727");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        long long15 = logMark1.getLogFileId();
        logMark1.setLogMark((long) 100, (long) '#');
        long long19 = logMark1.getLogFileOffset();
        long long20 = logMark1.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 52" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 52");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + (-1L) + "'", long15 == (-1L));
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 35L + "'", long19 == 35L);
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 35L + "'", long20 == 35L);
    }

    @Test
    public void test0728() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0728");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', 10L);
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0729() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0729");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) (byte) -1);
    }

    @Test
    public void test0730() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0730");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long4 = logMark3.getLogFileOffset();
        long long5 = logMark3.getLogFileOffset();
        int int6 = logMark2.compare(logMark3);
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 52L + "'", long4 == 52L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 52L + "'", long5 == 52L);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 1 + "'", int6 == 1);
    }

    @Test
    public void test0731() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0731");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long4 = logMark3.getLogFileOffset();
        long long5 = logMark3.getLogFileOffset();
        int int6 = logMark2.compare(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int13 = logMark9.compare(logMark12);
        logMark9.setLogMark((long) (short) 10, 10L);
        long long17 = logMark9.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int24 = logMark20.compare(logMark23);
        logMark20.setLogMark((long) (short) 10, 10L);
        long long28 = logMark20.getLogFileId();
        int int29 = logMark9.compare(logMark20);
        long long30 = logMark20.getLogFileOffset();
        long long31 = logMark20.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark34 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark37 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int38 = logMark34.compare(logMark37);
        logMark34.setLogMark((long) (short) 10, 10L);
        long long42 = logMark34.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark45 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark48 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int49 = logMark45.compare(logMark48);
        logMark45.setLogMark((long) (short) 10, 10L);
        long long53 = logMark45.getLogFileId();
        int int54 = logMark34.compare(logMark45);
        long long55 = logMark34.getLogFileId();
        long long56 = logMark34.getLogFileOffset();
        long long57 = logMark34.getLogFileId();
        int int58 = logMark20.compare(logMark34);
        int int59 = logMark3.compare(logMark34);
        java.lang.Class<?> wildcardClass60 = logMark34.getClass();
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 52L + "'", long4 == 52L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 52L + "'", long5 == 52L);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 1 + "'", int6 == 1);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 10L + "'", long17 == 10L);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + 0 + "'", int29 == 0);
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 10L + "'", long30 == 10L);
        org.junit.Assert.assertTrue("'" + long31 + "' != '" + 10L + "'", long31 == 10L);
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + 0 + "'", int38 == 0);
        org.junit.Assert.assertTrue("'" + long42 + "' != '" + 10L + "'", long42 == 10L);
        org.junit.Assert.assertTrue("'" + int49 + "' != '" + 0 + "'", int49 == 0);
        org.junit.Assert.assertTrue("'" + long53 + "' != '" + 10L + "'", long53 == 10L);
        org.junit.Assert.assertTrue("'" + int54 + "' != '" + 0 + "'", int54 == 0);
        org.junit.Assert.assertTrue("'" + long55 + "' != '" + 10L + "'", long55 == 10L);
        org.junit.Assert.assertTrue("'" + long56 + "' != '" + 10L + "'", long56 == 10L);
        org.junit.Assert.assertTrue("'" + long57 + "' != '" + 10L + "'", long57 == 10L);
        org.junit.Assert.assertTrue("'" + int58 + "' != '" + 0 + "'", int58 == 0);
        org.junit.Assert.assertTrue("'" + int59 + "' != '" + (-1) + "'", int59 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass60);
    }

    @Test
    public void test0732() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0732");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        long long15 = logMark1.getLogFileId();
        logMark1.setLogMark((long) 100, (long) '#');
        java.lang.String str19 = logMark1.toString();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 52" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 52");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + (-1L) + "'", long15 == (-1L));
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 35" + "'", str19, "LogMark: logFileId - 100 , logFileOffset - 35");
    }

    @Test
    public void test0733() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0733");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long5 = logMark4.getLogFileOffset();
        java.lang.String str6 = logMark4.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        long long10 = logMark9.getLogFileId();
        int int11 = logMark4.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark12 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long13 = logMark12.getLogFileOffset();
        int int14 = logMark4.compare(logMark12);
        long long15 = logMark12.getLogFileOffset();
        int int16 = logMark2.compare(logMark12);
        java.lang.Class<?> wildcardClass17 = logMark12.getClass();
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 52L + "'", long5 == 52L);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 52" + "'", str6, "LogMark: logFileId - -1 , logFileOffset - 52");
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + (-1L) + "'", long10 == (-1L));
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + 0 + "'", int11 == 0);
        org.junit.Assert.assertNotNull(logMark12);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 52L + "'", long13 == 52L);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 52L + "'", long15 == 52L);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0734() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0734");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        long long16 = logMark15.getLogFileId();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 52" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 52");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + (-1L) + "'", long16 == (-1L));
    }

    @Test
    public void test0735() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0735");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
    }

    @Test
    public void test0736() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0736");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long8 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
    }

    @Test
    public void test0737() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0737");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass7 = logMark2.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - -1" + "'", str4, "LogMark: logFileId - 52 , logFileOffset - -1");
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0738() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0738");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark((long) 'a', 35L);
        java.lang.String str7 = logMark3.toString();
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 35" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 35");
    }

    @Test
    public void test0739() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0739");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.String str11 = logMark10.toString();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 100" + "'", str11, "LogMark: logFileId - -1 , logFileOffset - 100");
    }

    @Test
    public void test0740() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0740");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark1.compare(logMark6);
        java.lang.String str8 = logMark1.toString();
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark1.readLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "41) test0740(Regression2Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str2, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + (-1) + "'", int7 == (-1));
// flaky "29) test0740(Regression2Test)":         org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str8, "LogMark: logFileId - 32 , logFileOffset - 32");
    }

    @Test
    public void test0741() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0741");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long6 = logMark5.getLogFileOffset();
        java.lang.String str7 = logMark5.toString();
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0742() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0742");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        java.lang.String str3 = logMark2.toString();
        logMark2.setLogMark((long) '4', 0L);
        java.lang.Class<?> wildcardClass7 = logMark2.getClass();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0743() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0743");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        long long6 = logMark5.getLogFileId();
        long long7 = logMark5.getLogFileId();
        int int8 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark9.writeLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 0L + "'", long7 == 0L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 1 + "'", int8 == 1);
    }

    @Test
    public void test0744() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0744");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long9 = logMark2.getLogFileOffset();
        long long10 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 0L + "'", long9 == 0L);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 97L + "'", long10 == 97L);
    }

    @Test
    public void test0745() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0745");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        logMark13.setLogMark((long) (byte) 10, (long) 1);
        long long28 = logMark13.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 1L + "'", long28 == 1L);
    }

    @Test
    public void test0746() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0746");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 10, (long) 0);
        java.lang.String str3 = logMark2.toString();
        logMark2.setLogMark((long) (byte) 10, (long) 10);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 0" + "'", str3, "LogMark: logFileId - 10 , logFileOffset - 0");
    }

    @Test
    public void test0747() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0747");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.writeLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "42) test0747(Regression2Test)":         org.junit.Assert.assertTrue("'" + long2 + "' != '" + 32L + "'", long2 == 32L);
    }

    @Test
    public void test0748() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0748");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        java.lang.String str12 = logMark8.toString();
        long long13 = logMark8.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
// flaky "43) test0748(Regression2Test)":         org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str10, "LogMark: logFileId - 32 , logFileOffset - 32");
// flaky "30) test0748(Regression2Test)":         org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str12, "LogMark: logFileId - 32 , logFileOffset - 32");
// flaky "19) test0748(Regression2Test)":         org.junit.Assert.assertTrue("'" + long13 + "' != '" + 32L + "'", long13 == 32L);
    }

    @Test
    public void test0749() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0749");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        long long3 = logMark2.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
    }

    @Test
    public void test0750() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0750");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileOffset();
        long long5 = logMark3.getLogFileId();
        logMark3.setLogMark(97L, (long) '4');
        java.lang.String str9 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(35L, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int19 = logMark15.compare(logMark18);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        logMark20.setLogMark((long) (short) 100, 35L);
        logMark20.setLogMark(32L, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark30 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long31 = logMark30.getLogFileOffset();
        long long32 = logMark30.getLogFileOffset();
        int int33 = logMark29.compare(logMark30);
        logMark29.setLogMark((long) (byte) 1, (long) 'a');
        int int37 = logMark20.compare(logMark29);
        int int38 = logMark12.compare(logMark20);
        int int39 = logMark3.compare(logMark12);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 35L + "'", long4 == 35L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 0L + "'", long5 == 0L);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 52" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 52");
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertNotNull(logMark30);
// flaky "44) test0750(Regression2Test)":         org.junit.Assert.assertTrue("'" + long31 + "' != '" + 32L + "'", long31 == 32L);
// flaky "31) test0750(Regression2Test)":         org.junit.Assert.assertTrue("'" + long32 + "' != '" + 32L + "'", long32 == 32L);
// flaky "20) test0750(Regression2Test)":         org.junit.Assert.assertTrue("'" + int33 + "' != '" + (-1) + "'", int33 == (-1));
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + 1 + "'", int37 == 1);
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + 1 + "'", int38 == 1);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + 1 + "'", int39 == 1);
    }

    @Test
    public void test0751() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0751");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark16 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark16);
        java.lang.String str18 = logMark17.toString();
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        java.lang.String str23 = logMark22.toString();
        logMark22.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark27 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark27);
        int int29 = logMark22.compare(logMark27);
        int int30 = logMark17.compare(logMark27);
        int int31 = logMark15.compare(logMark27);
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        java.nio.ByteBuffer byteBuffer33 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark15.writeLogMark(byteBuffer33);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "45) test0751(Regression2Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str2, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertNotNull(logMark16);
// flaky "32) test0751(Regression2Test)":         org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str18, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark27);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
    }

    @Test
    public void test0752() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0752");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 0, 97L);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0753() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0753");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        logMark3.setLogMark(0L, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int18 = logMark14.compare(logMark17);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark19);
        org.apache.bookkeeper.bookie.LogMark logMark21 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long22 = logMark21.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        java.lang.String str24 = logMark23.toString();
        logMark23.setLogMark(32L, 9223372036854775807L);
        long long28 = logMark23.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark(logMark23);
        int int30 = logMark20.compare(logMark29);
        int int31 = logMark3.compare(logMark20);
        long long32 = logMark3.getLogFileOffset();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 0 + "'", int18 == 0);
        org.junit.Assert.assertNotNull(logMark21);
// flaky "46) test0753(Regression2Test)":         org.junit.Assert.assertTrue("'" + long22 + "' != '" + 32L + "'", long22 == 32L);
// flaky "33) test0753(Regression2Test)":         org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str24, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 32L + "'", long28 == 32L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 1 + "'", int30 == 1);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + (-1) + "'", int31 == (-1));
        org.junit.Assert.assertTrue("'" + long32 + "' != '" + 0L + "'", long32 == 0L);
    }

    @Test
    public void test0754() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0754");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(10L, (long) (short) -1);
        long long3 = logMark2.getLogFileId();
        logMark2.setLogMark(0L, (long) 0);
        long long7 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 10L + "'", long3 == 10L);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 0L + "'", long7 == 0L);
    }

    @Test
    public void test0755() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0755");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), (-1L));
    }

    @Test
    public void test0756() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0756");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark2.setLogMark((long) (byte) 100, (long) (short) 0);
        logMark2.setLogMark(32L, 9223372036854775807L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, 35L);
        int int12 = logMark2.compare(logMark11);
        long long13 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 1 + "'", int12 == 1);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 32L + "'", long13 == 32L);
    }

    @Test
    public void test0757() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0757");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 100, 35L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 35L + "'", long4 == 35L);
    }

    @Test
    public void test0758() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0758");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long9 = logMark8.getLogFileOffset();
        long long10 = logMark8.getLogFileOffset();
        java.lang.String str11 = logMark8.toString();
        int int12 = logMark5.compare(logMark8);
        java.nio.ByteBuffer byteBuffer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.readLogMark(byteBuffer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
// flaky "47) test0758(Regression2Test)":         org.junit.Assert.assertTrue("'" + long9 + "' != '" + 32L + "'", long9 == 32L);
// flaky "34) test0758(Regression2Test)":         org.junit.Assert.assertTrue("'" + long10 + "' != '" + 32L + "'", long10 == 32L);
// flaky "21) test0758(Regression2Test)":         org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str11, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 1 + "'", int12 == 1);
    }

    @Test
    public void test0759() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0759");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long8 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', 0L);
        int int12 = logMark2.compare(logMark11);
        java.lang.String str13 = logMark11.toString();
        java.lang.String str14 = logMark11.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str13, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str14, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0760() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0760");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long4 = logMark0.getLogFileId();
        logMark0.setLogMark(52L, (long) 100);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long9 = logMark0.getLogFileId();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "48) test0760(Regression2Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 32L + "'", long1 == 32L);
// flaky "35) test0760(Regression2Test)":         org.junit.Assert.assertTrue("'" + long4 + "' != '" + 32L + "'", long4 == 32L);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 52L + "'", long9 == 52L);
    }

    @Test
    public void test0761() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0761");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        logMark2.setLogMark(32L, 9223372036854775807L);
        long long7 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 100L + "'", long1 == 100L);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str3, "LogMark: logFileId - 52 , logFileOffset - 100");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 32L + "'", long7 == 32L);
    }

    @Test
    public void test0762() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0762");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        java.lang.String str8 = logMark5.toString();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        long long11 = logMark10.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str8, "LogMark: logFileId - -1 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 0L + "'", long11 == 0L);
    }

    @Test
    public void test0763() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0763");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long6 = logMark5.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int13 = logMark9.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark14 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        int int16 = logMark9.compare(logMark15);
        int int17 = logMark5.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        java.lang.String str23 = logMark22.toString();
        logMark22.setLogMark((long) (short) -1, 10L);
        int int27 = logMark18.compare(logMark22);
        java.nio.ByteBuffer byteBuffer28 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark22.readLogMark(byteBuffer28);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertNotNull(logMark14);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
    }

    @Test
    public void test0764() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0764");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        java.lang.String str9 = logMark8.toString();
        logMark8.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark13 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        int int15 = logMark8.compare(logMark13);
        int int16 = logMark3.compare(logMark13);
        int int17 = logMark1.compare(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        java.lang.String str23 = logMark22.toString();
        logMark22.setLogMark((long) (-1), (long) (byte) 100);
        long long27 = logMark22.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark22);
        java.lang.String str29 = logMark22.toString();
        int int30 = logMark18.compare(logMark22);
        java.nio.ByteBuffer byteBuffer31 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark22.readLogMark(byteBuffer31);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertNotNull(logMark2);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str4, "LogMark: logFileId - 52 , logFileOffset - 100");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark13);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + (-1) + "'", int15 == (-1));
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 0 + "'", int16 == 0);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long27 + "' != '" + (-1L) + "'", long27 == (-1L));
        org.junit.Assert.assertEquals("'" + str29 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 100" + "'", str29, "LogMark: logFileId - -1 , logFileOffset - 100");
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 1 + "'", int30 == 1);
    }

    @Test
    public void test0765() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0765");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark(100L, (long) '4');
        long long7 = logMark3.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 52L + "'", long7 == 52L);
    }

    @Test
    public void test0766() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0766");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(35L, 32L);
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0767() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0767");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 0, 97L);
        logMark2.setLogMark(52L, 10L);
        java.lang.String str6 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 10" + "'", str6, "LogMark: logFileId - 52 , logFileOffset - 10");
    }

    @Test
    public void test0768() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0768");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        java.nio.ByteBuffer byteBuffer14 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark12.writeLogMark(byteBuffer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str10, "LogMark: logFileId - 52 , logFileOffset - 100");
    }

    @Test
    public void test0769() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0769");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark7.setLogMark((long) (short) 100, 35L);
        logMark7.setLogMark(32L, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark17 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long18 = logMark17.getLogFileOffset();
        long long19 = logMark17.getLogFileOffset();
        int int20 = logMark16.compare(logMark17);
        logMark16.setLogMark((long) (byte) 1, (long) 'a');
        int int24 = logMark7.compare(logMark16);
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark17);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 100L + "'", long18 == 100L);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 100L + "'", long19 == 100L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + (-1) + "'", int20 == (-1));
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 1 + "'", int24 == 1);
    }

    @Test
    public void test0770() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0770");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, (-1L));
    }

    @Test
    public void test0771() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0771");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), (long) (-1));
    }

    @Test
    public void test0772() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0772");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int14 = logMark10.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        logMark15.setLogMark((long) (short) 100, 35L);
        int int19 = logMark5.compare(logMark15);
        java.lang.Class<?> wildcardClass20 = logMark5.getClass();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass20);
    }

    @Test
    public void test0773() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0773");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, (-1L));
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0774() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0774");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long4 = logMark3.getLogFileOffset();
        long long5 = logMark3.getLogFileOffset();
        int int6 = logMark2.compare(logMark3);
        java.lang.String str7 = logMark3.toString();
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 100L + "'", long4 == 100L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 100L + "'", long5 == 100L);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + (-1) + "'", int6 == (-1));
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str7, "LogMark: logFileId - 52 , logFileOffset - 100");
    }

    @Test
    public void test0775() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0775");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        java.lang.String str8 = logMark7.toString();
        logMark7.setLogMark((long) (-1), (long) (byte) 100);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int13 = logMark3.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark14 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long15 = logMark14.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        logMark14.setLogMark((long) ' ', (long) ' ');
        long long20 = logMark14.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        org.apache.bookkeeper.bookie.LogMark logMark22 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark(logMark22);
        java.lang.String str24 = logMark23.toString();
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark27);
        int int29 = logMark23.compare(logMark28);
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        java.lang.String str34 = logMark32.toString();
        org.apache.bookkeeper.bookie.LogMark logMark35 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark36 = new org.apache.bookkeeper.bookie.LogMark(logMark35);
        java.lang.String str37 = logMark36.toString();
        org.apache.bookkeeper.bookie.LogMark logMark40 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark(logMark40);
        java.lang.String str42 = logMark41.toString();
        logMark41.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark46 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark47 = new org.apache.bookkeeper.bookie.LogMark(logMark46);
        int int48 = logMark41.compare(logMark46);
        int int49 = logMark36.compare(logMark46);
        logMark36.setLogMark(97L, (long) '4');
        logMark36.setLogMark(32L, 0L);
        int int56 = logMark32.compare(logMark36);
        int int57 = logMark28.compare(logMark32);
        int int58 = logMark14.compare(logMark28);
        int int59 = logMark3.compare(logMark28);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str8, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertNotNull(logMark14);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 100L + "'", long15 == 100L);
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 32L + "'", long20 == 32L);
        org.junit.Assert.assertNotNull(logMark22);
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str24, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertEquals("'" + str34 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str34, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertNotNull(logMark35);
        org.junit.Assert.assertEquals("'" + str37 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str37, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertEquals("'" + str42 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str42, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark46);
        org.junit.Assert.assertTrue("'" + int48 + "' != '" + (-1) + "'", int48 == (-1));
        org.junit.Assert.assertTrue("'" + int49 + "' != '" + 0 + "'", int49 == 0);
        org.junit.Assert.assertTrue("'" + int56 + "' != '" + (-1) + "'", int56 == (-1));
        org.junit.Assert.assertTrue("'" + int57 + "' != '" + 1 + "'", int57 == 1);
        org.junit.Assert.assertTrue("'" + int58 + "' != '" + (-1) + "'", int58 == (-1));
        org.junit.Assert.assertTrue("'" + int59 + "' != '" + (-1) + "'", int59 == (-1));
    }

    @Test
    public void test0776() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0776");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 100, 35L);
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0777() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0777");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, (-1L));
        long long3 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + (-1L) + "'", long3 == (-1L));
    }

    @Test
    public void test0778() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0778");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.writeLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
    }

    @Test
    public void test0779() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0779");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        java.lang.String str12 = logMark11.toString();
        logMark11.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark16 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark16);
        int int18 = logMark11.compare(logMark16);
        int int19 = logMark6.compare(logMark16);
        long long20 = logMark6.getLogFileId();
        logMark6.setLogMark((long) 100, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark24 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark(logMark24);
        logMark25.setLogMark((long) (short) -1, 0L);
        int int29 = logMark6.compare(logMark25);
        int int30 = logMark3.compare(logMark6);
        long long31 = logMark6.getLogFileId();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertNotNull(logMark5);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str7, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str12, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark16);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + (-1) + "'", int18 == (-1));
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 32L + "'", long20 == 32L);
        org.junit.Assert.assertNotNull(logMark24);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + 1 + "'", int29 == 1);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + (-1) + "'", int30 == (-1));
        org.junit.Assert.assertTrue("'" + long31 + "' != '" + 100L + "'", long31 == 100L);
    }

    @Test
    public void test0780() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0780");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        java.lang.String str6 = logMark5.toString();
        int int7 = logMark3.compare(logMark5);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 35L + "'", long4 == 35L);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str6, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
    }

    @Test
    public void test0781() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0781");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int14 = logMark10.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        int int16 = logMark6.compare(logMark10);
        long long17 = logMark10.getLogFileId();
        java.lang.String str18 = logMark10.toString();
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        java.nio.ByteBuffer byteBuffer20 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark10.writeLogMark(byteBuffer20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 97L + "'", long17 == 97L);
        org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str18, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0782() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0782");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) -1, (long) (short) -1);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + (-1L) + "'", long3 == (-1L));
    }

    @Test
    public void test0783() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0783");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        logMark5.setLogMark((long) (short) -1, 0L);
        long long9 = logMark5.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        long long14 = logMark12.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        int int17 = logMark12.compare(logMark16);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int24 = logMark20.compare(logMark23);
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark(logMark20);
        int int26 = logMark16.compare(logMark20);
        int int27 = logMark5.compare(logMark20);
        logMark20.setLogMark(52L, 35L);
        int int31 = logMark2.compare(logMark20);
        long long32 = logMark20.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + (-1L) + "'", long9 == (-1L));
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 0L + "'", long14 == 0L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 1 + "'", int17 == 1);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + (-1) + "'", int26 == (-1));
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 1 + "'", int31 == 1);
        org.junit.Assert.assertTrue("'" + long32 + "' != '" + 35L + "'", long32 == 35L);
    }

    @Test
    public void test0784() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0784");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int10 = logMark3.compare(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long12 = logMark11.getLogFileId();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
// flaky "49) test0784(Regression2Test)":         org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + (-1L) + "'", long12 == (-1L));
    }

    @Test
    public void test0785() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0785");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark2.setLogMark((long) (byte) 100, 52L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - -1" + "'", str4, "LogMark: logFileId - 52 , logFileOffset - -1");
    }

    @Test
    public void test0786() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0786");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        logMark3.setLogMark((long) 10, 1L);
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0787() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0787");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        long long24 = logMark13.getLogFileOffset();
        long long25 = logMark13.getLogFileId();
        java.lang.String str26 = logMark13.toString();
        java.nio.ByteBuffer byteBuffer27 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark13.readLogMark(byteBuffer27);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 10L + "'", long25 == 10L);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test0788() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0788");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int10 = logMark3.compare(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        int int18 = logMark3.compare(logMark16);
        long long19 = logMark3.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        logMark20.setLogMark(52L, 32L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
// flaky "50) test0788(Regression2Test)":         org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + (-1) + "'", int18 == (-1));
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + (-1L) + "'", long19 == (-1L));
    }

    @Test
    public void test0789() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0789");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.readLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
    }

    @Test
    public void test0790() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0790");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 100, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark5);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.writeLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
    }

    @Test
    public void test0791() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0791");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        long long24 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark25 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark25);
        java.lang.String str27 = logMark26.toString();
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        org.apache.bookkeeper.bookie.LogMark logMark29 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long30 = logMark29.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark(logMark29);
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark(logMark29);
        int int33 = logMark28.compare(logMark32);
        int int34 = logMark13.compare(logMark32);
        org.apache.bookkeeper.bookie.LogMark logMark35 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        logMark32.setLogMark((long) 'a', (long) (byte) -1);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertNotNull(logMark25);
// flaky "51) test0791(Regression2Test)":         org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str27, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark29);
// flaky "36) test0791(Regression2Test)":         org.junit.Assert.assertTrue("'" + long30 + "' != '" + 10L + "'", long30 == 10L);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 0 + "'", int33 == 0);
// flaky "22) test0791(Regression2Test)":         org.junit.Assert.assertTrue("'" + int34 + "' != '" + 1 + "'", int34 == 1);
    }

    @Test
    public void test0792() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0792");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 52L);
        long long3 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 1L + "'", long3 == 1L);
    }

    @Test
    public void test0793() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0793");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        long long3 = logMark0.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(35L, 1L);
        int int8 = logMark0.compare(logMark7);
        org.junit.Assert.assertNotNull(logMark0);
// flaky "52) test0793(Regression2Test)":         org.junit.Assert.assertTrue("'" + long2 + "' != '" + 10L + "'", long2 == 10L);
// flaky "37) test0793(Regression2Test)":         org.junit.Assert.assertTrue("'" + long3 + "' != '" + (-1L) + "'", long3 == (-1L));
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
    }

    @Test
    public void test0794() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0794");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long8 = logMark7.getLogFileId();
        long long9 = logMark7.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        java.lang.String str11 = logMark10.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 97L + "'", long8 == 97L);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 97L + "'", long9 == 97L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str11, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0795() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0795");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        logMark3.setLogMark((long) ' ', 9223372036854775807L);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        org.apache.bookkeeper.bookie.LogMark logMark18 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark18);
        int int20 = logMark13.compare(logMark19);
        int int21 = logMark3.compare(logMark19);
        long long22 = logMark19.getLogFileOffset();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertNotNull(logMark18);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 1 + "'", int20 == 1);
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + 1 + "'", int21 == 1);
// flaky "53) test0795(Regression2Test)":         org.junit.Assert.assertTrue("'" + long22 + "' != '" + 10L + "'", long22 == 10L);
    }

    @Test
    public void test0796() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0796");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long6 = logMark2.getLogFileOffset();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
    }

    @Test
    public void test0797() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0797");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark5.setLogMark(1L, (long) (byte) 1);
        long long10 = logMark5.getLogFileId();
        long long11 = logMark5.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        long long13 = logMark12.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 100, 35L);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int23 = logMark19.compare(logMark22);
        logMark19.setLogMark((long) (short) 10, 10L);
        long long27 = logMark19.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark30 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int34 = logMark30.compare(logMark33);
        logMark30.setLogMark((long) (short) 10, 10L);
        long long38 = logMark30.getLogFileId();
        int int39 = logMark19.compare(logMark30);
        java.lang.String str40 = logMark30.toString();
        int int41 = logMark16.compare(logMark30);
        int int42 = logMark12.compare(logMark30);
        java.lang.String str43 = logMark12.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 1L + "'", long10 == 1L);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 1L + "'", long11 == 1L);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 1L + "'", long13 == 1L);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 0 + "'", int23 == 0);
        org.junit.Assert.assertTrue("'" + long27 + "' != '" + 10L + "'", long27 == 10L);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
        org.junit.Assert.assertTrue("'" + long38 + "' != '" + 10L + "'", long38 == 10L);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + 0 + "'", int39 == 0);
        org.junit.Assert.assertEquals("'" + str40 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str40, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int41 + "' != '" + 1 + "'", int41 == 1);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + (-1) + "'", int42 == (-1));
        org.junit.Assert.assertEquals("'" + str43 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 1" + "'", str43, "LogMark: logFileId - 1 , logFileOffset - 1");
    }

    @Test
    public void test0798() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0798");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long9 = logMark8.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        java.lang.String str11 = logMark10.toString();
        int int12 = logMark7.compare(logMark10);
        int int13 = logMark2.compare(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int20 = logMark16.compare(logMark19);
        logMark16.setLogMark((long) (short) 10, 10L);
        long long24 = logMark16.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark30 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int31 = logMark27.compare(logMark30);
        logMark27.setLogMark((long) (short) 10, 10L);
        long long35 = logMark27.getLogFileId();
        int int36 = logMark16.compare(logMark27);
        long long37 = logMark27.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark40 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark(logMark40);
        java.lang.String str42 = logMark41.toString();
        logMark41.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark46 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark47 = new org.apache.bookkeeper.bookie.LogMark(logMark46);
        int int48 = logMark41.compare(logMark46);
        int int49 = logMark27.compare(logMark46);
        org.apache.bookkeeper.bookie.LogMark logMark52 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark55 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int56 = logMark52.compare(logMark55);
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark(logMark52);
        org.apache.bookkeeper.bookie.LogMark logMark58 = new org.apache.bookkeeper.bookie.LogMark(logMark57);
        int int59 = logMark27.compare(logMark58);
        long long60 = logMark58.getLogFileId();
        int int61 = logMark2.compare(logMark58);
        long long62 = logMark2.getLogFileOffset();
        logMark2.setLogMark((long) (byte) -1, 0L);
        org.junit.Assert.assertNotNull(logMark8);
// flaky "54) test0798(Regression2Test)":         org.junit.Assert.assertTrue("'" + long9 + "' != '" + 10L + "'", long9 == 10L);
// flaky "38) test0798(Regression2Test)":         org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str11, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 1 + "'", int12 == 1);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + int36 + "' != '" + 0 + "'", int36 == 0);
        org.junit.Assert.assertTrue("'" + long37 + "' != '" + 10L + "'", long37 == 10L);
        org.junit.Assert.assertEquals("'" + str42 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str42, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark46);
// flaky "23) test0798(Regression2Test)":         org.junit.Assert.assertTrue("'" + int48 + "' != '" + 0 + "'", int48 == 0);
// flaky "6) test0798(Regression2Test)":         org.junit.Assert.assertTrue("'" + int49 + "' != '" + 1 + "'", int49 == 1);
        org.junit.Assert.assertTrue("'" + int56 + "' != '" + 0 + "'", int56 == 0);
        org.junit.Assert.assertTrue("'" + int59 + "' != '" + (-1) + "'", int59 == (-1));
        org.junit.Assert.assertTrue("'" + long60 + "' != '" + 97L + "'", long60 == 97L);
        org.junit.Assert.assertTrue("'" + int61 + "' != '" + (-1) + "'", int61 == (-1));
        org.junit.Assert.assertTrue("'" + long62 + "' != '" + (-1L) + "'", long62 == (-1L));
    }

    @Test
    public void test0799() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0799");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        logMark5.setLogMark((long) (-1), (long) '4');
        java.lang.Class<?> wildcardClass9 = logMark5.getClass();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "55) test0799(Regression2Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
// flaky "39) test0799(Regression2Test)":         org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str4, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0800() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0800");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        logMark2.setLogMark((long) (short) 0, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark8.compare(logMark11);
        int int13 = logMark2.compare(logMark8);
        java.lang.String str14 = logMark2.toString();
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str14, "LogMark: logFileId - 0 , logFileOffset - 35");
    }

    @Test
    public void test0801() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0801");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (short) -1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
    }

    @Test
    public void test0802() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0802");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.lang.String str28 = logMark27.toString();
        logMark27.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark32 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        int int34 = logMark27.compare(logMark32);
        int int35 = logMark13.compare(logMark32);
        java.lang.String str36 = logMark32.toString();
        org.apache.bookkeeper.bookie.LogMark logMark37 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        logMark32.setLogMark((long) (short) -1, (long) 10);
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int47 = logMark43.compare(logMark46);
        logMark43.setLogMark((long) (short) 10, 10L);
        long long51 = logMark43.getLogFileId();
        int int52 = logMark32.compare(logMark43);
        org.apache.bookkeeper.bookie.LogMark logMark53 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        org.apache.bookkeeper.bookie.LogMark logMark56 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark(logMark56);
        java.lang.String str58 = logMark57.toString();
        logMark57.setLogMark((long) (-1), (long) (byte) 100);
        logMark57.setLogMark((long) 10, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark65 = new org.apache.bookkeeper.bookie.LogMark(logMark57);
        logMark65.setLogMark((long) (short) -1, (long) 'a');
        int int69 = logMark32.compare(logMark65);
        org.apache.bookkeeper.bookie.LogMark logMark70 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long71 = logMark70.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark72 = new org.apache.bookkeeper.bookie.LogMark(logMark70);
        org.apache.bookkeeper.bookie.LogMark logMark73 = new org.apache.bookkeeper.bookie.LogMark(logMark70);
        java.lang.String str74 = logMark73.toString();
        logMark73.setLogMark(0L, (long) (byte) 0);
        logMark73.setLogMark(100L, (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark81 = new org.apache.bookkeeper.bookie.LogMark(logMark73);
        int int82 = logMark65.compare(logMark73);
        long long83 = logMark73.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
// flaky "56) test0802(Regression2Test)":         org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
// flaky "40) test0802(Regression2Test)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + 1 + "'", int35 == 1);
// flaky "24) test0802(Regression2Test)":         org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str36, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long51 + "' != '" + 10L + "'", long51 == 10L);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + (-1) + "'", int52 == (-1));
        org.junit.Assert.assertEquals("'" + str58 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str58, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int69 + "' != '" + (-1) + "'", int69 == (-1));
        org.junit.Assert.assertNotNull(logMark70);
        org.junit.Assert.assertTrue("'" + long71 + "' != '" + 10L + "'", long71 == 10L);
        org.junit.Assert.assertEquals("'" + str74 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str74, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int82 + "' != '" + (-1) + "'", int82 == (-1));
        org.junit.Assert.assertTrue("'" + long83 + "' != '" + 100L + "'", long83 == 100L);
    }

    @Test
    public void test0803() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0803");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int16 = logMark12.compare(logMark15);
        logMark12.setLogMark((long) (short) 10, 10L);
        long long20 = logMark12.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int27 = logMark23.compare(logMark26);
        logMark23.setLogMark((long) (short) 10, 10L);
        long long31 = logMark23.getLogFileId();
        int int32 = logMark12.compare(logMark23);
        java.lang.String str33 = logMark23.toString();
        java.lang.String str34 = logMark23.toString();
        logMark23.setLogMark((long) (-1), 100L);
        int int38 = logMark2.compare(logMark23);
        java.nio.ByteBuffer byteBuffer39 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer39);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 0 + "'", int16 == 0);
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 10L + "'", long20 == 10L);
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 0 + "'", int27 == 0);
        org.junit.Assert.assertTrue("'" + long31 + "' != '" + 10L + "'", long31 == 10L);
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 0 + "'", int32 == 0);
        org.junit.Assert.assertEquals("'" + str33 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str33, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str34 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str34, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + 1 + "'", int38 == 1);
    }

    @Test
    public void test0804() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0804");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark2.setLogMark((long) (byte) 1, (long) '#');
        logMark2.setLogMark((long) (-1), (long) 1);
    }

    @Test
    public void test0805() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0805");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        logMark8.setLogMark(10L, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0806() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0806");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, 35L);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0807() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0807");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, (long) (short) 0);
        int int8 = logMark3.compare(logMark7);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 35L + "'", long4 == 35L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
    }

    @Test
    public void test0808() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0808");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(0L, 52L);
        int int6 = logMark2.compare(logMark5);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 1 + "'", int6 == 1);
    }

    @Test
    public void test0809() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0809");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        long long5 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark6.readLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 0L + "'", long5 == 0L);
    }

    @Test
    public void test0810() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0810");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int14 = logMark10.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        int int16 = logMark6.compare(logMark10);
        long long17 = logMark10.getLogFileId();
        java.lang.String str18 = logMark10.toString();
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        java.lang.Class<?> wildcardClass20 = logMark10.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 97L + "'", long17 == 97L);
        org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str18, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(wildcardClass20);
    }

    @Test
    public void test0811() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0811");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        logMark2.setLogMark((long) ' ', (long) (byte) -1);
        long long8 = logMark2.getLogFileOffset();
        long long9 = logMark2.getLogFileId();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 32L + "'", long9 == 32L);
    }

    @Test
    public void test0812() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0812");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark7.compare(logMark11);
        logMark11.setLogMark(97L, 97L);
        java.lang.Class<?> wildcardClass16 = logMark11.getClass();
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
        org.junit.Assert.assertNotNull(wildcardClass16);
    }

    @Test
    public void test0813() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0813");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), 10L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark12 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long13 = logMark12.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        java.lang.String str15 = logMark14.toString();
        int int16 = logMark11.compare(logMark14);
        int int17 = logMark6.compare(logMark14);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int24 = logMark20.compare(logMark23);
        logMark20.setLogMark((long) (short) 10, 10L);
        long long28 = logMark20.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark34 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int35 = logMark31.compare(logMark34);
        logMark31.setLogMark((long) (short) 10, 10L);
        long long39 = logMark31.getLogFileId();
        int int40 = logMark20.compare(logMark31);
        java.lang.String str41 = logMark31.toString();
        java.lang.String str42 = logMark31.toString();
        int int43 = logMark6.compare(logMark31);
        int int44 = logMark2.compare(logMark31);
        org.apache.bookkeeper.bookie.LogMark logMark45 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 10L + "'", long3 == 10L);
        org.junit.Assert.assertNotNull(logMark12);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str15, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 1 + "'", int17 == 1);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 0 + "'", int35 == 0);
        org.junit.Assert.assertTrue("'" + long39 + "' != '" + 10L + "'", long39 == 10L);
        org.junit.Assert.assertTrue("'" + int40 + "' != '" + 0 + "'", int40 == 0);
        org.junit.Assert.assertEquals("'" + str41 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str41, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str42 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str42, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int43 + "' != '" + 1 + "'", int43 == 1);
        org.junit.Assert.assertTrue("'" + int44 + "' != '" + (-1) + "'", int44 == (-1));
    }

    @Test
    public void test0814() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0814");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(9223372036854775807L, (long) (-1));
    }

    @Test
    public void test0815() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0815");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long5 = logMark4.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        java.lang.String str7 = logMark6.toString();
        int int8 = logMark3.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        long long13 = logMark11.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark17.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark17);
        int int22 = logMark11.compare(logMark21);
        int int23 = logMark3.compare(logMark11);
        java.lang.String str24 = logMark11.toString();
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 10L + "'", long5 == 10L);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str7, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 1 + "'", int8 == 1);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 0L + "'", long13 == 0L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 0 + "'", int23 == 0);
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str24, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0816() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0816");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0817() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0817");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 10L + "'", long4 == 10L);
    }

    @Test
    public void test0818() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0818");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        logMark8.setLogMark(10L, 0L);
        logMark8.setLogMark((long) 100, (long) 'a');
        long long17 = logMark8.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str10, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 100L + "'", long17 == 100L);
    }

    @Test
    public void test0819() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0819");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        java.lang.String str23 = logMark13.toString();
        java.lang.String str24 = logMark13.toString();
        org.apache.bookkeeper.bookie.LogMark logMark25 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark25);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        long long28 = logMark27.getLogFileId();
        int int29 = logMark13.compare(logMark27);
        org.apache.bookkeeper.bookie.LogMark logMark30 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark(logMark30);
        java.lang.String str32 = logMark31.toString();
        org.apache.bookkeeper.bookie.LogMark logMark35 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark36 = new org.apache.bookkeeper.bookie.LogMark(logMark35);
        java.lang.String str37 = logMark36.toString();
        logMark36.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark41 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark42 = new org.apache.bookkeeper.bookie.LogMark(logMark41);
        int int43 = logMark36.compare(logMark41);
        int int44 = logMark31.compare(logMark41);
        org.apache.bookkeeper.bookie.LogMark logMark45 = new org.apache.bookkeeper.bookie.LogMark(logMark41);
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark(logMark45);
        long long47 = logMark46.getLogFileId();
        int int48 = logMark27.compare(logMark46);
        java.lang.Class<?> wildcardClass49 = logMark27.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str23, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str24, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark25);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + (-1L) + "'", long28 == (-1L));
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + 1 + "'", int29 == 1);
        org.junit.Assert.assertNotNull(logMark30);
        org.junit.Assert.assertEquals("'" + str32 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str32, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str37 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str37, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark41);
        org.junit.Assert.assertTrue("'" + int43 + "' != '" + 0 + "'", int43 == 0);
        org.junit.Assert.assertTrue("'" + int44 + "' != '" + 0 + "'", int44 == 0);
        org.junit.Assert.assertTrue("'" + long47 + "' != '" + (-1L) + "'", long47 == (-1L));
        org.junit.Assert.assertTrue("'" + int48 + "' != '" + 0 + "'", int48 == 0);
        org.junit.Assert.assertNotNull(wildcardClass49);
    }

    @Test
    public void test0820() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0820");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        long long8 = logMark6.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        int int11 = logMark6.compare(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int18 = logMark14.compare(logMark17);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        int int20 = logMark10.compare(logMark14);
        java.lang.String str21 = logMark10.toString();
        int int22 = logMark2.compare(logMark10);
        java.nio.ByteBuffer byteBuffer23 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark10.writeLogMark(byteBuffer23);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + 1 + "'", int11 == 1);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 0 + "'", int18 == 0);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + (-1) + "'", int20 == (-1));
        org.junit.Assert.assertEquals("'" + str21 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str21, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + (-1) + "'", int22 == (-1));
    }

    @Test
    public void test0821() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0821");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long6 = logMark3.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int13 = logMark9.compare(logMark12);
        logMark12.setLogMark(1L, (long) (byte) 1);
        long long17 = logMark12.getLogFileId();
        long long18 = logMark12.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        logMark19.setLogMark(35L, (long) '4');
        int int23 = logMark3.compare(logMark19);
        java.lang.String str24 = logMark19.toString();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str4, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 10L + "'", long6 == 10L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 1L + "'", long17 == 1L);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 1L + "'", long18 == 1L);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + (-1) + "'", int23 == (-1));
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 52" + "'", str24, "LogMark: logFileId - 35 , logFileOffset - 52");
    }

    @Test
    public void test0822() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0822");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        long long3 = logMark2.getLogFileOffset();
        long long4 = logMark2.getLogFileOffset();
        java.lang.String str5 = logMark2.toString();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 35L + "'", long4 == 35L);
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str5, "LogMark: logFileId - 0 , logFileOffset - 35");
    }

    @Test
    public void test0823() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0823");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long4 = logMark3.getLogFileOffset();
        long long5 = logMark3.getLogFileOffset();
        int int6 = logMark2.compare(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        long long11 = logMark9.getLogFileOffset();
        java.lang.String str12 = logMark9.toString();
        java.lang.String str13 = logMark9.toString();
        logMark9.setLogMark(35L, (long) (short) 100);
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        int int18 = logMark3.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        java.nio.ByteBuffer byteBuffer20 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark19.readLogMark(byteBuffer20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 10L + "'", long4 == 10L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 10L + "'", long5 == 10L);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 1 + "'", int6 == 1);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 0L + "'", long11 == 0L);
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str12, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str13, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + (-1) + "'", int18 == (-1));
    }

    @Test
    public void test0824() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0824");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark9 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long10 = logMark9.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        java.lang.String str12 = logMark11.toString();
        logMark11.setLogMark(32L, 9223372036854775807L);
        long long16 = logMark11.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int18 = logMark8.compare(logMark17);
        logMark17.setLogMark((long) (byte) 0, (long) (short) 100);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark9);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str12, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 32L + "'", long16 == 32L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
    }

    @Test
    public void test0825() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0825");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        long long24 = logMark13.getLogFileId();
        long long25 = logMark13.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 10L + "'", long25 == 10L);
    }

    @Test
    public void test0826() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0826");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        logMark2.setLogMark((long) (byte) 0, (long) (byte) 0);
        logMark2.setLogMark(0L, 97L);
        long long9 = logMark2.getLogFileId();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 0L + "'", long9 == 0L);
    }

    @Test
    public void test0827() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0827");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long6 = logMark3.getLogFileOffset();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
    }

    @Test
    public void test0828() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0828");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 0);
    }

    @Test
    public void test0829() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0829");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        long long11 = logMark9.getLogFileId();
        int int12 = logMark3.compare(logMark9);
        java.lang.String str13 = logMark9.toString();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        long long16 = logMark14.getLogFileOffset();
        long long17 = logMark14.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str13, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 0L + "'", long16 == 0L);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 0L + "'", long17 == 0L);
    }

    @Test
    public void test0830() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0830");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark((long) 'a', 35L);
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0831() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0831");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.lang.String str28 = logMark27.toString();
        logMark27.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark32 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        int int34 = logMark27.compare(logMark32);
        int int35 = logMark13.compare(logMark32);
        java.lang.String str36 = logMark13.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 1 + "'", int35 == 1);
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str36, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test0832() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0832");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long5 = logMark4.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        java.lang.String str7 = logMark6.toString();
        int int8 = logMark3.compare(logMark6);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark6.writeLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 10L + "'", long5 == 10L);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str7, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 1 + "'", int8 == 1);
    }

    @Test
    public void test0833() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0833");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        long long3 = logMark2.getLogFileId();
        logMark2.setLogMark((long) (short) 10, (long) (short) 10);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + (-1L) + "'", long3 == (-1L));
    }

    @Test
    public void test0834() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0834");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        logMark9.setLogMark(1L, (long) (byte) 1);
        long long14 = logMark9.getLogFileId();
        int int15 = logMark2.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark16 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark16);
        long long18 = logMark16.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark16);
        long long20 = logMark16.getLogFileOffset();
        java.lang.String str21 = logMark16.toString();
        logMark16.setLogMark((long) '#', 10L);
        int int25 = logMark2.compare(logMark16);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 1L + "'", long14 == 1L);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + (-1) + "'", int15 == (-1));
        org.junit.Assert.assertNotNull(logMark16);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 10L + "'", long18 == 10L);
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 10L + "'", long20 == 10L);
        org.junit.Assert.assertEquals("'" + str21 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str21, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + (-1) + "'", int25 == (-1));
    }

    @Test
    public void test0835() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0835");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        java.lang.String str11 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int19 = logMark15.compare(logMark18);
        logMark15.setLogMark((long) (short) 10, 10L);
        long long23 = logMark15.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int30 = logMark26.compare(logMark29);
        logMark26.setLogMark((long) (short) 10, 10L);
        long long34 = logMark26.getLogFileId();
        int int35 = logMark15.compare(logMark26);
        java.lang.String str36 = logMark26.toString();
        int int37 = logMark2.compare(logMark26);
        org.apache.bookkeeper.bookie.LogMark logMark40 = new org.apache.bookkeeper.bookie.LogMark((long) 100, (long) '#');
        logMark40.setLogMark((long) (byte) 10, (long) (byte) 100);
        long long44 = logMark40.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark45 = new org.apache.bookkeeper.bookie.LogMark(logMark40);
        int int46 = logMark26.compare(logMark45);
        java.lang.String str47 = logMark26.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str11, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 10L + "'", long34 == 10L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 0 + "'", int35 == 0);
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str36, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + 0 + "'", int37 == 0);
        org.junit.Assert.assertTrue("'" + long44 + "' != '" + 10L + "'", long44 == 10L);
        org.junit.Assert.assertTrue("'" + int46 + "' != '" + (-1) + "'", int46 == (-1));
        org.junit.Assert.assertEquals("'" + str47 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str47, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test0836() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0836");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int9 = logMark5.compare(logMark8);
        logMark5.setLogMark((long) (short) 10, 10L);
        long long13 = logMark5.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int20 = logMark16.compare(logMark19);
        logMark16.setLogMark((long) (short) 10, 10L);
        long long24 = logMark16.getLogFileId();
        int int25 = logMark5.compare(logMark16);
        java.lang.String str26 = logMark16.toString();
        int int27 = logMark2.compare(logMark16);
        long long28 = logMark16.getLogFileId();
        long long29 = logMark16.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark35 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int36 = logMark32.compare(logMark35);
        logMark32.setLogMark((long) (short) 10, 10L);
        long long40 = logMark32.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int47 = logMark43.compare(logMark46);
        logMark43.setLogMark((long) (short) 10, 10L);
        long long51 = logMark43.getLogFileId();
        int int52 = logMark32.compare(logMark43);
        long long53 = logMark32.getLogFileId();
        long long54 = logMark32.getLogFileOffset();
        long long55 = logMark32.getLogFileId();
        int int56 = logMark16.compare(logMark32);
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark(logMark16);
        java.lang.String str58 = logMark57.toString();
        long long59 = logMark57.getLogFileOffset();
        long long60 = logMark57.getLogFileId();
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 10L + "'", long29 == 10L);
        org.junit.Assert.assertTrue("'" + int36 + "' != '" + 0 + "'", int36 == 0);
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + 10L + "'", long40 == 10L);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long51 + "' != '" + 10L + "'", long51 == 10L);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 0 + "'", int52 == 0);
        org.junit.Assert.assertTrue("'" + long53 + "' != '" + 10L + "'", long53 == 10L);
        org.junit.Assert.assertTrue("'" + long54 + "' != '" + 10L + "'", long54 == 10L);
        org.junit.Assert.assertTrue("'" + long55 + "' != '" + 10L + "'", long55 == 10L);
        org.junit.Assert.assertTrue("'" + int56 + "' != '" + 0 + "'", int56 == 0);
        org.junit.Assert.assertEquals("'" + str58 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str58, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long59 + "' != '" + 10L + "'", long59 == 10L);
        org.junit.Assert.assertTrue("'" + long60 + "' != '" + 10L + "'", long60 == 10L);
    }

    @Test
    public void test0837() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0837");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        logMark9.setLogMark(1L, (long) (byte) 1);
        long long14 = logMark9.getLogFileId();
        int int15 = logMark2.compare(logMark9);
        long long16 = logMark9.getLogFileId();
        java.lang.Class<?> wildcardClass17 = logMark9.getClass();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 1L + "'", long14 == 1L);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + (-1) + "'", int15 == (-1));
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 1L + "'", long16 == 1L);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0838() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0838");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.lang.String str28 = logMark27.toString();
        logMark27.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark32 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        int int34 = logMark27.compare(logMark32);
        int int35 = logMark13.compare(logMark32);
        org.apache.bookkeeper.bookie.LogMark logMark38 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int42 = logMark38.compare(logMark41);
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark(logMark38);
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark(logMark43);
        int int45 = logMark13.compare(logMark44);
        long long46 = logMark13.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark49 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark50 = new org.apache.bookkeeper.bookie.LogMark(logMark49);
        java.lang.String str51 = logMark49.toString();
        int int52 = logMark13.compare(logMark49);
        long long53 = logMark49.getLogFileOffset();
        long long54 = logMark49.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark55 = new org.apache.bookkeeper.bookie.LogMark(logMark49);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + (-1) + "'", int45 == (-1));
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
        org.junit.Assert.assertEquals("'" + str51 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str51, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 1 + "'", int52 == 1);
        org.junit.Assert.assertTrue("'" + long53 + "' != '" + 35L + "'", long53 == 35L);
        org.junit.Assert.assertTrue("'" + long54 + "' != '" + 35L + "'", long54 == 35L);
    }

    @Test
    public void test0839() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0839");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 100, (long) '#');
        logMark2.setLogMark((long) (byte) 10, (long) (byte) 100);
        long long6 = logMark2.getLogFileId();
        logMark2.setLogMark(32L, 100L);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 10L + "'", long6 == 10L);
    }

    @Test
    public void test0840() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0840");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        java.lang.String str2 = logMark0.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        long long6 = logMark5.getLogFileId();
        int int7 = logMark0.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long9 = logMark8.getLogFileOffset();
        int int10 = logMark0.compare(logMark8);
        long long11 = logMark8.getLogFileOffset();
        long long12 = logMark8.getLogFileId();
        java.nio.ByteBuffer byteBuffer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.writeLogMark(byteBuffer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "57) test0840(Regression2Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
// flaky "41) test0840(Regression2Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark3);
// flaky "25) test0840(Regression2Test)":         org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
        org.junit.Assert.assertNotNull(logMark8);
// flaky "7) test0840(Regression2Test)":         org.junit.Assert.assertTrue("'" + long9 + "' != '" + 97L + "'", long9 == 97L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
// flaky "3) test0840(Regression2Test)":         org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
// flaky "1) test0840(Regression2Test)":         org.junit.Assert.assertTrue("'" + long12 + "' != '" + 100L + "'", long12 == 100L);
    }

    @Test
    public void test0841() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0841");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 0);
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        logMark6.setLogMark((long) (short) 10, 10L);
        long long14 = logMark6.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int21 = logMark17.compare(logMark20);
        logMark17.setLogMark((long) (short) 10, 10L);
        long long25 = logMark17.getLogFileId();
        int int26 = logMark6.compare(logMark17);
        java.lang.String str27 = logMark17.toString();
        java.lang.String str28 = logMark17.toString();
        logMark17.setLogMark(0L, (long) '#');
        int int32 = logMark2.compare(logMark17);
        logMark2.setLogMark((long) 10, (long) (short) -1);
        java.lang.String str36 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark39 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark42 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark45 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int46 = logMark42.compare(logMark45);
        logMark42.setLogMark((long) (short) 10, 10L);
        long long50 = logMark42.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark53 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark56 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int57 = logMark53.compare(logMark56);
        logMark53.setLogMark((long) (short) 10, 10L);
        long long61 = logMark53.getLogFileId();
        int int62 = logMark42.compare(logMark53);
        java.lang.String str63 = logMark53.toString();
        int int64 = logMark39.compare(logMark53);
        long long65 = logMark53.getLogFileId();
        long long66 = logMark53.getLogFileId();
        logMark53.setLogMark(1L, (long) (short) -1);
        logMark53.setLogMark(1L, 10L);
        int int73 = logMark2.compare(logMark53);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 10L + "'", long14 == 10L);
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + 0 + "'", int21 == 0);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 10L + "'", long25 == 10L);
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + 0 + "'", int26 == 0);
        org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str27, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str28, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 1 + "'", int32 == 1);
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - -1" + "'", str36, "LogMark: logFileId - 10 , logFileOffset - -1");
        org.junit.Assert.assertTrue("'" + int46 + "' != '" + 0 + "'", int46 == 0);
        org.junit.Assert.assertTrue("'" + long50 + "' != '" + 10L + "'", long50 == 10L);
        org.junit.Assert.assertTrue("'" + int57 + "' != '" + 0 + "'", int57 == 0);
        org.junit.Assert.assertTrue("'" + long61 + "' != '" + 10L + "'", long61 == 10L);
        org.junit.Assert.assertTrue("'" + int62 + "' != '" + 0 + "'", int62 == 0);
        org.junit.Assert.assertEquals("'" + str63 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str63, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int64 + "' != '" + (-1) + "'", int64 == (-1));
        org.junit.Assert.assertTrue("'" + long65 + "' != '" + 10L + "'", long65 == 10L);
        org.junit.Assert.assertTrue("'" + long66 + "' != '" + 10L + "'", long66 == 10L);
        org.junit.Assert.assertTrue("'" + int73 + "' != '" + 1 + "'", int73 == 1);
    }

    @Test
    public void test0842() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0842");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        logMark2.setLogMark((long) (byte) 0, (long) (byte) 0);
        logMark2.setLogMark((long) (byte) -1, (long) (byte) 0);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test0843() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0843");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int14 = logMark10.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        int int16 = logMark6.compare(logMark10);
        long long17 = logMark10.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((-1L), 10L);
        long long21 = logMark20.getLogFileOffset();
        int int22 = logMark10.compare(logMark20);
        logMark20.setLogMark((long) 0, (long) '4');
        java.lang.Class<?> wildcardClass26 = logMark20.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 97L + "'", long17 == 97L);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertNotNull(wildcardClass26);
    }

    @Test
    public void test0844() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0844");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        logMark2.setLogMark((long) (-1), 9223372036854775807L);
        long long6 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + (-1L) + "'", long6 == (-1L));
    }

    @Test
    public void test0845() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0845");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 32L);
    }

    @Test
    public void test0846() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0846");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        java.lang.String str8 = logMark7.toString();
        logMark7.setLogMark((long) (-1), (long) (byte) 100);
        long long12 = logMark7.getLogFileId();
        logMark7.setLogMark((long) '4', (long) '4');
        logMark7.setLogMark(35L, (long) 10);
        int int19 = logMark0.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        java.lang.String str21 = logMark20.toString();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "58) test0846(Regression2Test)":         org.junit.Assert.assertTrue("'" + long2 + "' != '" + 97L + "'", long2 == 97L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str8, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + (-1L) + "'", long12 == (-1L));
// flaky "42) test0846(Regression2Test)":         org.junit.Assert.assertTrue("'" + int19 + "' != '" + 1 + "'", int19 == 1);
        org.junit.Assert.assertEquals("'" + str21 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 10" + "'", str21, "LogMark: logFileId - 35 , logFileOffset - 10");
    }

    @Test
    public void test0847() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0847");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long4 = logMark3.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "59) test0847(Regression2Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
// flaky "43) test0847(Regression2Test)":         org.junit.Assert.assertTrue("'" + long4 + "' != '" + 97L + "'", long4 == 97L);
    }

    @Test
    public void test0848() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0848");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        java.lang.String str11 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int19 = logMark15.compare(logMark18);
        logMark15.setLogMark((long) (short) 10, 10L);
        long long23 = logMark15.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int30 = logMark26.compare(logMark29);
        logMark26.setLogMark((long) (short) 10, 10L);
        long long34 = logMark26.getLogFileId();
        int int35 = logMark15.compare(logMark26);
        java.lang.String str36 = logMark26.toString();
        int int37 = logMark2.compare(logMark26);
        long long38 = logMark26.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark42 = new org.apache.bookkeeper.bookie.LogMark(logMark41);
        long long43 = logMark41.getLogFileOffset();
        long long44 = logMark41.getLogFileOffset();
        int int45 = logMark26.compare(logMark41);
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark(logMark41);
        java.nio.ByteBuffer byteBuffer47 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark41.writeLogMark(byteBuffer47);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str11, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 10L + "'", long34 == 10L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 0 + "'", int35 == 0);
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str36, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + 0 + "'", int37 == 0);
        org.junit.Assert.assertTrue("'" + long38 + "' != '" + 10L + "'", long38 == 10L);
        org.junit.Assert.assertTrue("'" + long43 + "' != '" + 35L + "'", long43 == 35L);
        org.junit.Assert.assertTrue("'" + long44 + "' != '" + 35L + "'", long44 == 35L);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + 1 + "'", int45 == 1);
    }

    @Test
    public void test0849() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0849");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 10, (long) 100);
        long long3 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
    }

    @Test
    public void test0850() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0850");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), 10L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int14 = logMark10.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark15 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        int int17 = logMark10.compare(logMark16);
        logMark10.setLogMark((long) 0, (long) '#');
        long long21 = logMark10.getLogFileOffset();
        java.lang.String str22 = logMark10.toString();
        int int23 = logMark5.compare(logMark10);
        java.nio.ByteBuffer byteBuffer24 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.writeLogMark(byteBuffer24);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + (-1) + "'", int7 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertNotNull(logMark15);
// flaky "60) test0850(Regression2Test)":         org.junit.Assert.assertTrue("'" + int17 + "' != '" + (-1) + "'", int17 == (-1));
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 35L + "'", long21 == 35L);
        org.junit.Assert.assertEquals("'" + str22 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str22, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 0 + "'", int23 == 0);
    }

    @Test
    public void test0851() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0851");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        java.lang.String str23 = logMark13.toString();
        java.lang.String str24 = logMark13.toString();
        logMark13.setLogMark((long) (-1), 100L);
        java.nio.ByteBuffer byteBuffer28 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark13.readLogMark(byteBuffer28);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str23, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str24, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test0852() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0852");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int8 = logMark4.compare(logMark7);
        logMark4.setLogMark((long) (short) 10, 10L);
        long long12 = logMark4.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int19 = logMark15.compare(logMark18);
        logMark15.setLogMark((long) (short) 10, 10L);
        long long23 = logMark15.getLogFileId();
        int int24 = logMark4.compare(logMark15);
        java.lang.String str25 = logMark15.toString();
        java.lang.String str26 = logMark15.toString();
        logMark15.setLogMark((long) (-1), 100L);
        long long30 = logMark15.getLogFileOffset();
        int int31 = logMark0.compare(logMark15);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 10L + "'", long12 == 10L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertEquals("'" + str25 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str25, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 100L + "'", long30 == 100L);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 1 + "'", int31 == 1);
    }

    @Test
    public void test0853() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0853");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, 1L);
        long long3 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 1L + "'", long3 == 1L);
    }

    @Test
    public void test0854() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0854");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 0, (long) (byte) -1);
    }

    @Test
    public void test0855() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0855");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark16 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark16);
        java.lang.String str18 = logMark17.toString();
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        java.lang.String str23 = logMark22.toString();
        logMark22.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark27 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark27);
        int int29 = logMark22.compare(logMark27);
        int int30 = logMark17.compare(logMark27);
        int int31 = logMark15.compare(logMark27);
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark(logMark27);
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark27);
        long long34 = logMark33.getLogFileId();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "61) test0855(Regression2Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertNotNull(logMark16);
// flaky "44) test0855(Regression2Test)":         org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str18, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark27);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
// flaky "26) test0855(Regression2Test)":         org.junit.Assert.assertTrue("'" + long34 + "' != '" + 100L + "'", long34 == 100L);
    }

    @Test
    public void test0856() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0856");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        java.lang.String str9 = logMark7.toString();
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0857() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0857");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', (long) (byte) 10);
    }

    @Test
    public void test0858() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0858");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) -1, (long) (short) -1);
        long long3 = logMark2.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + (-1L) + "'", long3 == (-1L));
    }

    @Test
    public void test0859() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0859");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 0, 9223372036854775807L);
        java.lang.String str3 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 9223372036854775807" + "'", str3, "LogMark: logFileId - 0 , logFileOffset - 9223372036854775807");
    }

    @Test
    public void test0860() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0860");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) -1, 10L);
    }

    @Test
    public void test0861() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0861");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(35L, 32L);
        logMark2.setLogMark((long) (short) 10, (long) ' ');
    }

    @Test
    public void test0862() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0862");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        int int9 = logMark2.compare(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int16 = logMark12.compare(logMark15);
        logMark12.setLogMark((long) (short) 10, 10L);
        long long20 = logMark12.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int27 = logMark23.compare(logMark26);
        logMark23.setLogMark((long) (short) 10, 10L);
        long long31 = logMark23.getLogFileId();
        int int32 = logMark12.compare(logMark23);
        long long33 = logMark23.getLogFileOffset();
        long long34 = logMark23.getLogFileId();
        java.lang.String str35 = logMark23.toString();
        long long36 = logMark23.getLogFileId();
        int int37 = logMark2.compare(logMark23);
        logMark2.setLogMark((long) '#', (-1L));
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer42 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark41.readLogMark(byteBuffer42);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 0 + "'", int16 == 0);
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 10L + "'", long20 == 10L);
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 0 + "'", int27 == 0);
        org.junit.Assert.assertTrue("'" + long31 + "' != '" + 10L + "'", long31 == 10L);
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 0 + "'", int32 == 0);
        org.junit.Assert.assertTrue("'" + long33 + "' != '" + 10L + "'", long33 == 10L);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 10L + "'", long34 == 10L);
        org.junit.Assert.assertEquals("'" + str35 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str35, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 10L + "'", long36 == 10L);
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + 1 + "'", int37 == 1);
    }

    @Test
    public void test0863() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0863");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str4 = logMark3.toString();
        long long5 = logMark3.getLogFileId();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
// flaky "62) test0863(Regression2Test)":         org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str4, "LogMark: logFileId - -1 , logFileOffset - 10");
// flaky "45) test0863(Regression2Test)":         org.junit.Assert.assertTrue("'" + long5 + "' != '" + (-1L) + "'", long5 == (-1L));
    }

    @Test
    public void test0864() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0864");
        org.apache.bookkeeper.bookie.LogMark logMark0 = new org.apache.bookkeeper.bookie.LogMark();
        logMark0.setLogMark((long) (short) 1, (long) 0);
        logMark0.setLogMark((long) 'a', 1L);
    }

    @Test
    public void test0865() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0865");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        logMark8.setLogMark((long) (byte) 100, (long) (byte) 0);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0866() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0866");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, 52L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
    }

    @Test
    public void test0867() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0867");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, 35L);
        long long3 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
    }

    @Test
    public void test0868() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0868");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, (long) 'a');
        long long3 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 97L + "'", long3 == 97L);
    }

    @Test
    public void test0869() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0869");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', 1L);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0870() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0870");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark8.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int13 = logMark2.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        long long15 = logMark12.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark16 = null;
        // The following exception was thrown during execution in test generation
        try {
            int int17 = logMark12.compare(logMark16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 1L + "'", long15 == 1L);
    }

    @Test
    public void test0871() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0871");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        long long24 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark30 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int31 = logMark27.compare(logMark30);
        logMark27.setLogMark((long) (short) 10, 10L);
        long long35 = logMark27.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark38 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int42 = logMark38.compare(logMark41);
        logMark38.setLogMark((long) (short) 10, 10L);
        long long46 = logMark38.getLogFileId();
        int int47 = logMark27.compare(logMark38);
        long long48 = logMark27.getLogFileId();
        long long49 = logMark27.getLogFileOffset();
        long long50 = logMark27.getLogFileId();
        int int51 = logMark13.compare(logMark27);
        org.apache.bookkeeper.bookie.LogMark logMark52 = new org.apache.bookkeeper.bookie.LogMark(logMark27);
        long long53 = logMark52.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long48 + "' != '" + 10L + "'", long48 == 10L);
        org.junit.Assert.assertTrue("'" + long49 + "' != '" + 10L + "'", long49 == 10L);
        org.junit.Assert.assertTrue("'" + long50 + "' != '" + 10L + "'", long50 == 10L);
        org.junit.Assert.assertTrue("'" + int51 + "' != '" + 0 + "'", int51 == 0);
        org.junit.Assert.assertTrue("'" + long53 + "' != '" + 10L + "'", long53 == 10L);
    }

    @Test
    public void test0872() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0872");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark7.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        long long14 = logMark13.getLogFileId();
        java.lang.String str15 = logMark13.toString();
        java.lang.String str16 = logMark13.toString();
        long long17 = logMark13.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 97L + "'", long14 == 97L);
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str15, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str16 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str16, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 0L + "'", long17 == 0L);
    }

    @Test
    public void test0873() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0873");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), 10L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int14 = logMark10.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark15 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        int int17 = logMark10.compare(logMark16);
        logMark10.setLogMark((long) 0, (long) '#');
        long long21 = logMark10.getLogFileOffset();
        java.lang.String str22 = logMark10.toString();
        int int23 = logMark5.compare(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        org.apache.bookkeeper.bookie.LogMark logMark30 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark36 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int37 = logMark33.compare(logMark36);
        logMark33.setLogMark((long) (short) 10, 10L);
        long long41 = logMark33.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark47 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int48 = logMark44.compare(logMark47);
        logMark44.setLogMark((long) (short) 10, 10L);
        long long52 = logMark44.getLogFileId();
        int int53 = logMark33.compare(logMark44);
        java.lang.String str54 = logMark44.toString();
        int int55 = logMark30.compare(logMark44);
        org.apache.bookkeeper.bookie.LogMark logMark56 = new org.apache.bookkeeper.bookie.LogMark(logMark30);
        int int57 = logMark27.compare(logMark56);
        int int58 = logMark5.compare(logMark27);
        java.lang.String str59 = logMark5.toString();
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + (-1) + "'", int7 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertNotNull(logMark15);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 1 + "'", int17 == 1);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 35L + "'", long21 == 35L);
        org.junit.Assert.assertEquals("'" + str22 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str22, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 0 + "'", int23 == 0);
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + 0 + "'", int37 == 0);
        org.junit.Assert.assertTrue("'" + long41 + "' != '" + 10L + "'", long41 == 10L);
        org.junit.Assert.assertTrue("'" + int48 + "' != '" + 0 + "'", int48 == 0);
        org.junit.Assert.assertTrue("'" + long52 + "' != '" + 10L + "'", long52 == 10L);
        org.junit.Assert.assertTrue("'" + int53 + "' != '" + 0 + "'", int53 == 0);
        org.junit.Assert.assertEquals("'" + str54 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str54, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int55 + "' != '" + (-1) + "'", int55 == (-1));
        org.junit.Assert.assertTrue("'" + int57 + "' != '" + 1 + "'", int57 == 1);
        org.junit.Assert.assertTrue("'" + int58 + "' != '" + 0 + "'", int58 == 0);
        org.junit.Assert.assertEquals("'" + str59 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str59, "LogMark: logFileId - 0 , logFileOffset - 35");
    }

    @Test
    public void test0874() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0874");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        logMark9.setLogMark(1L, (long) (byte) 1);
        long long14 = logMark9.getLogFileId();
        int int15 = logMark2.compare(logMark9);
        java.lang.String str16 = logMark2.toString();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 1L + "'", long14 == 1L);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + (-1) + "'", int15 == (-1));
        org.junit.Assert.assertEquals("'" + str16 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 1" + "'", str16, "LogMark: logFileId - 0 , logFileOffset - 1");
    }

    @Test
    public void test0875() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0875");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int9 = logMark5.compare(logMark8);
        logMark5.setLogMark((long) (short) 10, 10L);
        long long13 = logMark5.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int20 = logMark16.compare(logMark19);
        logMark16.setLogMark((long) (short) 10, 10L);
        long long24 = logMark16.getLogFileId();
        int int25 = logMark5.compare(logMark16);
        java.lang.String str26 = logMark16.toString();
        int int27 = logMark2.compare(logMark16);
        long long28 = logMark16.getLogFileId();
        long long29 = logMark16.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark35 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int36 = logMark32.compare(logMark35);
        logMark32.setLogMark((long) (short) 10, 10L);
        long long40 = logMark32.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int47 = logMark43.compare(logMark46);
        logMark43.setLogMark((long) (short) 10, 10L);
        long long51 = logMark43.getLogFileId();
        int int52 = logMark32.compare(logMark43);
        long long53 = logMark32.getLogFileId();
        long long54 = logMark32.getLogFileOffset();
        long long55 = logMark32.getLogFileId();
        int int56 = logMark16.compare(logMark32);
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark(logMark16);
        java.lang.String str58 = logMark57.toString();
        org.apache.bookkeeper.bookie.LogMark logMark59 = new org.apache.bookkeeper.bookie.LogMark(logMark57);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 10L + "'", long29 == 10L);
        org.junit.Assert.assertTrue("'" + int36 + "' != '" + 0 + "'", int36 == 0);
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + 10L + "'", long40 == 10L);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long51 + "' != '" + 10L + "'", long51 == 10L);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 0 + "'", int52 == 0);
        org.junit.Assert.assertTrue("'" + long53 + "' != '" + 10L + "'", long53 == 10L);
        org.junit.Assert.assertTrue("'" + long54 + "' != '" + 10L + "'", long54 == 10L);
        org.junit.Assert.assertTrue("'" + long55 + "' != '" + 10L + "'", long55 == 10L);
        org.junit.Assert.assertTrue("'" + int56 + "' != '" + 0 + "'", int56 == 0);
        org.junit.Assert.assertEquals("'" + str58 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str58, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test0876() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0876");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + (-1L) + "'", long4 == (-1L));
    }

    @Test
    public void test0877() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0877");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(97L, (long) (short) 0);
        int int10 = logMark6.compare(logMark9);
        long long11 = logMark9.getLogFileOffset();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 0L + "'", long11 == 0L);
    }

    @Test
    public void test0878() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0878");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        logMark2.setLogMark(32L, 9223372036854775807L);
        long long7 = logMark2.getLogFileId();
        java.lang.Class<?> wildcardClass8 = logMark2.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
// flaky "63) test0878(Regression2Test)":         org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 32L + "'", long7 == 32L);
        org.junit.Assert.assertNotNull(wildcardClass8);
    }

    @Test
    public void test0879() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0879");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 0L);
    }

    @Test
    public void test0880() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0880");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        logMark2.setLogMark((long) ' ', (long) (byte) -1);
        long long8 = logMark2.getLogFileOffset();
        logMark2.setLogMark((long) (short) -1, 32L);
        logMark2.setLogMark((long) 0, (long) (short) -1);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
    }

    @Test
    public void test0881() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0881");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), (long) '#');
        logMark2.setLogMark((long) '#', 100L);
    }

    @Test
    public void test0882() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0882");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        logMark2.setLogMark(32L, 9223372036854775807L);
        long long7 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.writeLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
// flaky "64) test0882(Regression2Test)":         org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 32L + "'", long7 == 32L);
    }

    @Test
    public void test0883() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0883");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, (-1L));
    }

    @Test
    public void test0884() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0884");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) -1, (long) (short) -1);
        java.lang.String str3 = logMark2.toString();
        long long4 = logMark2.getLogFileId();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - -1" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - -1");
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + (-1L) + "'", long4 == (-1L));
    }

    @Test
    public void test0885() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0885");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long9 = logMark8.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        java.lang.String str11 = logMark10.toString();
        int int12 = logMark7.compare(logMark10);
        int int13 = logMark2.compare(logMark10);
        logMark10.setLogMark(32L, (long) ' ');
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int23 = logMark19.compare(logMark22);
        logMark19.setLogMark((long) (short) 10, 10L);
        long long27 = logMark19.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark30 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int34 = logMark30.compare(logMark33);
        logMark30.setLogMark((long) (short) 10, 10L);
        long long38 = logMark30.getLogFileId();
        int int39 = logMark19.compare(logMark30);
        int int40 = logMark10.compare(logMark19);
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 10L + "'", long9 == 10L);
// flaky "65) test0885(Regression2Test)":         org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str11, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 1 + "'", int12 == 1);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 0 + "'", int23 == 0);
        org.junit.Assert.assertTrue("'" + long27 + "' != '" + 10L + "'", long27 == 10L);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
        org.junit.Assert.assertTrue("'" + long38 + "' != '" + 10L + "'", long38 == 10L);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + 0 + "'", int39 == 0);
        org.junit.Assert.assertTrue("'" + int40 + "' != '" + 1 + "'", int40 == 1);
    }

    @Test
    public void test0886() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0886");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 0, (long) (short) -1);
        java.lang.String str3 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - -1" + "'", str3, "LogMark: logFileId - 0 , logFileOffset - -1");
    }

    @Test
    public void test0887() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0887");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 10, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.String str5 = logMark4.toString();
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        java.lang.String str10 = logMark9.toString();
        logMark9.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark14 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        int int16 = logMark9.compare(logMark14);
        int int17 = logMark4.compare(logMark14);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        org.apache.bookkeeper.bookie.LogMark logMark19 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark19);
        java.lang.String str21 = logMark20.toString();
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark(logMark24);
        java.lang.String str26 = logMark25.toString();
        logMark25.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark30 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark(logMark30);
        int int32 = logMark25.compare(logMark30);
        int int33 = logMark20.compare(logMark30);
        int int34 = logMark18.compare(logMark30);
        org.apache.bookkeeper.bookie.LogMark logMark35 = new org.apache.bookkeeper.bookie.LogMark(logMark18);
        long long36 = logMark35.getLogFileOffset();
        int int37 = logMark2.compare(logMark35);
        logMark35.setLogMark((long) (byte) -1, (long) 'a');
        org.junit.Assert.assertNotNull(logMark3);
// flaky "66) test0887(Regression2Test)":         org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str5, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str10, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark14);
// flaky "46) test0887(Regression2Test)":         org.junit.Assert.assertTrue("'" + int16 + "' != '" + 0 + "'", int16 == 0);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertNotNull(logMark19);
// flaky "27) test0887(Regression2Test)":         org.junit.Assert.assertEquals("'" + str21 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str21, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str26, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark30);
// flaky "8) test0887(Regression2Test)":         org.junit.Assert.assertTrue("'" + int32 + "' != '" + 0 + "'", int32 == 0);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 0 + "'", int33 == 0);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 10L + "'", long36 == 10L);
// flaky "4) test0887(Regression2Test)":         org.junit.Assert.assertTrue("'" + int37 + "' != '" + 1 + "'", int37 == 1);
    }

    @Test
    public void test0888() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0888");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        logMark2.setLogMark((long) 0, (long) '#');
        long long13 = logMark2.getLogFileOffset();
        long long14 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer15 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 35L + "'", long13 == 35L);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 0L + "'", long14 == 0L);
    }

    @Test
    public void test0889() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0889");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
    }

    @Test
    public void test0890() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0890");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        java.lang.String str3 = logMark2.toString();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str3, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0891() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0891");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, (long) 100);
    }

    @Test
    public void test0892() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0892");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark2.setLogMark((long) (byte) 100, (long) (short) 0);
        logMark2.setLogMark(32L, 9223372036854775807L);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0893() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0893");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        java.lang.String str11 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int19 = logMark15.compare(logMark18);
        logMark15.setLogMark((long) (short) 10, 10L);
        long long23 = logMark15.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int30 = logMark26.compare(logMark29);
        logMark26.setLogMark((long) (short) 10, 10L);
        long long34 = logMark26.getLogFileId();
        int int35 = logMark15.compare(logMark26);
        java.lang.String str36 = logMark26.toString();
        int int37 = logMark2.compare(logMark26);
        long long38 = logMark26.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark42 = new org.apache.bookkeeper.bookie.LogMark(logMark41);
        long long43 = logMark41.getLogFileOffset();
        long long44 = logMark41.getLogFileOffset();
        int int45 = logMark26.compare(logMark41);
        java.nio.ByteBuffer byteBuffer46 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark41.writeLogMark(byteBuffer46);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str11, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 10L + "'", long34 == 10L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 0 + "'", int35 == 0);
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str36, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + 0 + "'", int37 == 0);
        org.junit.Assert.assertTrue("'" + long38 + "' != '" + 10L + "'", long38 == 10L);
        org.junit.Assert.assertTrue("'" + long43 + "' != '" + 35L + "'", long43 == 35L);
        org.junit.Assert.assertTrue("'" + long44 + "' != '" + 35L + "'", long44 == 35L);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + 1 + "'", int45 == 1);
    }

    @Test
    public void test0894() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0894");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        java.lang.String str14 = logMark12.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
// flaky "67) test0894(Regression2Test)":         org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 52" + "'", str10, "LogMark: logFileId - 1 , logFileOffset - 52");
// flaky "47) test0894(Regression2Test)":         org.junit.Assert.assertEquals("'" + str14 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 52" + "'", str14, "LogMark: logFileId - 1 , logFileOffset - 52");
    }

    @Test
    public void test0895() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0895");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        logMark2.setLogMark((long) 1, (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int19 = logMark15.compare(logMark18);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        int int21 = logMark2.compare(logMark15);
        long long22 = logMark2.getLogFileId();
        logMark2.setLogMark((long) 0, (long) (byte) 10);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + (-1) + "'", int21 == (-1));
        org.junit.Assert.assertTrue("'" + long22 + "' != '" + 1L + "'", long22 == 1L);
    }

    @Test
    public void test0896() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0896");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        long long4 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "68) test0896(Regression2Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 52L + "'", long1 == 52L);
// flaky "48) test0896(Regression2Test)":         org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 52" + "'", str3, "LogMark: logFileId - 1 , logFileOffset - 52");
// flaky "28) test0896(Regression2Test)":         org.junit.Assert.assertTrue("'" + long4 + "' != '" + 1L + "'", long4 == 1L);
    }

    @Test
    public void test0897() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0897");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        java.lang.String str9 = logMark8.toString();
        logMark8.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark13 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        int int15 = logMark8.compare(logMark13);
        int int16 = logMark3.compare(logMark13);
        int int17 = logMark1.compare(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int24 = logMark20.compare(logMark23);
        org.apache.bookkeeper.bookie.LogMark logMark25 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark25);
        int int27 = logMark20.compare(logMark26);
        logMark20.setLogMark((long) 0, (long) '#');
        java.lang.String str31 = logMark20.toString();
        int int32 = logMark3.compare(logMark20);
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertNotNull(logMark2);
// flaky "69) test0897(Regression2Test)":         org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 52" + "'", str4, "LogMark: logFileId - 1 , logFileOffset - 52");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark13);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + (-1) + "'", int15 == (-1));
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 0 + "'", int16 == 0);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertNotNull(logMark25);
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
        org.junit.Assert.assertEquals("'" + str31 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str31, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 1 + "'", int32 == 1);
    }

    @Test
    public void test0898() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0898");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 0, (-1L));
        long long3 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + (-1L) + "'", long3 == (-1L));
    }

    @Test
    public void test0899() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0899");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        java.lang.String str8 = logMark5.toString();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark9.readLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str8, "LogMark: logFileId - -1 , logFileOffset - 0");
    }

    @Test
    public void test0900() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0900");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark5);
        logMark2.setLogMark(0L, 9223372036854775807L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
    }

    @Test
    public void test0901() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0901");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark(0L, (long) (byte) 0);
        logMark3.setLogMark((long) '4', (long) (byte) -1);
        long long11 = logMark3.getLogFileId();
        long long12 = logMark3.getLogFileOffset();
        java.lang.String str13 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark16);
        long long18 = logMark16.getLogFileOffset();
        java.lang.String str19 = logMark16.toString();
        java.lang.String str20 = logMark16.toString();
        logMark16.setLogMark(35L, (long) (short) 100);
        long long24 = logMark16.getLogFileId();
        int int25 = logMark3.compare(logMark16);
        org.junit.Assert.assertNotNull(logMark0);
// flaky "70) test0901(Regression2Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 52L + "'", long1 == 52L);
// flaky "49) test0901(Regression2Test)":         org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 52" + "'", str4, "LogMark: logFileId - 1 , logFileOffset - 52");
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 52L + "'", long11 == 52L);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + (-1L) + "'", long12 == (-1L));
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - -1" + "'", str13, "LogMark: logFileId - 52 , logFileOffset - -1");
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 0L + "'", long18 == 0L);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str19, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str20 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str20, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 35L + "'", long24 == 35L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 1 + "'", int25 == 1);
    }

    @Test
    public void test0902() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0902");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) 1);
    }

    @Test
    public void test0903() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0903");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) 0);
    }

    @Test
    public void test0904() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0904");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        long long7 = logMark6.getLogFileId();
        long long8 = logMark6.getLogFileId();
        long long9 = logMark6.getLogFileId();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 97L + "'", long8 == 97L);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 97L + "'", long9 == 97L);
    }

    @Test
    public void test0905() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0905");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark7.setLogMark((long) (short) 100, 35L);
        long long11 = logMark7.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark7.writeLogMark(byteBuffer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 35L + "'", long11 == 35L);
    }

    @Test
    public void test0906() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0906");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark5);
        logMark2.setLogMark(0L, 9223372036854775807L);
        long long11 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 9223372036854775807L + "'", long11 == 9223372036854775807L);
    }

    @Test
    public void test0907() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0907");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, (long) (short) 0);
        int int8 = logMark3.compare(logMark7);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 35L + "'", long4 == 35L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
    }

    @Test
    public void test0908() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0908");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        java.lang.String str3 = logMark2.toString();
        java.lang.String str4 = logMark2.toString();
        java.lang.String str5 = logMark2.toString();
        logMark2.setLogMark((long) (short) 0, 52L);
        logMark2.setLogMark((long) (byte) -1, (long) 0);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str3, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str4, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str5, "LogMark: logFileId - 1 , logFileOffset - 35");
    }

    @Test
    public void test0909() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0909");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) (byte) 10);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) 10);
        java.lang.String str8 = logMark7.toString();
        int int9 = logMark3.compare(logMark7);
        java.lang.Class<?> wildcardClass10 = logMark3.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 97L + "'", long4 == 97L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 10" + "'", str8, "LogMark: logFileId - 0 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertNotNull(wildcardClass10);
    }

    @Test
    public void test0910() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0910");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        long long8 = logMark3.getLogFileId();
        long long9 = logMark3.getLogFileOffset();
        java.lang.String str10 = logMark3.toString();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 10L + "'", long9 == 10L);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str10, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test0911() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0911");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark8.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int13 = logMark2.compare(logMark12);
        long long14 = logMark12.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark((-1L), (long) '4');
        int int18 = logMark12.compare(logMark17);
        long long19 = logMark12.getLogFileOffset();
        long long20 = logMark12.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark(logMark23);
        java.lang.String str25 = logMark24.toString();
        logMark24.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark29 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark30 = new org.apache.bookkeeper.bookie.LogMark(logMark29);
        int int31 = logMark24.compare(logMark29);
        org.apache.bookkeeper.bookie.LogMark logMark34 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark37 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int38 = logMark34.compare(logMark37);
        int int39 = logMark24.compare(logMark37);
        long long40 = logMark24.getLogFileId();
        int int41 = logMark12.compare(logMark24);
        org.apache.bookkeeper.bookie.LogMark logMark42 = new org.apache.bookkeeper.bookie.LogMark(logMark24);
        org.apache.bookkeeper.bookie.LogMark logMark43 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark(logMark43);
        org.apache.bookkeeper.bookie.LogMark logMark45 = new org.apache.bookkeeper.bookie.LogMark(logMark44);
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark(logMark44);
        int int47 = logMark42.compare(logMark44);
        java.lang.String str48 = logMark44.toString();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 1L + "'", long14 == 1L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 35L + "'", long19 == 35L);
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 35L + "'", long20 == 35L);
        org.junit.Assert.assertEquals("'" + str25 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str25, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark29);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + (-1) + "'", int31 == (-1));
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + 0 + "'", int38 == 0);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + (-1) + "'", int39 == (-1));
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + (-1L) + "'", long40 == (-1L));
        org.junit.Assert.assertTrue("'" + int41 + "' != '" + 1 + "'", int41 == 1);
        org.junit.Assert.assertNotNull(logMark43);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + (-1) + "'", int47 == (-1));
// flaky "71) test0911(Regression2Test)":         org.junit.Assert.assertEquals("'" + str48 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 52" + "'", str48, "LogMark: logFileId - 1 , logFileOffset - 52");
    }

    @Test
    public void test0912() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0912");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        int int11 = logMark5.compare(logMark10);
        java.lang.String str12 = logMark5.toString();
        logMark5.setLogMark((long) 10, 35L);
        int int16 = logMark0.compare(logMark5);
        java.lang.String str17 = logMark0.toString();
        java.nio.ByteBuffer byteBuffer18 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.writeLogMark(byteBuffer18);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "72) test0912(Regression2Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 52L + "'", long1 == 52L);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + (-1) + "'", int11 == (-1));
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str12, "LogMark: logFileId - 0 , logFileOffset - 35");
// flaky "50) test0912(Regression2Test)":         org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
// flaky "29) test0912(Regression2Test)":         org.junit.Assert.assertEquals("'" + str17 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 52" + "'", str17, "LogMark: logFileId - 1 , logFileOffset - 52");
    }

    @Test
    public void test0913() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0913");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 0);
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        logMark6.setLogMark((long) (short) 10, 10L);
        long long14 = logMark6.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int21 = logMark17.compare(logMark20);
        logMark17.setLogMark((long) (short) 10, 10L);
        long long25 = logMark17.getLogFileId();
        int int26 = logMark6.compare(logMark17);
        java.lang.String str27 = logMark17.toString();
        java.lang.String str28 = logMark17.toString();
        logMark17.setLogMark(0L, (long) '#');
        int int32 = logMark2.compare(logMark17);
        logMark2.setLogMark((long) 10, (long) (short) -1);
        java.lang.String str36 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark39 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark42 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long43 = logMark42.getLogFileOffset();
        int int44 = logMark39.compare(logMark42);
        java.lang.String str45 = logMark42.toString();
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark(logMark42);
        java.lang.String str47 = logMark46.toString();
        logMark46.setLogMark(32L, (long) (byte) 1);
        int int51 = logMark2.compare(logMark46);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 10L + "'", long14 == 10L);
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + 0 + "'", int21 == 0);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 10L + "'", long25 == 10L);
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + 0 + "'", int26 == 0);
        org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str27, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str28, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 1 + "'", int32 == 1);
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - -1" + "'", str36, "LogMark: logFileId - 10 , logFileOffset - -1");
        org.junit.Assert.assertTrue("'" + long43 + "' != '" + 0L + "'", long43 == 0L);
        org.junit.Assert.assertTrue("'" + int44 + "' != '" + 1 + "'", int44 == 1);
        org.junit.Assert.assertEquals("'" + str45 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str45, "LogMark: logFileId - -1 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str47 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str47, "LogMark: logFileId - -1 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int51 + "' != '" + (-1) + "'", int51 == (-1));
    }

    @Test
    public void test0914() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0914");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long6 = logMark5.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int13 = logMark9.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark14 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        int int16 = logMark9.compare(logMark15);
        int int17 = logMark5.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark18);
        logMark18.setLogMark(10L, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark23 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long24 = logMark23.getLogFileOffset();
        java.lang.String str25 = logMark23.toString();
        org.apache.bookkeeper.bookie.LogMark logMark26 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark27);
        long long29 = logMark28.getLogFileId();
        int int30 = logMark23.compare(logMark28);
        logMark28.setLogMark((long) (byte) 100, (long) (byte) 100);
        int int34 = logMark18.compare(logMark28);
        org.apache.bookkeeper.bookie.LogMark logMark35 = new org.apache.bookkeeper.bookie.LogMark(logMark28);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertNotNull(logMark14);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertNotNull(logMark23);
// flaky "73) test0914(Regression2Test)":         org.junit.Assert.assertTrue("'" + long24 + "' != '" + 52L + "'", long24 == 52L);
// flaky "51) test0914(Regression2Test)":         org.junit.Assert.assertEquals("'" + str25 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 52" + "'", str25, "LogMark: logFileId - 1 , logFileOffset - 52");
        org.junit.Assert.assertNotNull(logMark26);
// flaky "30) test0914(Regression2Test)":         org.junit.Assert.assertTrue("'" + long29 + "' != '" + 1L + "'", long29 == 1L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
    }

    @Test
    public void test0915() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0915");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        long long8 = logMark6.getLogFileOffset();
        logMark6.setLogMark((long) 100, (long) 'a');
        org.apache.bookkeeper.bookie.LogMark logMark12 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long13 = logMark12.getLogFileOffset();
        int int14 = logMark6.compare(logMark12);
        long long15 = logMark12.getLogFileOffset();
        int int16 = logMark5.compare(logMark12);
        long long17 = logMark12.getLogFileId();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark6);
// flaky "74) test0915(Regression2Test)":         org.junit.Assert.assertTrue("'" + long8 + "' != '" + 52L + "'", long8 == 52L);
        org.junit.Assert.assertNotNull(logMark12);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 97L + "'", long13 == 97L);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 97L + "'", long15 == 97L);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 100L + "'", long17 == 100L);
    }

    @Test
    public void test0916() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0916");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        java.nio.ByteBuffer byteBuffer16 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark15.writeLogMark(byteBuffer16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
    }

    @Test
    public void test0917() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0917");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(1L, (long) 10);
    }

    @Test
    public void test0918() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0918");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        logMark3.setLogMark(0L, 0L);
        java.lang.Class<?> wildcardClass12 = logMark3.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertNotNull(wildcardClass12);
    }

    @Test
    public void test0919() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0919");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        java.lang.String str2 = logMark0.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        long long6 = logMark5.getLogFileId();
        int int7 = logMark0.compare(logMark5);
        logMark5.setLogMark((long) (byte) 100, (long) (byte) 100);
        logMark5.setLogMark((long) (short) -1, (long) (-1));
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
    }

    @Test
    public void test0920() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0920");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        logMark3.setLogMark((long) 100, 100L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
    }

    @Test
    public void test0921() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0921");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        java.lang.String str11 = logMark10.toString();
        logMark10.setLogMark((long) (-1), (long) (byte) 100);
        logMark10.setLogMark((long) ' ', 9223372036854775807L);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int24 = logMark20.compare(logMark23);
        org.apache.bookkeeper.bookie.LogMark logMark25 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark25);
        int int27 = logMark20.compare(logMark26);
        int int28 = logMark10.compare(logMark26);
        long long29 = logMark26.getLogFileId();
        long long30 = logMark26.getLogFileOffset();
        int int31 = logMark2.compare(logMark26);
        java.lang.String str32 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str11, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertNotNull(logMark25);
// flaky "75) test0921(Regression2Test)":         org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
// flaky "52) test0921(Regression2Test)":         org.junit.Assert.assertTrue("'" + int28 + "' != '" + 1 + "'", int28 == 1);
// flaky "31) test0921(Regression2Test)":         org.junit.Assert.assertTrue("'" + long29 + "' != '" + (-1L) + "'", long29 == (-1L));
// flaky "9) test0921(Regression2Test)":         org.junit.Assert.assertTrue("'" + long30 + "' != '" + 10L + "'", long30 == 10L);
// flaky "5) test0921(Regression2Test)":         org.junit.Assert.assertTrue("'" + int31 + "' != '" + 1 + "'", int31 == 1);
        org.junit.Assert.assertEquals("'" + str32 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str32, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0922() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0922");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark7.setLogMark((long) 10, (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        logMark14.setLogMark(100L, (long) '4');
        long long18 = logMark14.getLogFileOffset();
        int int19 = logMark7.compare(logMark14);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        long long21 = logMark14.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 52L + "'", long18 == 52L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 100L + "'", long21 == 100L);
    }

    @Test
    public void test0923() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0923");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 0, (long) (short) 0);
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0924() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0924");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', 0L);
        long long3 = logMark2.getLogFileId();
        java.lang.String str4 = logMark2.toString();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 32 , logFileOffset - 0");
    }

    @Test
    public void test0925() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0925");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark(0L, (long) (byte) 0);
        long long8 = logMark3.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.junit.Assert.assertNotNull(logMark0);
// flaky "76) test0925(Regression2Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
// flaky "53) test0925(Regression2Test)":         org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str4, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
    }

    @Test
    public void test0926() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0926");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, 0L);
        logMark2.setLogMark((long) (byte) 0, (long) (byte) 100);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark6.setLogMark((long) 'a', 32L);
    }

    @Test
    public void test0927() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0927");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long8 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int16 = logMark12.compare(logMark15);
        logMark12.setLogMark((long) (short) 10, 10L);
        long long20 = logMark12.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int27 = logMark23.compare(logMark26);
        logMark23.setLogMark((long) (short) 10, 10L);
        long long31 = logMark23.getLogFileId();
        int int32 = logMark12.compare(logMark23);
        long long33 = logMark23.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark36 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark37 = new org.apache.bookkeeper.bookie.LogMark(logMark36);
        java.lang.String str38 = logMark37.toString();
        logMark37.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark42 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark(logMark42);
        int int44 = logMark37.compare(logMark42);
        int int45 = logMark23.compare(logMark42);
        org.apache.bookkeeper.bookie.LogMark logMark48 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark51 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int52 = logMark48.compare(logMark51);
        org.apache.bookkeeper.bookie.LogMark logMark53 = new org.apache.bookkeeper.bookie.LogMark(logMark48);
        org.apache.bookkeeper.bookie.LogMark logMark54 = new org.apache.bookkeeper.bookie.LogMark(logMark53);
        int int55 = logMark23.compare(logMark54);
        long long56 = logMark23.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark59 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark60 = new org.apache.bookkeeper.bookie.LogMark(logMark59);
        java.lang.String str61 = logMark59.toString();
        int int62 = logMark23.compare(logMark59);
        java.lang.String str63 = logMark59.toString();
        org.apache.bookkeeper.bookie.LogMark logMark64 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark65 = new org.apache.bookkeeper.bookie.LogMark(logMark64);
        int int66 = logMark59.compare(logMark64);
        java.lang.String str67 = logMark64.toString();
        int int68 = logMark9.compare(logMark64);
        java.nio.ByteBuffer byteBuffer69 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark64.readLogMark(byteBuffer69);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 0 + "'", int16 == 0);
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 10L + "'", long20 == 10L);
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 0 + "'", int27 == 0);
        org.junit.Assert.assertTrue("'" + long31 + "' != '" + 10L + "'", long31 == 10L);
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 0 + "'", int32 == 0);
        org.junit.Assert.assertTrue("'" + long33 + "' != '" + 10L + "'", long33 == 10L);
        org.junit.Assert.assertEquals("'" + str38 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str38, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark42);
// flaky "77) test0927(Regression2Test)":         org.junit.Assert.assertTrue("'" + int44 + "' != '" + 0 + "'", int44 == 0);
// flaky "54) test0927(Regression2Test)":         org.junit.Assert.assertTrue("'" + int45 + "' != '" + 1 + "'", int45 == 1);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 0 + "'", int52 == 0);
        org.junit.Assert.assertTrue("'" + int55 + "' != '" + (-1) + "'", int55 == (-1));
        org.junit.Assert.assertTrue("'" + long56 + "' != '" + 10L + "'", long56 == 10L);
        org.junit.Assert.assertEquals("'" + str61 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str61, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int62 + "' != '" + 1 + "'", int62 == 1);
        org.junit.Assert.assertEquals("'" + str63 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str63, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int66 + "' != '" + 1 + "'", int66 == 1);
        org.junit.Assert.assertEquals("'" + str67 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str67, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int68 + "' != '" + 1 + "'", int68 == 1);
    }

    @Test
    public void test0928() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0928");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int9 = logMark5.compare(logMark8);
        logMark5.setLogMark((long) (short) 10, 10L);
        long long13 = logMark5.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int20 = logMark16.compare(logMark19);
        logMark16.setLogMark((long) (short) 10, 10L);
        long long24 = logMark16.getLogFileId();
        int int25 = logMark5.compare(logMark16);
        java.lang.String str26 = logMark16.toString();
        int int27 = logMark2.compare(logMark16);
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark16);
        java.lang.Class<?> wildcardClass29 = logMark28.getClass();
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass29);
    }

    @Test
    public void test0929() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0929");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        java.lang.String str11 = logMark10.toString();
        logMark10.setLogMark((long) (-1), (long) (byte) 100);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        int int16 = logMark6.compare(logMark15);
        int int17 = logMark2.compare(logMark15);
        java.nio.ByteBuffer byteBuffer18 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark15.readLogMark(byteBuffer18);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str11, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 1 + "'", int17 == 1);
    }

    @Test
    public void test0930() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0930");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        long long7 = logMark5.getLogFileId();
        long long8 = logMark5.getLogFileId();
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 97L + "'", long8 == 97L);
    }

    @Test
    public void test0931() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0931");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long3 = logMark2.getLogFileId();
        logMark2.setLogMark((long) 'a', (long) (-1));
        org.junit.Assert.assertNotNull(logMark0);
// flaky "78) test0931(Regression2Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
// flaky "55) test0931(Regression2Test)":         org.junit.Assert.assertTrue("'" + long3 + "' != '" + (-1L) + "'", long3 == (-1L));
    }

    @Test
    public void test0932() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0932");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        logMark2.setLogMark((long) (-1), 9223372036854775807L);
        java.lang.String str6 = logMark2.toString();
        logMark2.setLogMark(35L, (long) (byte) 10);
        logMark2.setLogMark((long) (byte) 10, (long) (byte) 0);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(32L, (long) 'a');
        int int16 = logMark2.compare(logMark15);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807" + "'", str6, "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807");
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
    }

    @Test
    public void test0933() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0933");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), 10L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark12 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long13 = logMark12.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        java.lang.String str15 = logMark14.toString();
        int int16 = logMark11.compare(logMark14);
        int int17 = logMark6.compare(logMark14);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int24 = logMark20.compare(logMark23);
        logMark20.setLogMark((long) (short) 10, 10L);
        long long28 = logMark20.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark34 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int35 = logMark31.compare(logMark34);
        logMark31.setLogMark((long) (short) 10, 10L);
        long long39 = logMark31.getLogFileId();
        int int40 = logMark20.compare(logMark31);
        java.lang.String str41 = logMark31.toString();
        java.lang.String str42 = logMark31.toString();
        int int43 = logMark6.compare(logMark31);
        int int44 = logMark2.compare(logMark31);
        long long45 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 10L + "'", long3 == 10L);
        org.junit.Assert.assertNotNull(logMark12);
// flaky "79) test0933(Regression2Test)":         org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
// flaky "56) test0933(Regression2Test)":         org.junit.Assert.assertEquals("'" + str15 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str15, "LogMark: logFileId - -1 , logFileOffset - 10");
// flaky "32) test0933(Regression2Test)":         org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
// flaky "10) test0933(Regression2Test)":         org.junit.Assert.assertTrue("'" + int17 + "' != '" + 1 + "'", int17 == 1);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 0 + "'", int35 == 0);
        org.junit.Assert.assertTrue("'" + long39 + "' != '" + 10L + "'", long39 == 10L);
        org.junit.Assert.assertTrue("'" + int40 + "' != '" + 0 + "'", int40 == 0);
        org.junit.Assert.assertEquals("'" + str41 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str41, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str42 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str42, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int43 + "' != '" + 1 + "'", int43 == 1);
        org.junit.Assert.assertTrue("'" + int44 + "' != '" + (-1) + "'", int44 == (-1));
        org.junit.Assert.assertTrue("'" + long45 + "' != '" + 10L + "'", long45 == 10L);
    }

    @Test
    public void test0934() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0934");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(35L, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int9 = logMark5.compare(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        logMark10.setLogMark((long) (short) 100, 35L);
        logMark10.setLogMark(32L, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark20 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long21 = logMark20.getLogFileOffset();
        long long22 = logMark20.getLogFileOffset();
        int int23 = logMark19.compare(logMark20);
        logMark19.setLogMark((long) (byte) 1, (long) 'a');
        int int27 = logMark10.compare(logMark19);
        int int28 = logMark2.compare(logMark10);
        java.nio.ByteBuffer byteBuffer29 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark10.readLogMark(byteBuffer29);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertNotNull(logMark20);
// flaky "80) test0934(Regression2Test)":         org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
// flaky "57) test0934(Regression2Test)":         org.junit.Assert.assertTrue("'" + long22 + "' != '" + 10L + "'", long22 == 10L);
// flaky "33) test0934(Regression2Test)":         org.junit.Assert.assertTrue("'" + int23 + "' != '" + 1 + "'", int23 == 1);
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
        org.junit.Assert.assertTrue("'" + int28 + "' != '" + 1 + "'", int28 == 1);
    }

    @Test
    public void test0935() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0935");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark(100L, (long) '4');
        long long7 = logMark3.getLogFileOffset();
        long long8 = logMark3.getLogFileId();
        java.lang.String str9 = logMark3.toString();
        long long10 = logMark3.getLogFileId();
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 52L + "'", long7 == 52L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 100L + "'", long8 == 100L);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 52" + "'", str9, "LogMark: logFileId - 100 , logFileOffset - 52");
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 100L + "'", long10 == 100L);
    }

    @Test
    public void test0936() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0936");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, (long) ' ');
    }

    @Test
    public void test0937() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0937");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 100, (long) 100);
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0938() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0938");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) -1, (long) 'a');
    }

    @Test
    public void test0939() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0939");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        logMark8.setLogMark((long) 10, (long) (-1));
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) (short) 10);
        int int15 = logMark8.compare(logMark14);
        logMark14.setLogMark(1L, 1L);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + (-1) + "'", int15 == (-1));
    }

    @Test
    public void test0940() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0940");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        java.lang.String str9 = logMark8.toString();
        java.lang.String str10 = logMark8.toString();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int28 = logMark24.compare(logMark27);
        logMark24.setLogMark((long) (short) 10, 10L);
        long long32 = logMark24.getLogFileId();
        int int33 = logMark13.compare(logMark24);
        long long34 = logMark24.getLogFileOffset();
        long long35 = logMark24.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark38 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int42 = logMark38.compare(logMark41);
        logMark38.setLogMark((long) (short) 10, 10L);
        long long46 = logMark38.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark49 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark52 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int53 = logMark49.compare(logMark52);
        logMark49.setLogMark((long) (short) 10, 10L);
        long long57 = logMark49.getLogFileId();
        int int58 = logMark38.compare(logMark49);
        long long59 = logMark38.getLogFileId();
        long long60 = logMark38.getLogFileOffset();
        long long61 = logMark38.getLogFileId();
        int int62 = logMark24.compare(logMark38);
        org.apache.bookkeeper.bookie.LogMark logMark63 = new org.apache.bookkeeper.bookie.LogMark(logMark38);
        int int64 = logMark8.compare(logMark63);
        org.apache.bookkeeper.bookie.LogMark logMark65 = new org.apache.bookkeeper.bookie.LogMark(logMark63);
        logMark65.setLogMark((long) (byte) 1, 0L);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str10, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int28 + "' != '" + 0 + "'", int28 == 0);
        org.junit.Assert.assertTrue("'" + long32 + "' != '" + 10L + "'", long32 == 10L);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 0 + "'", int33 == 0);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 10L + "'", long34 == 10L);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
        org.junit.Assert.assertTrue("'" + int53 + "' != '" + 0 + "'", int53 == 0);
        org.junit.Assert.assertTrue("'" + long57 + "' != '" + 10L + "'", long57 == 10L);
        org.junit.Assert.assertTrue("'" + int58 + "' != '" + 0 + "'", int58 == 0);
        org.junit.Assert.assertTrue("'" + long59 + "' != '" + 10L + "'", long59 == 10L);
        org.junit.Assert.assertTrue("'" + long60 + "' != '" + 10L + "'", long60 == 10L);
        org.junit.Assert.assertTrue("'" + long61 + "' != '" + 10L + "'", long61 == 10L);
        org.junit.Assert.assertTrue("'" + int62 + "' != '" + 0 + "'", int62 == 0);
        org.junit.Assert.assertTrue("'" + int64 + "' != '" + 1 + "'", int64 == 1);
    }

    @Test
    public void test0941() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0941");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(52L, 0L);
        java.lang.String str3 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 0" + "'", str3, "LogMark: logFileId - 52 , logFileOffset - 0");
    }

    @Test
    public void test0942() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0942");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark9 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long10 = logMark9.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        java.lang.String str12 = logMark11.toString();
        logMark11.setLogMark(32L, 9223372036854775807L);
        long long16 = logMark11.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int18 = logMark8.compare(logMark17);
        java.lang.Class<?> wildcardClass19 = logMark17.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark9);
// flaky "81) test0942(Regression2Test)":         org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
// flaky "58) test0942(Regression2Test)":         org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str12, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 32L + "'", long16 == 32L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0943() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0943");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        logMark8.setLogMark(10L, 0L);
        logMark8.setLogMark((long) 'a', 0L);
        java.nio.ByteBuffer byteBuffer17 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.writeLogMark(byteBuffer17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
// flaky "82) test0943(Regression2Test)":         org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
// flaky "59) test0943(Regression2Test)":         org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str10, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test0944() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0944");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.lang.String str28 = logMark27.toString();
        logMark27.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark32 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        int int34 = logMark27.compare(logMark32);
        int int35 = logMark13.compare(logMark32);
        org.apache.bookkeeper.bookie.LogMark logMark38 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int42 = logMark38.compare(logMark41);
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark(logMark38);
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark(logMark43);
        int int45 = logMark13.compare(logMark44);
        long long46 = logMark13.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark49 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark50 = new org.apache.bookkeeper.bookie.LogMark(logMark49);
        java.lang.String str51 = logMark49.toString();
        int int52 = logMark13.compare(logMark49);
        java.lang.String str53 = logMark49.toString();
        org.apache.bookkeeper.bookie.LogMark logMark54 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark55 = new org.apache.bookkeeper.bookie.LogMark(logMark54);
        int int56 = logMark49.compare(logMark54);
        java.lang.String str57 = logMark54.toString();
        logMark54.setLogMark((long) (-1), (long) (byte) 1);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
// flaky "83) test0944(Regression2Test)":         org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
// flaky "60) test0944(Regression2Test)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + 1 + "'", int35 == 1);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + (-1) + "'", int45 == (-1));
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
        org.junit.Assert.assertEquals("'" + str51 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str51, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 1 + "'", int52 == 1);
        org.junit.Assert.assertEquals("'" + str53 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str53, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int56 + "' != '" + 1 + "'", int56 == 1);
        org.junit.Assert.assertEquals("'" + str57 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str57, "LogMark: logFileId - 0 , logFileOffset - 0");
    }

    @Test
    public void test0945() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0945");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        logMark2.setLogMark((long) 0, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str14 = logMark13.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
// flaky "84) test0945(Regression2Test)":         org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str14, "LogMark: logFileId - 0 , logFileOffset - 35");
    }

    @Test
    public void test0946() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0946");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark1.compare(logMark6);
        java.lang.String str8 = logMark1.toString();
        java.lang.Class<?> wildcardClass9 = logMark1.getClass();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "85) test0946(Regression2Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
// flaky "61) test0946(Regression2Test)":         org.junit.Assert.assertTrue("'" + int7 + "' != '" + (-1) + "'", int7 == (-1));
// flaky "34) test0946(Regression2Test)":         org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str8, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0947() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0947");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        org.apache.bookkeeper.bookie.LogMark logMark6 = null;
        // The following exception was thrown during execution in test generation
        try {
            int int7 = logMark4.compare(logMark6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "86) test0947(Regression2Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test0948() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0948");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
    }

    @Test
    public void test0949() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0949");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.lang.String str28 = logMark27.toString();
        logMark27.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark32 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        int int34 = logMark27.compare(logMark32);
        int int35 = logMark13.compare(logMark32);
        java.lang.String str36 = logMark32.toString();
        org.apache.bookkeeper.bookie.LogMark logMark37 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        logMark32.setLogMark((long) (short) -1, (long) 10);
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int47 = logMark43.compare(logMark46);
        logMark43.setLogMark((long) (short) 10, 10L);
        long long51 = logMark43.getLogFileId();
        int int52 = logMark32.compare(logMark43);
        org.apache.bookkeeper.bookie.LogMark logMark53 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        java.lang.String str54 = logMark53.toString();
        org.apache.bookkeeper.bookie.LogMark logMark55 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long56 = logMark55.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark(logMark55);
        org.apache.bookkeeper.bookie.LogMark logMark60 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark61 = new org.apache.bookkeeper.bookie.LogMark(logMark60);
        org.apache.bookkeeper.bookie.LogMark logMark64 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark65 = new org.apache.bookkeeper.bookie.LogMark(logMark64);
        int int66 = logMark60.compare(logMark65);
        java.lang.String str67 = logMark60.toString();
        logMark60.setLogMark((long) 10, 35L);
        int int71 = logMark55.compare(logMark60);
        org.apache.bookkeeper.bookie.LogMark logMark72 = new org.apache.bookkeeper.bookie.LogMark(logMark60);
        int int73 = logMark53.compare(logMark60);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
// flaky "87) test0949(Regression2Test)":         org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
// flaky "62) test0949(Regression2Test)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + 1 + "'", int35 == 1);
// flaky "35) test0949(Regression2Test)":         org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str36, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long51 + "' != '" + 10L + "'", long51 == 10L);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + (-1) + "'", int52 == (-1));
        org.junit.Assert.assertEquals("'" + str54 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str54, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark55);
        org.junit.Assert.assertTrue("'" + long56 + "' != '" + 10L + "'", long56 == 10L);
        org.junit.Assert.assertTrue("'" + int66 + "' != '" + (-1) + "'", int66 == (-1));
        org.junit.Assert.assertEquals("'" + str67 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str67, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int71 + "' != '" + (-1) + "'", int71 == (-1));
        org.junit.Assert.assertTrue("'" + int73 + "' != '" + (-1) + "'", int73 == (-1));
    }

    @Test
    public void test0950() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0950");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long4 = logMark0.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 10L + "'", long4 == 10L);
    }

    @Test
    public void test0951() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0951");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        logMark6.setLogMark((long) (short) 10, 10L);
        long long14 = logMark6.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int21 = logMark17.compare(logMark20);
        logMark17.setLogMark((long) (short) 10, 10L);
        long long25 = logMark17.getLogFileId();
        int int26 = logMark6.compare(logMark17);
        java.lang.String str27 = logMark17.toString();
        java.lang.String str28 = logMark17.toString();
        logMark17.setLogMark(0L, (long) '#');
        int int32 = logMark2.compare(logMark17);
        java.lang.Class<?> wildcardClass33 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 10L + "'", long14 == 10L);
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + 0 + "'", int21 == 0);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 10L + "'", long25 == 10L);
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + 0 + "'", int26 == 0);
        org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str27, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str28, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 1 + "'", int32 == 1);
        org.junit.Assert.assertNotNull(wildcardClass33);
    }

    @Test
    public void test0952() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0952");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) -1, (long) (short) -1);
        java.lang.String str3 = logMark2.toString();
        logMark2.setLogMark(0L, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int13 = logMark9.compare(logMark12);
        logMark9.setLogMark((long) (short) 10, 10L);
        long long17 = logMark9.getLogFileId();
        java.lang.String str18 = logMark9.toString();
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int26 = logMark22.compare(logMark25);
        logMark22.setLogMark((long) (short) 10, 10L);
        long long30 = logMark22.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark36 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int37 = logMark33.compare(logMark36);
        logMark33.setLogMark((long) (short) 10, 10L);
        long long41 = logMark33.getLogFileId();
        int int42 = logMark22.compare(logMark33);
        java.lang.String str43 = logMark33.toString();
        int int44 = logMark9.compare(logMark33);
        java.lang.String str45 = logMark33.toString();
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark(logMark33);
        int int47 = logMark2.compare(logMark46);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - -1" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - -1");
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 10L + "'", long17 == 10L);
        org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str18, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + 0 + "'", int26 == 0);
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 10L + "'", long30 == 10L);
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + 0 + "'", int37 == 0);
        org.junit.Assert.assertTrue("'" + long41 + "' != '" + 10L + "'", long41 == 10L);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertEquals("'" + str43 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str43, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int44 + "' != '" + 0 + "'", int44 == 0);
        org.junit.Assert.assertEquals("'" + str45 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str45, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + (-1) + "'", int47 == (-1));
    }

    @Test
    public void test0953() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0953");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        long long11 = logMark9.getLogFileId();
        int int12 = logMark3.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long14 = logMark3.getLogFileId();
        long long15 = logMark3.getLogFileId();
        long long16 = logMark3.getLogFileId();
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 0L + "'", long14 == 0L);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 0L + "'", long15 == 0L);
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 0L + "'", long16 == 0L);
    }

    @Test
    public void test0954() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0954");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark7.compare(logMark11);
        logMark11.setLogMark(97L, 97L);
        logMark11.setLogMark((long) (short) 100, (long) (byte) 100);
        logMark11.setLogMark(1L, 1L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
    }

    @Test
    public void test0955() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0955");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        java.lang.String str6 = logMark5.toString();
        int int7 = logMark3.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        java.lang.String str10 = logMark9.toString();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        java.lang.String str15 = logMark14.toString();
        logMark14.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark19 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark19);
        int int21 = logMark14.compare(logMark19);
        int int22 = logMark9.compare(logMark19);
        int int23 = logMark5.compare(logMark9);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 35L + "'", long4 == 35L);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str6, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str10, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str15, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark19);
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + 0 + "'", int21 == 0);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 1 + "'", int23 == 1);
    }

    @Test
    public void test0956() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0956");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        int int9 = logMark2.compare(logMark8);
        long long10 = logMark8.getLogFileId();
        long long11 = logMark8.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + (-1L) + "'", long10 == (-1L));
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 10L + "'", long11 == 10L);
    }

    @Test
    public void test0957() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0957");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(35L, 97L);
        long long3 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
    }

    @Test
    public void test0958() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0958");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) -1, (long) (short) -1);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long5 = logMark4.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + (-1L) + "'", long3 == (-1L));
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + (-1L) + "'", long5 == (-1L));
    }

    @Test
    public void test0959() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0959");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        java.lang.String str5 = logMark2.toString();
        java.lang.String str6 = logMark2.toString();
        logMark2.setLogMark(35L, (long) (short) 100);
        java.lang.Class<?> wildcardClass10 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str5, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str6, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(wildcardClass10);
    }

    @Test
    public void test0960() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0960");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        logMark13.setLogMark((long) (short) 10, 10L);
        long long21 = logMark13.getLogFileId();
        int int22 = logMark2.compare(logMark13);
        long long23 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.lang.String str28 = logMark27.toString();
        logMark27.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark32 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        int int34 = logMark27.compare(logMark32);
        int int35 = logMark13.compare(logMark32);
        java.lang.String str36 = logMark32.toString();
        org.apache.bookkeeper.bookie.LogMark logMark37 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        logMark32.setLogMark((long) (short) -1, (long) 10);
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int47 = logMark43.compare(logMark46);
        logMark43.setLogMark((long) (short) 10, 10L);
        long long51 = logMark43.getLogFileId();
        int int52 = logMark32.compare(logMark43);
        org.apache.bookkeeper.bookie.LogMark logMark55 = new org.apache.bookkeeper.bookie.LogMark(10L, 9223372036854775807L);
        org.apache.bookkeeper.bookie.LogMark logMark58 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int59 = logMark55.compare(logMark58);
        int int60 = logMark32.compare(logMark55);
        java.nio.ByteBuffer byteBuffer61 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark55.writeLogMark(byteBuffer61);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 1 + "'", int35 == 1);
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str36, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long51 + "' != '" + 10L + "'", long51 == 10L);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + (-1) + "'", int52 == (-1));
        org.junit.Assert.assertTrue("'" + int59 + "' != '" + (-1) + "'", int59 == (-1));
        org.junit.Assert.assertTrue("'" + int60 + "' != '" + (-1) + "'", int60 == (-1));
    }

    @Test
    public void test0961() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0961");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        long long4 = logMark2.getLogFileOffset();
        logMark2.setLogMark(52L, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark8.setLogMark((long) (short) 100, (long) (short) 100);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark((long) 10, (long) (byte) 0);
        int int15 = logMark8.compare(logMark14);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 10L + "'", long4 == 10L);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + 1 + "'", int15 == 1);
    }

    @Test
    public void test0962() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0962");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        long long5 = logMark1.getLogFileId();
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark1.readLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + (-1L) + "'", long5 == (-1L));
    }

    @Test
    public void test0963() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0963");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
        logMark6.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark6.compare(logMark11);
        int int14 = logMark1.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark16 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark16);
        java.lang.String str18 = logMark17.toString();
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        java.lang.String str23 = logMark22.toString();
        logMark22.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark27 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark27);
        int int29 = logMark22.compare(logMark27);
        int int30 = logMark17.compare(logMark27);
        int int31 = logMark15.compare(logMark27);
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark(logMark27);
        logMark32.setLogMark((long) (byte) 1, 10L);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertNotNull(logMark16);
        org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str18, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark27);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + 0 + "'", int29 == 0);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
    }

    @Test
    public void test0964() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0964");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        logMark2.setLogMark((long) 1, (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int19 = logMark15.compare(logMark18);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        int int21 = logMark2.compare(logMark15);
        java.nio.ByteBuffer byteBuffer22 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer22);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + (-1) + "'", int21 == (-1));
    }

    @Test
    public void test0965() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0965");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark5.setLogMark(1L, (long) (byte) 1);
        long long10 = logMark5.getLogFileId();
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.writeLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 1L + "'", long10 == 1L);
    }

    @Test
    public void test0966() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0966");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(10L, (long) (short) -1);
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0967() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0967");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        long long3 = logMark2.getLogFileId();
        java.lang.String str4 = logMark2.toString();
        long long5 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 1" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 1");
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 0L + "'", long5 == 0L);
    }

    @Test
    public void test0968() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0968");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(52L, 52L);
    }

    @Test
    public void test0969() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0969");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        long long7 = logMark5.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int10 = logMark5.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark13.compare(logMark16);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        int int19 = logMark9.compare(logMark13);
        long long20 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        logMark13.setLogMark((long) 10, (long) (byte) -1);
        logMark13.setLogMark((long) ' ', (long) (byte) -1);
        long long28 = logMark13.getLogFileId();
        java.lang.String str29 = logMark13.toString();
        long long30 = logMark13.getLogFileOffset();
        int int31 = logMark2.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark34 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark35 = new org.apache.bookkeeper.bookie.LogMark(logMark34);
        long long36 = logMark34.getLogFileOffset();
        java.lang.String str37 = logMark34.toString();
        java.lang.String str38 = logMark34.toString();
        logMark34.setLogMark(35L, (long) (short) 100);
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (short) -1);
        int int45 = logMark34.compare(logMark44);
        int int46 = logMark13.compare(logMark44);
        org.apache.bookkeeper.bookie.LogMark logMark49 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark50 = new org.apache.bookkeeper.bookie.LogMark(logMark49);
        java.lang.String str51 = logMark49.toString();
        org.apache.bookkeeper.bookie.LogMark logMark52 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark53 = new org.apache.bookkeeper.bookie.LogMark(logMark52);
        java.lang.String str54 = logMark53.toString();
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark58 = new org.apache.bookkeeper.bookie.LogMark(logMark57);
        java.lang.String str59 = logMark58.toString();
        logMark58.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark63 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark64 = new org.apache.bookkeeper.bookie.LogMark(logMark63);
        int int65 = logMark58.compare(logMark63);
        int int66 = logMark53.compare(logMark63);
        logMark53.setLogMark(97L, (long) '4');
        logMark53.setLogMark(32L, 0L);
        int int73 = logMark49.compare(logMark53);
        int int74 = logMark13.compare(logMark49);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 0L + "'", long7 == 0L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 1 + "'", int10 == 1);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 0L + "'", long20 == 0L);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 32L + "'", long28 == 32L);
        org.junit.Assert.assertEquals("'" + str29 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - -1" + "'", str29, "LogMark: logFileId - 32 , logFileOffset - -1");
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + (-1L) + "'", long30 == (-1L));
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + (-1) + "'", int31 == (-1));
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 0L + "'", long36 == 0L);
        org.junit.Assert.assertEquals("'" + str37 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str37, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str38 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str38, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + 1 + "'", int45 == 1);
        org.junit.Assert.assertTrue("'" + int46 + "' != '" + 1 + "'", int46 == 1);
        org.junit.Assert.assertEquals("'" + str51 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str51, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertNotNull(logMark52);
        org.junit.Assert.assertEquals("'" + str54 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str54, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str59 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str59, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark63);
        org.junit.Assert.assertTrue("'" + int65 + "' != '" + 0 + "'", int65 == 0);
        org.junit.Assert.assertTrue("'" + int66 + "' != '" + 0 + "'", int66 == 0);
        org.junit.Assert.assertTrue("'" + int73 + "' != '" + (-1) + "'", int73 == (-1));
        org.junit.Assert.assertTrue("'" + int74 + "' != '" + 1 + "'", int74 == 1);
    }

    @Test
    public void test0970() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0970");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long14 = logMark13.getLogFileOffset();
        int int15 = logMark10.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        long long17 = logMark16.getLogFileId();
        int int18 = logMark2.compare(logMark16);
        java.nio.ByteBuffer byteBuffer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark16.writeLogMark(byteBuffer19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 0L + "'", long14 == 0L);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + 1 + "'", int15 == 1);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + (-1L) + "'", long17 == (-1L));
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
    }

    @Test
    public void test0971() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0971");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 100, (long) 1);
        logMark2.setLogMark((long) 1, (long) (short) 0);
    }

    @Test
    public void test0972() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0972");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        logMark0.setLogMark((long) ' ', (long) ' ');
        long long6 = logMark0.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.readLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 32L + "'", long6 == 32L);
    }

    @Test
    public void test0973() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0973");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int14 = logMark10.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        int int16 = logMark6.compare(logMark10);
        long long17 = logMark10.getLogFileOffset();
        long long18 = logMark10.getLogFileOffset();
        long long19 = logMark10.getLogFileId();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 0L + "'", long17 == 0L);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 0L + "'", long18 == 0L);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 97L + "'", long19 == 97L);
    }

    @Test
    public void test0974() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0974");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, 0L);
        logMark2.setLogMark((long) (byte) 0, (long) (byte) 100);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str7 = logMark6.toString();
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 100" + "'", str7, "LogMark: logFileId - 0 , logFileOffset - 100");
    }

    @Test
    public void test0975() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0975");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark5.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark();
        int int11 = logMark5.compare(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark15 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long16 = logMark15.getLogFileOffset();
        long long17 = logMark15.getLogFileOffset();
        int int18 = logMark14.compare(logMark15);
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        long long23 = logMark21.getLogFileOffset();
        java.lang.String str24 = logMark21.toString();
        java.lang.String str25 = logMark21.toString();
        logMark21.setLogMark(35L, (long) (short) 100);
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        int int30 = logMark15.compare(logMark21);
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        int int32 = logMark5.compare(logMark21);
        long long33 = logMark5.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + 1 + "'", int11 == 1);
        org.junit.Assert.assertNotNull(logMark15);
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 32L + "'", long16 == 32L);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 32L + "'", long17 == 32L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + (-1) + "'", int18 == (-1));
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 0L + "'", long23 == 0L);
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str24, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str25 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str25, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + (-1) + "'", int30 == (-1));
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + (-1) + "'", int32 == (-1));
        org.junit.Assert.assertTrue("'" + long33 + "' != '" + 1L + "'", long33 == 1L);
    }

    @Test
    public void test0976() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0976");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), 10L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str5 = logMark4.toString();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 10L + "'", long3 == 10L);
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str5, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test0977() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0977");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark7.compare(logMark11);
        logMark11.setLogMark(97L, 97L);
        logMark11.setLogMark((long) (short) 100, (long) (byte) 100);
        logMark11.setLogMark(9223372036854775807L, 10L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
    }

    @Test
    public void test0978() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0978");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, (long) (byte) 10);
    }

    @Test
    public void test0979() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0979");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 100, (long) '#');
        logMark2.setLogMark((long) (byte) 10, (long) (byte) 100);
        long long6 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int13 = logMark9.compare(logMark12);
        logMark9.setLogMark((long) (short) 10, 10L);
        long long17 = logMark9.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int24 = logMark20.compare(logMark23);
        logMark20.setLogMark((long) (short) 10, 10L);
        long long28 = logMark20.getLogFileId();
        int int29 = logMark9.compare(logMark20);
        java.lang.String str30 = logMark20.toString();
        java.lang.String str31 = logMark20.toString();
        logMark20.setLogMark((long) (-1), 100L);
        logMark20.setLogMark((long) (short) 0, (long) (byte) 0);
        int int38 = logMark2.compare(logMark20);
        java.lang.String str39 = logMark2.toString();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 10L + "'", long6 == 10L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 10L + "'", long17 == 10L);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + 0 + "'", int29 == 0);
        org.junit.Assert.assertEquals("'" + str30 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str30, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str31 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str31, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + 1 + "'", int38 == 1);
        org.junit.Assert.assertEquals("'" + str39 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 100" + "'", str39, "LogMark: logFileId - 10 , logFileOffset - 100");
    }

    @Test
    public void test0980() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0980");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        java.lang.String str3 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long7 = logMark6.getLogFileId();
        logMark6.setLogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long12 = logMark11.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int15 = logMark6.compare(logMark11);
        logMark6.setLogMark(10L, (long) '4');
        int int19 = logMark2.compare(logMark6);
        java.lang.Class<?> wildcardClass20 = logMark6.getClass();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 100L + "'", long7 == 100L);
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 32L + "'", long12 == 32L);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + (-1) + "'", int15 == (-1));
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass20);
    }

    @Test
    public void test0981() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0981");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) -1, (long) (short) 1);
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0982() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0982");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(52L, (long) 'a');
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        long long7 = logMark5.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int10 = logMark5.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long17 = logMark16.getLogFileOffset();
        int int18 = logMark13.compare(logMark16);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark16);
        long long20 = logMark19.getLogFileId();
        int int21 = logMark5.compare(logMark19);
        int int22 = logMark2.compare(logMark19);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 0L + "'", long7 == 0L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 1 + "'", int10 == 1);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 0L + "'", long17 == 0L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + (-1L) + "'", long20 == (-1L));
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + 1 + "'", int21 == 1);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
    }

    @Test
    public void test0983() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0983");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        logMark3.setLogMark((long) 10, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.nio.ByteBuffer byteBuffer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark11.readLogMark(byteBuffer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0984() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0984");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        java.lang.String str3 = logMark2.toString();
        java.lang.String str4 = logMark2.toString();
        java.lang.String str5 = logMark2.toString();
        java.lang.String str6 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str3, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str4, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str5, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str6, "LogMark: logFileId - 1 , logFileOffset - 35");
    }

    @Test
    public void test0985() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0985");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        logMark2.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark6.readLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test0986() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0986");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 1);
    }

    @Test
    public void test0987() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0987");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (-1L));
    }

    @Test
    public void test0988() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0988");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int14 = logMark10.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        int int16 = logMark6.compare(logMark10);
        long long17 = logMark10.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        long long19 = logMark10.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(0L, 97L);
        int int23 = logMark10.compare(logMark22);
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 0L + "'", long17 == 0L);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 97L + "'", long19 == 97L);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 1 + "'", int23 == 1);
    }

    @Test
    public void test0989() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0989");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int9 = logMark5.compare(logMark8);
        logMark5.setLogMark((long) (short) 10, 10L);
        long long13 = logMark5.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int20 = logMark16.compare(logMark19);
        logMark16.setLogMark((long) (short) 10, 10L);
        long long24 = logMark16.getLogFileId();
        int int25 = logMark5.compare(logMark16);
        java.lang.String str26 = logMark16.toString();
        int int27 = logMark2.compare(logMark16);
        long long28 = logMark16.getLogFileId();
        long long29 = logMark16.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark35 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int36 = logMark32.compare(logMark35);
        logMark32.setLogMark((long) (short) 10, 10L);
        long long40 = logMark32.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int47 = logMark43.compare(logMark46);
        logMark43.setLogMark((long) (short) 10, 10L);
        long long51 = logMark43.getLogFileId();
        int int52 = logMark32.compare(logMark43);
        long long53 = logMark32.getLogFileId();
        long long54 = logMark32.getLogFileOffset();
        long long55 = logMark32.getLogFileId();
        int int56 = logMark16.compare(logMark32);
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark(logMark16);
        java.lang.String str58 = logMark16.toString();
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 10L + "'", long29 == 10L);
        org.junit.Assert.assertTrue("'" + int36 + "' != '" + 0 + "'", int36 == 0);
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + 10L + "'", long40 == 10L);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long51 + "' != '" + 10L + "'", long51 == 10L);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 0 + "'", int52 == 0);
        org.junit.Assert.assertTrue("'" + long53 + "' != '" + 10L + "'", long53 == 10L);
        org.junit.Assert.assertTrue("'" + long54 + "' != '" + 10L + "'", long54 == 10L);
        org.junit.Assert.assertTrue("'" + long55 + "' != '" + 10L + "'", long55 == 10L);
        org.junit.Assert.assertTrue("'" + int56 + "' != '" + 0 + "'", int56 == 0);
        org.junit.Assert.assertEquals("'" + str58 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str58, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test0990() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0990");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 0);
        long long3 = logMark2.getLogFileId();
        logMark2.setLogMark(32L, 0L);
        long long7 = logMark2.getLogFileOffset();
        long long8 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 0L + "'", long7 == 0L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
    }

    @Test
    public void test0991() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0991");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, 35L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        java.lang.String str8 = logMark7.toString();
        int int9 = logMark2.compare(logMark7);
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark7.writeLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str8, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
    }

    @Test
    public void test0992() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0992");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        java.lang.String str9 = logMark8.toString();
        logMark8.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark13 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        int int15 = logMark8.compare(logMark13);
        int int16 = logMark3.compare(logMark13);
        int int17 = logMark1.compare(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int26 = logMark22.compare(logMark25);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark22);
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark27);
        org.apache.bookkeeper.bookie.LogMark logMark29 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long30 = logMark29.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark(logMark29);
        java.lang.String str32 = logMark31.toString();
        logMark31.setLogMark(32L, 9223372036854775807L);
        long long36 = logMark31.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark37 = new org.apache.bookkeeper.bookie.LogMark(logMark31);
        int int38 = logMark28.compare(logMark37);
        org.apache.bookkeeper.bookie.LogMark logMark39 = new org.apache.bookkeeper.bookie.LogMark(logMark37);
        int int40 = logMark19.compare(logMark39);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertNotNull(logMark2);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str4, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark13);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + (-1) + "'", int15 == (-1));
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 0 + "'", int16 == 0);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + 0 + "'", int26 == 0);
        org.junit.Assert.assertNotNull(logMark29);
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 32L + "'", long30 == 32L);
        org.junit.Assert.assertEquals("'" + str32 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str32, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 32L + "'", long36 == 32L);
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + 1 + "'", int38 == 1);
        org.junit.Assert.assertTrue("'" + int40 + "' != '" + (-1) + "'", int40 == (-1));
    }

    @Test
    public void test0993() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0993");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, (long) 'a');
    }

    @Test
    public void test0994() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0994");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int10 = logMark3.compare(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.nio.ByteBuffer byteBuffer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark11.writeLogMark(byteBuffer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
    }

    @Test
    public void test0995() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0995");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), (long) 10);
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0996() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0996");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark8.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int13 = logMark2.compare(logMark12);
        long long14 = logMark12.getLogFileOffset();
        logMark12.setLogMark(0L, (long) (short) 100);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 35L + "'", long14 == 35L);
    }

    @Test
    public void test0997() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0997");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark6);
        logMark2.setLogMark((long) (short) -1, (long) (-1));
        logMark2.setLogMark((-1L), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int23 = logMark19.compare(logMark22);
        logMark19.setLogMark((long) (short) 10, 10L);
        long long27 = logMark19.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark30 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int34 = logMark30.compare(logMark33);
        logMark30.setLogMark((long) (short) 10, 10L);
        long long38 = logMark30.getLogFileId();
        int int39 = logMark19.compare(logMark30);
        java.lang.String str40 = logMark30.toString();
        int int41 = logMark16.compare(logMark30);
        long long42 = logMark30.getLogFileId();
        long long43 = logMark30.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark49 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int50 = logMark46.compare(logMark49);
        logMark46.setLogMark((long) (short) 10, 10L);
        long long54 = logMark46.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark60 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int61 = logMark57.compare(logMark60);
        logMark57.setLogMark((long) (short) 10, 10L);
        long long65 = logMark57.getLogFileId();
        int int66 = logMark46.compare(logMark57);
        long long67 = logMark46.getLogFileId();
        long long68 = logMark46.getLogFileOffset();
        long long69 = logMark46.getLogFileId();
        int int70 = logMark30.compare(logMark46);
        int int71 = logMark2.compare(logMark46);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 0 + "'", int23 == 0);
        org.junit.Assert.assertTrue("'" + long27 + "' != '" + 10L + "'", long27 == 10L);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
        org.junit.Assert.assertTrue("'" + long38 + "' != '" + 10L + "'", long38 == 10L);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + 0 + "'", int39 == 0);
        org.junit.Assert.assertEquals("'" + str40 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str40, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int41 + "' != '" + (-1) + "'", int41 == (-1));
        org.junit.Assert.assertTrue("'" + long42 + "' != '" + 10L + "'", long42 == 10L);
        org.junit.Assert.assertTrue("'" + long43 + "' != '" + 10L + "'", long43 == 10L);
        org.junit.Assert.assertTrue("'" + int50 + "' != '" + 0 + "'", int50 == 0);
        org.junit.Assert.assertTrue("'" + long54 + "' != '" + 10L + "'", long54 == 10L);
        org.junit.Assert.assertTrue("'" + int61 + "' != '" + 0 + "'", int61 == 0);
        org.junit.Assert.assertTrue("'" + long65 + "' != '" + 10L + "'", long65 == 10L);
        org.junit.Assert.assertTrue("'" + int66 + "' != '" + 0 + "'", int66 == 0);
        org.junit.Assert.assertTrue("'" + long67 + "' != '" + 10L + "'", long67 == 10L);
        org.junit.Assert.assertTrue("'" + long68 + "' != '" + 10L + "'", long68 == 10L);
        org.junit.Assert.assertTrue("'" + long69 + "' != '" + 10L + "'", long69 == 10L);
        org.junit.Assert.assertTrue("'" + int70 + "' != '" + 0 + "'", int70 == 0);
        org.junit.Assert.assertTrue("'" + int71 + "' != '" + (-1) + "'", int71 == (-1));
    }

    @Test
    public void test0998() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0998");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        logMark3.setLogMark((long) 10, 1L);
        logMark3.setLogMark((long) (byte) 0, (long) '#');
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0999() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test0999");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) (byte) 10);
    }

    @Test
    public void test1000() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression2Test.test1000");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark2.setLogMark((long) (byte) 100, (long) (short) 0);
        logMark2.setLogMark(32L, 9223372036854775807L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, 35L);
        int int12 = logMark2.compare(logMark11);
        java.nio.ByteBuffer byteBuffer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark11.readLogMark(byteBuffer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 1 + "'", int12 == 1);
    }
}
