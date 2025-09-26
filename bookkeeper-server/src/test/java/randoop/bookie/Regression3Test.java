package randoop.bookie;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class Regression3Test {

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
    public void test1001() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1001");
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
        java.lang.String str12 = logMark8.toString();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "1) test1001(Regression3Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
// flaky "1) test1001(Regression3Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark3);
// flaky "1) test1001(Regression3Test)":         org.junit.Assert.assertTrue("'" + long6 + "' != '" + (-1L) + "'", long6 == (-1L));
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
        org.junit.Assert.assertNotNull(logMark8);
// flaky "1) test1001(Regression3Test)":         org.junit.Assert.assertTrue("'" + long9 + "' != '" + 10L + "'", long9 == 10L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
// flaky "1) test1001(Regression3Test)":         org.junit.Assert.assertTrue("'" + long11 + "' != '" + 10L + "'", long11 == 10L);
// flaky "1) test1001(Regression3Test)":         org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str12, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test1002() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1002");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, 9223372036854775807L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass4 = logMark3.getClass();
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test1003() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1003");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 10, (long) '#');
        long long3 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 10L + "'", long3 == 10L);
    }

    @Test
    public void test1004() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1004");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), (long) (short) -1);
        logMark2.setLogMark((long) '4', (long) (short) 100);
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1005() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1005");
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
            logMark13.writeLogMark(byteBuffer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
// flaky "2) test1005(Regression3Test)":         org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
// flaky "2) test1005(Regression3Test)":         org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str10, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test1006() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1006");
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
        java.lang.String str46 = logMark13.toString();
        long long47 = logMark13.getLogFileOffset();
        long long48 = logMark13.getLogFileId();
        logMark13.setLogMark((long) (byte) 10, (long) (-1));
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
// flaky "3) test1006(Regression3Test)":         org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
// flaky "3) test1006(Regression3Test)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + 1 + "'", int35 == 1);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + (-1) + "'", int45 == (-1));
        org.junit.Assert.assertEquals("'" + str46 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str46, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long47 + "' != '" + 10L + "'", long47 == 10L);
        org.junit.Assert.assertTrue("'" + long48 + "' != '" + 10L + "'", long48 == 10L);
    }

    @Test
    public void test1007() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1007");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        long long11 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 10L + "'", long11 == 10L);
    }

    @Test
    public void test1008() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1008");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), (long) (short) -1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
    }

    @Test
    public void test1009() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1009");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(52L, (long) (short) 100);
    }

    @Test
    public void test1010() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1010");
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
        long long25 = logMark13.getLogFileOffset();
        java.lang.String str26 = logMark13.toString();
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
    public void test1011() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1011");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long11 = logMark10.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 10L + "'", long11 == 10L);
    }

    @Test
    public void test1012() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1012");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) (byte) 10);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileId();
        long long5 = logMark3.getLogFileId();
        java.lang.String str6 = logMark3.toString();
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 97L + "'", long4 == 97L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 97L + "'", long5 == 97L);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 10" + "'", str6, "LogMark: logFileId - 97 , logFileOffset - 10");
    }

    @Test
    public void test1013() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1013");
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
        java.nio.ByteBuffer byteBuffer38 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark35.readLogMark(byteBuffer38);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark3);
// flaky "4) test1013(Regression3Test)":         org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str5, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str10, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark14);
// flaky "4) test1013(Regression3Test)":         org.junit.Assert.assertTrue("'" + int16 + "' != '" + 0 + "'", int16 == 0);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertNotNull(logMark19);
// flaky "2) test1013(Regression3Test)":         org.junit.Assert.assertEquals("'" + str21 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str21, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str26, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark30);
// flaky "2) test1013(Regression3Test)":         org.junit.Assert.assertTrue("'" + int32 + "' != '" + 0 + "'", int32 == 0);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 0 + "'", int33 == 0);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
// flaky "2) test1013(Regression3Test)":         org.junit.Assert.assertTrue("'" + long36 + "' != '" + 10L + "'", long36 == 10L);
// flaky "2) test1013(Regression3Test)":         org.junit.Assert.assertTrue("'" + int37 + "' != '" + 1 + "'", int37 == 1);
    }

    @Test
    public void test1014() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1014");
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
            logMark26.readLogMark(byteBuffer39);
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
    public void test1015() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1015");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long8 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', 0L);
        int int12 = logMark2.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int19 = logMark15.compare(logMark18);
        logMark18.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark();
        int int24 = logMark18.compare(logMark23);
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
        long long48 = logMark38.getLogFileOffset();
        long long49 = logMark38.getLogFileId();
        java.lang.String str50 = logMark38.toString();
        int int51 = logMark23.compare(logMark38);
        long long52 = logMark23.getLogFileId();
        long long53 = logMark23.getLogFileOffset();
        long long54 = logMark23.getLogFileId();
        int int55 = logMark2.compare(logMark23);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 1 + "'", int24 == 1);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long48 + "' != '" + 10L + "'", long48 == 10L);
        org.junit.Assert.assertTrue("'" + long49 + "' != '" + 10L + "'", long49 == 10L);
        org.junit.Assert.assertEquals("'" + str50 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str50, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int51 + "' != '" + (-1) + "'", int51 == (-1));
        org.junit.Assert.assertTrue("'" + long52 + "' != '" + 0L + "'", long52 == 0L);
        org.junit.Assert.assertTrue("'" + long53 + "' != '" + 0L + "'", long53 == 0L);
        org.junit.Assert.assertTrue("'" + long54 + "' != '" + 0L + "'", long54 == 0L);
        org.junit.Assert.assertTrue("'" + int55 + "' != '" + 1 + "'", int55 == 1);
    }

    @Test
    public void test1016() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1016");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.junit.Assert.assertNotNull(logMark0);
// flaky "5) test1016(Regression3Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
// flaky "5) test1016(Regression3Test)":         org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str4, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test1017() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1017");
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
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark45 = new org.apache.bookkeeper.bookie.LogMark(logMark44);
        org.apache.bookkeeper.bookie.LogMark logMark48 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark49 = new org.apache.bookkeeper.bookie.LogMark(logMark48);
        int int50 = logMark44.compare(logMark49);
        java.lang.String str51 = logMark44.toString();
        org.apache.bookkeeper.bookie.LogMark logMark54 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark55 = new org.apache.bookkeeper.bookie.LogMark(logMark54);
        java.lang.String str56 = logMark54.toString();
        org.apache.bookkeeper.bookie.LogMark logMark59 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark59.setLogMark((long) (byte) 100, (long) (short) 0);
        int int63 = logMark54.compare(logMark59);
        int int64 = logMark44.compare(logMark54);
        org.apache.bookkeeper.bookie.LogMark logMark67 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark68 = new org.apache.bookkeeper.bookie.LogMark(logMark67);
        java.lang.String str69 = logMark68.toString();
        org.apache.bookkeeper.bookie.LogMark logMark70 = new org.apache.bookkeeper.bookie.LogMark(logMark68);
        long long71 = logMark70.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark74 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark77 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int78 = logMark74.compare(logMark77);
        org.apache.bookkeeper.bookie.LogMark logMark79 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark80 = new org.apache.bookkeeper.bookie.LogMark(logMark79);
        int int81 = logMark74.compare(logMark80);
        int int82 = logMark70.compare(logMark74);
        org.apache.bookkeeper.bookie.LogMark logMark83 = new org.apache.bookkeeper.bookie.LogMark(logMark70);
        org.apache.bookkeeper.bookie.LogMark logMark84 = new org.apache.bookkeeper.bookie.LogMark(logMark83);
        int int85 = logMark54.compare(logMark84);
        int int86 = logMark12.compare(logMark54);
        java.nio.ByteBuffer byteBuffer87 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark12.writeLogMark(byteBuffer87);
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
        org.junit.Assert.assertEquals("'" + str25 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str25, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark29);
// flaky "6) test1017(Regression3Test)":         org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + 0 + "'", int38 == 0);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + (-1) + "'", int39 == (-1));
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + (-1L) + "'", long40 == (-1L));
        org.junit.Assert.assertTrue("'" + int41 + "' != '" + 1 + "'", int41 == 1);
        org.junit.Assert.assertTrue("'" + int50 + "' != '" + (-1) + "'", int50 == (-1));
        org.junit.Assert.assertEquals("'" + str51 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str51, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str56 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str56, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int63 + "' != '" + (-1) + "'", int63 == (-1));
        org.junit.Assert.assertTrue("'" + int64 + "' != '" + 0 + "'", int64 == 0);
        org.junit.Assert.assertEquals("'" + str69 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str69, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long71 + "' != '" + 0L + "'", long71 == 0L);
        org.junit.Assert.assertTrue("'" + int78 + "' != '" + 0 + "'", int78 == 0);
        org.junit.Assert.assertNotNull(logMark79);
// flaky "6) test1017(Regression3Test)":         org.junit.Assert.assertTrue("'" + int81 + "' != '" + 1 + "'", int81 == 1);
        org.junit.Assert.assertTrue("'" + int82 + "' != '" + 0 + "'", int82 == 0);
        org.junit.Assert.assertTrue("'" + int85 + "' != '" + (-1) + "'", int85 == (-1));
        org.junit.Assert.assertTrue("'" + int86 + "' != '" + 1 + "'", int86 == 1);
    }

    @Test
    public void test1018() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1018");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        long long4 = logMark2.getLogFileOffset();
        logMark2.setLogMark(52L, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark8.setLogMark((long) (short) 100, (long) (short) 100);
        java.nio.ByteBuffer byteBuffer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.writeLogMark(byteBuffer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "7) test1018(Regression3Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
// flaky "7) test1018(Regression3Test)":         org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 10");
// flaky "3) test1018(Regression3Test)":         org.junit.Assert.assertTrue("'" + long4 + "' != '" + 10L + "'", long4 == 10L);
    }

    @Test
    public void test1019() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1019");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        long long3 = logMark2.getLogFileOffset();
        long long4 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 1L + "'", long4 == 1L);
    }

    @Test
    public void test1020() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1020");
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
        java.lang.String str19 = logMark17.toString();
        long long20 = logMark17.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark9);
// flaky "8) test1020(Regression3Test)":         org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
// flaky "8) test1020(Regression3Test)":         org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str12, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 32L + "'", long16 == 32L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 9223372036854775807" + "'", str19, "LogMark: logFileId - 32 , logFileOffset - 9223372036854775807");
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 9223372036854775807L + "'", long20 == 9223372036854775807L);
    }

    @Test
    public void test1021() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1021");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        logMark2.setLogMark(1L, (long) (byte) 1);
        logMark2.setLogMark((long) (byte) 0, 0L);
        logMark2.setLogMark((long) 10, (long) (byte) 10);
        logMark2.setLogMark(0L, (long) (byte) 1);
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test1022() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1022");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        logMark5.setLogMark(1L, 10L);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
    }

    @Test
    public void test1023() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1023");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(10L, (long) (short) -1);
        long long3 = logMark2.getLogFileId();
        logMark2.setLogMark(0L, (long) 0);
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 10L + "'", long3 == 10L);
    }

    @Test
    public void test1024() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1024");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        long long5 = logMark3.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 10L);
        int int9 = logMark3.compare(logMark8);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 35L + "'", long5 == 35L);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
    }

    @Test
    public void test1025() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1025");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark(10L, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long9 = logMark3.getLogFileId();
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 10L + "'", long9 == 10L);
    }

    @Test
    public void test1026() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1026");
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
        logMark35.setLogMark((long) (byte) 10, 100L);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertNotNull(logMark25);
// flaky "9) test1026(Regression3Test)":         org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str27, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark29);
// flaky "9) test1026(Regression3Test)":         org.junit.Assert.assertTrue("'" + long30 + "' != '" + 10L + "'", long30 == 10L);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 0 + "'", int33 == 0);
// flaky "4) test1026(Regression3Test)":         org.junit.Assert.assertTrue("'" + int34 + "' != '" + 1 + "'", int34 == 1);
    }

    @Test
    public void test1027() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1027");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 0);
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark13 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long14 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        java.lang.String str16 = logMark15.toString();
        logMark15.setLogMark(32L, 9223372036854775807L);
        long long20 = logMark15.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        int int22 = logMark12.compare(logMark21);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        int int24 = logMark2.compare(logMark21);
        long long25 = logMark21.getLogFileId();
        java.lang.String str26 = logMark21.toString();
        java.lang.String str27 = logMark21.toString();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertNotNull(logMark13);
// flaky "10) test1027(Regression3Test)":         org.junit.Assert.assertTrue("'" + long14 + "' != '" + 10L + "'", long14 == 10L);
// flaky "10) test1027(Regression3Test)":         org.junit.Assert.assertEquals("'" + str16 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str16, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 32L + "'", long20 == 32L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + (-1) + "'", int24 == (-1));
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 32L + "'", long25 == 32L);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 9223372036854775807" + "'", str26, "LogMark: logFileId - 32 , logFileOffset - 9223372036854775807");
        org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 9223372036854775807" + "'", str27, "LogMark: logFileId - 32 , logFileOffset - 9223372036854775807");
    }

    @Test
    public void test1028() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1028");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 0, 97L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 97" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 97");
    }

    @Test
    public void test1029() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1029");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 100, 1L);
    }

    @Test
    public void test1030() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1030");
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
        org.apache.bookkeeper.bookie.LogMark logMark40 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark(logMark40);
        long long42 = logMark40.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark(logMark43);
        int int45 = logMark40.compare(logMark44);
        org.apache.bookkeeper.bookie.LogMark logMark48 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark51 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int52 = logMark48.compare(logMark51);
        org.apache.bookkeeper.bookie.LogMark logMark53 = new org.apache.bookkeeper.bookie.LogMark(logMark48);
        int int54 = logMark44.compare(logMark48);
        long long55 = logMark48.getLogFileId();
        java.lang.String str56 = logMark48.toString();
        int int57 = logMark2.compare(logMark48);
        org.apache.bookkeeper.bookie.LogMark logMark58 = new org.apache.bookkeeper.bookie.LogMark(logMark48);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark8);
// flaky "11) test1030(Regression3Test)":         org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
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
        org.junit.Assert.assertTrue("'" + long42 + "' != '" + 0L + "'", long42 == 0L);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + 1 + "'", int45 == 1);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 0 + "'", int52 == 0);
        org.junit.Assert.assertTrue("'" + int54 + "' != '" + (-1) + "'", int54 == (-1));
        org.junit.Assert.assertTrue("'" + long55 + "' != '" + 97L + "'", long55 == 97L);
        org.junit.Assert.assertEquals("'" + str56 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str56, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int57 + "' != '" + 0 + "'", int57 == 0);
    }

    @Test
    public void test1031() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1031");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(32L, 100L);
    }

    @Test
    public void test1032() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1032");
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
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        java.lang.Class<?> wildcardClass19 = logMark1.getClass();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "12) test1032(Regression3Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
// flaky "11) test1032(Regression3Test)":         org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test1033() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1033");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long4 = logMark0.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.readLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "13) test1033(Regression3Test)":         org.junit.Assert.assertTrue("'" + long2 + "' != '" + 10L + "'", long2 == 10L);
// flaky "12) test1033(Regression3Test)":         org.junit.Assert.assertTrue("'" + long4 + "' != '" + 10L + "'", long4 == 10L);
    }

    @Test
    public void test1034() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1034");
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
        java.nio.ByteBuffer byteBuffer39 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark16.writeLogMark(byteBuffer39);
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
    public void test1035() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1035");
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
        long long59 = logMark5.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + (-1) + "'", int7 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertNotNull(logMark15);
// flaky "14) test1035(Regression3Test)":         org.junit.Assert.assertTrue("'" + int17 + "' != '" + 1 + "'", int17 == 1);
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
        org.junit.Assert.assertTrue("'" + long59 + "' != '" + 35L + "'", long59 == 35L);
    }

    @Test
    public void test1036() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1036");
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
        long long53 = logMark13.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
// flaky "15) test1036(Regression3Test)":         org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
// flaky "13) test1036(Regression3Test)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + 1 + "'", int35 == 1);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + (-1) + "'", int45 == (-1));
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
        org.junit.Assert.assertEquals("'" + str51 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str51, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 1 + "'", int52 == 1);
        org.junit.Assert.assertTrue("'" + long53 + "' != '" + 10L + "'", long53 == 10L);
    }

    @Test
    public void test1037() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1037");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) -1, 0L);
    }

    @Test
    public void test1038() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1038");
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
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.nio.ByteBuffer byteBuffer28 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark27.readLogMark(byteBuffer28);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "16) test1038(Regression3Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
// flaky "14) test1038(Regression3Test)":         org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
// flaky "5) test1038(Regression3Test)":         org.junit.Assert.assertTrue("'" + long15 + "' != '" + (-1L) + "'", long15 == (-1L));
        org.junit.Assert.assertEquals("'" + str22 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str22, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 1 + "'", int25 == 1);
    }

    @Test
    public void test1039() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1039");
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
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark42 = new org.apache.bookkeeper.bookie.LogMark(logMark41);
        java.lang.String str43 = logMark42.toString();
        logMark42.setLogMark((long) (-1), (long) (byte) 100);
        org.apache.bookkeeper.bookie.LogMark logMark47 = new org.apache.bookkeeper.bookie.LogMark(logMark42);
        int int48 = logMark20.compare(logMark42);
        java.lang.Class<?> wildcardClass49 = logMark20.getClass();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 10L + "'", long6 == 10L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 10L + "'", long17 == 10L);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + 0 + "'", int29 == 0);
        org.junit.Assert.assertEquals("'" + str30 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str30, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str31 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str31, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + 1 + "'", int38 == 1);
        org.junit.Assert.assertEquals("'" + str43 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str43, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int48 + "' != '" + 1 + "'", int48 == 1);
        org.junit.Assert.assertNotNull(wildcardClass49);
    }

    @Test
    public void test1040() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1040");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark5.setLogMark((long) (short) 100, 35L);
        long long9 = logMark5.getLogFileOffset();
        long long10 = logMark5.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 35L + "'", long9 == 35L);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 35L + "'", long10 == 35L);
    }

    @Test
    public void test1041() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1041");
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
        logMark54.setLogMark((long) (short) 1, (long) '4');
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
// flaky "17) test1041(Regression3Test)":         org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
// flaky "15) test1041(Regression3Test)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + 1 + "'", int35 == 1);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + (-1) + "'", int45 == (-1));
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
        org.junit.Assert.assertEquals("'" + str51 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str51, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 1 + "'", int52 == 1);
        org.junit.Assert.assertEquals("'" + str53 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str53, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int56 + "' != '" + 1 + "'", int56 == 1);
    }

    @Test
    public void test1042() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1042");
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
        long long41 = logMark32.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
// flaky "18) test1042(Regression3Test)":         org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
// flaky "16) test1042(Regression3Test)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + 1 + "'", int35 == 1);
// flaky "6) test1042(Regression3Test)":         org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str36, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long41 + "' != '" + 10L + "'", long41 == 10L);
    }

    @Test
    public void test1043() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1043");
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
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark((long) (short) -1, (long) (short) -1);
        java.lang.String str30 = logMark29.toString();
        int int31 = logMark13.compare(logMark29);
        org.apache.bookkeeper.bookie.LogMark logMark32 = null;
        // The following exception was thrown during execution in test generation
        try {
            int int33 = logMark13.compare(logMark32);
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
        org.junit.Assert.assertEquals("'" + str30 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - -1" + "'", str30, "LogMark: logFileId - -1 , logFileOffset - -1");
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 1 + "'", int31 == 1);
    }

    @Test
    public void test1044() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1044");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        java.lang.String str5 = logMark2.toString();
        java.lang.String str6 = logMark2.toString();
        logMark2.setLogMark(35L, (long) (short) 100);
        long long10 = logMark2.getLogFileId();
        logMark2.setLogMark((long) '4', (long) (byte) 100);
        java.nio.ByteBuffer byteBuffer14 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str5, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str6, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 35L + "'", long10 == 35L);
    }

    @Test
    public void test1045() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1045");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) ' ');
    }

    @Test
    public void test1046() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1046");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 0);
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark13 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long14 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        java.lang.String str16 = logMark15.toString();
        logMark15.setLogMark(32L, 9223372036854775807L);
        long long20 = logMark15.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        int int22 = logMark12.compare(logMark21);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        int int24 = logMark2.compare(logMark21);
        long long25 = logMark21.getLogFileId();
        java.lang.String str26 = logMark21.toString();
        long long27 = logMark21.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertNotNull(logMark13);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 10L + "'", long14 == 10L);
        org.junit.Assert.assertEquals("'" + str16 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str16, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 32L + "'", long20 == 32L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + (-1) + "'", int24 == (-1));
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 32L + "'", long25 == 32L);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 9223372036854775807" + "'", str26, "LogMark: logFileId - 32 , logFileOffset - 9223372036854775807");
        org.junit.Assert.assertTrue("'" + long27 + "' != '" + 9223372036854775807L + "'", long27 == 9223372036854775807L);
    }

    @Test
    public void test1047() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1047");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, 97L);
        logMark2.setLogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
    }

    @Test
    public void test1048() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1048");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark1.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        java.lang.String str12 = logMark10.toString();
        org.apache.bookkeeper.bookie.LogMark logMark13 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        java.lang.String str15 = logMark14.toString();
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark18);
        java.lang.String str20 = logMark19.toString();
        logMark19.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark24 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark(logMark24);
        int int26 = logMark19.compare(logMark24);
        int int27 = logMark14.compare(logMark24);
        logMark14.setLogMark(97L, (long) '4');
        logMark14.setLogMark(32L, 0L);
        int int34 = logMark10.compare(logMark14);
        int int35 = logMark6.compare(logMark10);
        java.nio.ByteBuffer byteBuffer36 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark10.readLogMark(byteBuffer36);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + (-1) + "'", int7 == (-1));
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str12, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertNotNull(logMark13);
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str15, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str20 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str20, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark24);
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + 0 + "'", int26 == 0);
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 0 + "'", int27 == 0);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 1 + "'", int35 == 1);
    }

    @Test
    public void test1049() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1049");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, 35L);
        long long3 = logMark2.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
    }

    @Test
    public void test1050() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1050");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(97L, (long) (short) 0);
        int int7 = logMark3.compare(logMark6);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + (-1) + "'", int7 == (-1));
    }

    @Test
    public void test1051() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1051");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark(1L, (long) (short) 100);
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.readLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 10L + "'", long2 == 10L);
    }

    @Test
    public void test1052() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1052");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(52L, 9223372036854775807L);
    }

    @Test
    public void test1053() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1053");
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
        java.lang.String str53 = logMark13.toString();
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
        org.junit.Assert.assertEquals("'" + str53 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str53, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test1054() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1054");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.String str6 = logMark5.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        long long8 = logMark7.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 100L + "'", long1 == 100L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 100" + "'", str4, "LogMark: logFileId - 1 , logFileOffset - 100");
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 100" + "'", str6, "LogMark: logFileId - 1 , logFileOffset - 100");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 100L + "'", long8 == 100L);
    }

    @Test
    public void test1055() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1055");
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
        long long33 = logMark32.getLogFileOffset();
        long long34 = logMark32.getLogFileId();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 100" + "'", str2, "LogMark: logFileId - 1 , logFileOffset - 100");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertNotNull(logMark16);
        org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 100" + "'", str18, "LogMark: logFileId - 1 , logFileOffset - 100");
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark27);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + long33 + "' != '" + 100L + "'", long33 == 100L);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 1L + "'", long34 == 1L);
    }

    @Test
    public void test1056() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1056");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long8 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark2.setLogMark(0L, (long) (byte) 0);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
    }

    @Test
    public void test1057() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1057");
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
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str13, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test1058() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1058");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) '#');
        logMark2.setLogMark(52L, 97L);
    }

    @Test
    public void test1059() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1059");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark5.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark();
        int int11 = logMark5.compare(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int18 = logMark14.compare(logMark17);
        logMark14.setLogMark((long) (short) 10, 10L);
        long long22 = logMark14.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int29 = logMark25.compare(logMark28);
        logMark25.setLogMark((long) (short) 10, 10L);
        long long33 = logMark25.getLogFileId();
        int int34 = logMark14.compare(logMark25);
        long long35 = logMark25.getLogFileOffset();
        long long36 = logMark25.getLogFileId();
        java.lang.String str37 = logMark25.toString();
        int int38 = logMark10.compare(logMark25);
        java.nio.ByteBuffer byteBuffer39 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark10.writeLogMark(byteBuffer39);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + 1 + "'", int11 == 1);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 0 + "'", int18 == 0);
        org.junit.Assert.assertTrue("'" + long22 + "' != '" + 10L + "'", long22 == 10L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + 0 + "'", int29 == 0);
        org.junit.Assert.assertTrue("'" + long33 + "' != '" + 10L + "'", long33 == 10L);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 10L + "'", long36 == 10L);
        org.junit.Assert.assertEquals("'" + str37 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str37, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + (-1) + "'", int38 == (-1));
    }

    @Test
    public void test1060() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1060");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', 1L);
    }

    @Test
    public void test1061() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1061");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        java.lang.String str3 = logMark2.toString();
        logMark2.setLogMark((long) (short) 10, (long) '#');
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 1" + "'", str3, "LogMark: logFileId - 0 , logFileOffset - 1");
    }

    @Test
    public void test1062() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1062");
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
        long long19 = logMark3.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark((long) '4', 9223372036854775807L);
        int int23 = logMark3.compare(logMark22);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + (-1L) + "'", long13 == (-1L));
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 97L + "'", long19 == 97L);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 1 + "'", int23 == 1);
    }

    @Test
    public void test1063() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1063");
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
        long long11 = logMark0.getLogFileId();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 100L + "'", long1 == 100L);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 100" + "'", str2, "LogMark: logFileId - 1 , logFileOffset - 100");
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 1L + "'", long6 == 1L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 100L + "'", long9 == 100L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 1L + "'", long11 == 1L);
    }

    @Test
    public void test1064() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1064");
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
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        java.lang.String str30 = logMark29.toString();
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertNotNull(logMark20);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 100L + "'", long21 == 100L);
        org.junit.Assert.assertTrue("'" + long22 + "' != '" + 100L + "'", long22 == 100L);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + (-1) + "'", int23 == (-1));
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
        org.junit.Assert.assertTrue("'" + int28 + "' != '" + 1 + "'", int28 == 1);
        org.junit.Assert.assertEquals("'" + str30 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 1" + "'", str30, "LogMark: logFileId - 32 , logFileOffset - 1");
    }

    @Test
    public void test1065() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1065");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        logMark2.setLogMark((long) (byte) 0, (long) (byte) 0);
        logMark2.setLogMark((long) (byte) -1, (long) (byte) 0);
        long long9 = logMark2.getLogFileId();
        java.lang.Class<?> wildcardClass10 = logMark2.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + (-1L) + "'", long9 == (-1L));
        org.junit.Assert.assertNotNull(wildcardClass10);
    }

    @Test
    public void test1066() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1066");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int10 = logMark3.compare(logMark8);
        long long11 = logMark8.getLogFileOffset();
        logMark8.setLogMark((-1L), (long) (short) 10);
        java.nio.ByteBuffer byteBuffer15 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.writeLogMark(byteBuffer15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 100L + "'", long11 == 100L);
    }

    @Test
    public void test1067() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1067");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        java.lang.String str3 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark1.compare(logMark6);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark6.writeLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
    }

    @Test
    public void test1068() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1068");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        java.lang.String str5 = logMark2.toString();
        java.lang.String str6 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str5, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str6, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test1069() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1069");
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
        java.lang.Class<?> wildcardClass18 = logMark9.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertNotNull(logMark14);
// flaky "19) test1069(Regression3Test)":         org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertNotNull(wildcardClass18);
    }

    @Test
    public void test1070() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1070");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        long long9 = logMark5.getLogFileOffset();
        java.lang.Class<?> wildcardClass10 = logMark5.getClass();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 0L + "'", long9 == 0L);
        org.junit.Assert.assertNotNull(wildcardClass10);
    }

    @Test
    public void test1071() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1071");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, 32L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, (-1L));
        long long6 = logMark5.getLogFileId();
        int int7 = logMark2.compare(logMark5);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 1L + "'", long6 == 1L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + (-1) + "'", int7 == (-1));
    }

    @Test
    public void test1072() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1072");
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
        logMark2.setLogMark((-1L), (long) (short) 0);
        java.lang.String str33 = logMark2.toString();
        java.lang.Class<?> wildcardClass34 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 0L + "'", long29 == 0L);
        org.junit.Assert.assertEquals("'" + str33 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str33, "LogMark: logFileId - -1 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(wildcardClass34);
    }

    @Test
    public void test1073() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1073");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        java.lang.String str9 = logMark2.toString();
        long long10 = logMark2.getLogFileId();
        logMark2.setLogMark(35L, 0L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str9, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 0L + "'", long10 == 0L);
    }

    @Test
    public void test1074() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1074");
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
        logMark10.setLogMark((long) 10, (long) (byte) -1);
        logMark10.setLogMark((long) ' ', (long) (byte) -1);
        long long25 = logMark10.getLogFileId();
        java.lang.String str26 = logMark10.toString();
        long long27 = logMark10.getLogFileOffset();
        logMark10.setLogMark(35L, (long) '#');
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 0L + "'", long17 == 0L);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 32L + "'", long25 == 32L);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - -1" + "'", str26, "LogMark: logFileId - 32 , logFileOffset - -1");
        org.junit.Assert.assertTrue("'" + long27 + "' != '" + (-1L) + "'", long27 == (-1L));
    }

    @Test
    public void test1075() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1075");
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
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.junit.Assert.assertNotNull(logMark4);
// flaky "20) test1075(Regression3Test)":         org.junit.Assert.assertTrue("'" + long5 + "' != '" + 97L + "'", long5 == 97L);
// flaky "17) test1075(Regression3Test)":         org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str7, "LogMark: logFileId - 100 , logFileOffset - 97");
// flaky "7) test1075(Regression3Test)":         org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 0L + "'", long13 == 0L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 0 + "'", int23 == 0);
    }

    @Test
    public void test1076() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1076");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, 0L);
        logMark2.setLogMark((long) (byte) 100, 35L);
        long long6 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 35L + "'", long6 == 35L);
    }

    @Test
    public void test1077() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1077");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        java.lang.String str2 = logMark0.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        long long6 = logMark5.getLogFileId();
        int int7 = logMark0.compare(logMark5);
        logMark5.setLogMark((long) '4', 52L);
        long long11 = logMark5.getLogFileId();
        java.lang.String str12 = logMark5.toString();
        logMark5.setLogMark(32L, (long) '4');
        org.junit.Assert.assertNotNull(logMark0);
// flaky "21) test1077(Regression3Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
// flaky "18) test1077(Regression3Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark3);
// flaky "8) test1077(Regression3Test)":         org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 52L + "'", long11 == 52L);
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 52" + "'", str12, "LogMark: logFileId - 52 , logFileOffset - 52");
    }

    @Test
    public void test1078() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1078");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        long long3 = logMark2.getLogFileOffset();
        long long4 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long8 = logMark7.getLogFileId();
        int int9 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str11 = logMark10.toString();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 1L + "'", long4 == 1L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 100L + "'", long8 == 100L);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str11, "LogMark: logFileId - 1 , logFileOffset - 35");
    }

    @Test
    public void test1079() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1079");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        logMark2.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark7.readLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test1080() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1080");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        java.lang.String str3 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        java.lang.String str6 = logMark4.toString();
        long long7 = logMark4.getLogFileOffset();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str3, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str6, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 35L + "'", long7 == 35L);
    }

    @Test
    public void test1081() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1081");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 100, (long) (short) -1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.String str5 = logMark4.toString();
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int10 = logMark4.compare(logMark9);
        java.lang.String str11 = logMark4.toString();
        long long12 = logMark4.getLogFileId();
        int int13 = logMark2.compare(logMark4);
        org.junit.Assert.assertNotNull(logMark3);
// flaky "22) test1081(Regression3Test)":         org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str5, "LogMark: logFileId - 100 , logFileOffset - 97");
// flaky "19) test1081(Regression3Test)":         org.junit.Assert.assertTrue("'" + int10 + "' != '" + 1 + "'", int10 == 1);
// flaky "9) test1081(Regression3Test)":         org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str11, "LogMark: logFileId - 100 , logFileOffset - 97");
// flaky "3) test1081(Regression3Test)":         org.junit.Assert.assertTrue("'" + long12 + "' != '" + 100L + "'", long12 == 100L);
// flaky "3) test1081(Regression3Test)":         org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
    }

    @Test
    public void test1082() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1082");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark(0L, (long) (byte) 0);
        logMark3.setLogMark((long) '4', (long) (byte) -1);
        long long11 = logMark3.getLogFileId();
        long long12 = logMark3.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        java.lang.String str16 = logMark15.toString();
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
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
        int int44 = logMark19.compare(logMark33);
        long long45 = logMark33.getLogFileId();
        long long46 = logMark33.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark49 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark52 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int53 = logMark49.compare(logMark52);
        logMark49.setLogMark((long) (short) 10, 10L);
        long long57 = logMark49.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark60 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark63 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int64 = logMark60.compare(logMark63);
        logMark60.setLogMark((long) (short) 10, 10L);
        long long68 = logMark60.getLogFileId();
        int int69 = logMark49.compare(logMark60);
        long long70 = logMark49.getLogFileId();
        long long71 = logMark49.getLogFileOffset();
        long long72 = logMark49.getLogFileId();
        int int73 = logMark33.compare(logMark49);
        org.apache.bookkeeper.bookie.LogMark logMark74 = new org.apache.bookkeeper.bookie.LogMark(logMark33);
        long long75 = logMark74.getLogFileOffset();
        long long76 = logMark74.getLogFileId();
        int int77 = logMark15.compare(logMark74);
        int int78 = logMark3.compare(logMark15);
        long long79 = logMark15.getLogFileId();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "23) test1082(Regression3Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
// flaky "20) test1082(Regression3Test)":         org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str4, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 52L + "'", long11 == 52L);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 52L + "'", long12 == 52L);
        org.junit.Assert.assertEquals("'" + str16 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str16, "LogMark: logFileId - -1 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + 0 + "'", int26 == 0);
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 10L + "'", long30 == 10L);
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + 0 + "'", int37 == 0);
        org.junit.Assert.assertTrue("'" + long41 + "' != '" + 10L + "'", long41 == 10L);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertEquals("'" + str43 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str43, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int44 + "' != '" + (-1) + "'", int44 == (-1));
        org.junit.Assert.assertTrue("'" + long45 + "' != '" + 10L + "'", long45 == 10L);
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
        org.junit.Assert.assertTrue("'" + int53 + "' != '" + 0 + "'", int53 == 0);
        org.junit.Assert.assertTrue("'" + long57 + "' != '" + 10L + "'", long57 == 10L);
        org.junit.Assert.assertTrue("'" + int64 + "' != '" + 0 + "'", int64 == 0);
        org.junit.Assert.assertTrue("'" + long68 + "' != '" + 10L + "'", long68 == 10L);
        org.junit.Assert.assertTrue("'" + int69 + "' != '" + 0 + "'", int69 == 0);
        org.junit.Assert.assertTrue("'" + long70 + "' != '" + 10L + "'", long70 == 10L);
        org.junit.Assert.assertTrue("'" + long71 + "' != '" + 10L + "'", long71 == 10L);
        org.junit.Assert.assertTrue("'" + long72 + "' != '" + 10L + "'", long72 == 10L);
        org.junit.Assert.assertTrue("'" + int73 + "' != '" + 0 + "'", int73 == 0);
        org.junit.Assert.assertTrue("'" + long75 + "' != '" + 10L + "'", long75 == 10L);
        org.junit.Assert.assertTrue("'" + long76 + "' != '" + 10L + "'", long76 == 10L);
        org.junit.Assert.assertTrue("'" + int77 + "' != '" + (-1) + "'", int77 == (-1));
        org.junit.Assert.assertTrue("'" + int78 + "' != '" + 1 + "'", int78 == 1);
        org.junit.Assert.assertTrue("'" + long79 + "' != '" + (-1L) + "'", long79 == (-1L));
    }

    @Test
    public void test1083() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1083");
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
        long long23 = logMark18.getLogFileOffset();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertNotNull(logMark14);
// flaky "24) test1083(Regression3Test)":         org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 1L + "'", long23 == 1L);
    }

    @Test
    public void test1084() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1084");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        logMark2.setLogMark((long) 0, (long) '#');
        java.lang.Class<?> wildcardClass13 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
// flaky "25) test1084(Regression3Test)":         org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test1085() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1085");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', (long) (byte) 100);
        long long3 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
    }

    @Test
    public void test1086() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1086");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        logMark2.setLogMark(32L, 9223372036854775807L);
        long long7 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        java.lang.String str10 = logMark9.toString();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "26) test1086(Regression3Test)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
// flaky "21) test1086(Regression3Test)":         org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str3, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 32L + "'", long7 == 32L);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 9223372036854775807" + "'", str10, "LogMark: logFileId - 32 , logFileOffset - 9223372036854775807");
    }

    @Test
    public void test1087() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1087");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        long long3 = logMark2.getLogFileId();
        java.lang.String str4 = logMark2.toString();
        long long5 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        logMark7.setLogMark((long) (short) -1, 0L);
        long long11 = logMark7.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        long long16 = logMark14.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark17);
        int int19 = logMark14.compare(logMark18);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int26 = logMark22.compare(logMark25);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark22);
        int int28 = logMark18.compare(logMark22);
        int int29 = logMark7.compare(logMark22);
        logMark22.setLogMark(52L, 35L);
        int int33 = logMark2.compare(logMark22);
        java.lang.String str34 = logMark22.toString();
        org.apache.bookkeeper.bookie.LogMark logMark37 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long38 = logMark37.getLogFileOffset();
        logMark37.setLogMark(97L, (long) '4');
        int int42 = logMark22.compare(logMark37);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 1" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 1");
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 0L + "'", long5 == 0L);
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + (-1L) + "'", long11 == (-1L));
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 0L + "'", long16 == 0L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 1 + "'", int19 == 1);
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + 0 + "'", int26 == 0);
        org.junit.Assert.assertTrue("'" + int28 + "' != '" + (-1) + "'", int28 == (-1));
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + (-1) + "'", int33 == (-1));
        org.junit.Assert.assertEquals("'" + str34 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 35" + "'", str34, "LogMark: logFileId - 52 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long38 + "' != '" + 0L + "'", long38 == 0L);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + (-1) + "'", int42 == (-1));
    }

    @Test
    public void test1088() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1088");
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
        logMark3.setLogMark(100L, (long) (short) 100);
        java.lang.String str22 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark25);
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark30 = new org.apache.bookkeeper.bookie.LogMark(logMark29);
        int int31 = logMark25.compare(logMark30);
        long long32 = logMark25.getLogFileId();
        int int33 = logMark3.compare(logMark25);
        java.lang.Class<?> wildcardClass34 = logMark25.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
// flaky "27) test1088(Regression3Test)":         org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + (-1) + "'", int18 == (-1));
        org.junit.Assert.assertEquals("'" + str22 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 100" + "'", str22, "LogMark: logFileId - 100 , logFileOffset - 100");
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + (-1) + "'", int31 == (-1));
        org.junit.Assert.assertTrue("'" + long32 + "' != '" + 0L + "'", long32 == 0L);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 1 + "'", int33 == 1);
        org.junit.Assert.assertNotNull(wildcardClass34);
    }

    @Test
    public void test1089() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1089");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) 1);
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
    public void test1090() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1090");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, (long) (byte) 1);
    }

    @Test
    public void test1091() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1091");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 100, (long) (byte) 100);
    }

    @Test
    public void test1092() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1092");
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
            logMark39.readLogMark(byteBuffer48);
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
// flaky "28) test1092(Regression3Test)":         org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
// flaky "22) test1092(Regression3Test)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
        org.junit.Assert.assertNotNull(logMark36);
// flaky "10) test1092(Regression3Test)":         org.junit.Assert.assertTrue("'" + long37 + "' != '" + 97L + "'", long37 == 97L);
// flaky "4) test1092(Regression3Test)":         org.junit.Assert.assertEquals("'" + str40 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str40, "LogMark: logFileId - 100 , logFileOffset - 97");
// flaky "4) test1092(Regression3Test)":         org.junit.Assert.assertTrue("'" + int47 + "' != '" + 1 + "'", int47 == 1);
    }

    @Test
    public void test1093() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1093");
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
        java.lang.Class<?> wildcardClass23 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
// flaky "29) test1093(Regression3Test)":         org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + (-1) + "'", int21 == (-1));
        org.junit.Assert.assertTrue("'" + long22 + "' != '" + 1L + "'", long22 == 1L);
        org.junit.Assert.assertNotNull(wildcardClass23);
    }

    @Test
    public void test1094() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1094");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        long long5 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass7 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 0L + "'", long5 == 0L);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test1095() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1095");
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
        logMark5.setLogMark((long) 'a', 32L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark6);
// flaky "30) test1095(Regression3Test)":         org.junit.Assert.assertTrue("'" + long8 + "' != '" + 97L + "'", long8 == 97L);
        org.junit.Assert.assertNotNull(logMark12);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 97L + "'", long13 == 97L);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 97L + "'", long15 == 97L);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
    }

    @Test
    public void test1096() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1096");
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
        java.lang.Class<?> wildcardClass24 = logMark11.getClass();
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 97L + "'", long5 == 97L);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str7, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 0L + "'", long13 == 0L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 0 + "'", int23 == 0);
        org.junit.Assert.assertNotNull(wildcardClass24);
    }

    @Test
    public void test1097() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1097");
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
        java.lang.String str24 = logMark2.toString();
        java.lang.Class<?> wildcardClass25 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str24, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(wildcardClass25);
    }

    @Test
    public void test1098() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1098");
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
        long long21 = logMark14.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer22 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark14.writeLogMark(byteBuffer22);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 52L + "'", long18 == 52L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 52L + "'", long21 == 52L);
    }

    @Test
    public void test1099() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1099");
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
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
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
        long long53 = logMark43.getLogFileOffset();
        long long54 = logMark43.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark60 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int61 = logMark57.compare(logMark60);
        logMark57.setLogMark((long) (short) 10, 10L);
        long long65 = logMark57.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark68 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark71 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int72 = logMark68.compare(logMark71);
        logMark68.setLogMark((long) (short) 10, 10L);
        long long76 = logMark68.getLogFileId();
        int int77 = logMark57.compare(logMark68);
        long long78 = logMark57.getLogFileId();
        long long79 = logMark57.getLogFileOffset();
        long long80 = logMark57.getLogFileId();
        int int81 = logMark43.compare(logMark57);
        logMark43.setLogMark(97L, (long) (short) -1);
        int int85 = logMark10.compare(logMark43);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertNotNull(logMark20);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 97L + "'", long21 == 97L);
        org.junit.Assert.assertTrue("'" + long22 + "' != '" + 97L + "'", long22 == 97L);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + (-1) + "'", int23 == (-1));
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
        org.junit.Assert.assertTrue("'" + int28 + "' != '" + 1 + "'", int28 == 1);
        org.junit.Assert.assertTrue("'" + int36 + "' != '" + 0 + "'", int36 == 0);
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + 10L + "'", long40 == 10L);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long51 + "' != '" + 10L + "'", long51 == 10L);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 0 + "'", int52 == 0);
        org.junit.Assert.assertTrue("'" + long53 + "' != '" + 10L + "'", long53 == 10L);
        org.junit.Assert.assertTrue("'" + long54 + "' != '" + 10L + "'", long54 == 10L);
        org.junit.Assert.assertTrue("'" + int61 + "' != '" + 0 + "'", int61 == 0);
        org.junit.Assert.assertTrue("'" + long65 + "' != '" + 10L + "'", long65 == 10L);
        org.junit.Assert.assertTrue("'" + int72 + "' != '" + 0 + "'", int72 == 0);
        org.junit.Assert.assertTrue("'" + long76 + "' != '" + 10L + "'", long76 == 10L);
        org.junit.Assert.assertTrue("'" + int77 + "' != '" + 0 + "'", int77 == 0);
        org.junit.Assert.assertTrue("'" + long78 + "' != '" + 10L + "'", long78 == 10L);
        org.junit.Assert.assertTrue("'" + long79 + "' != '" + 10L + "'", long79 == 10L);
        org.junit.Assert.assertTrue("'" + long80 + "' != '" + 10L + "'", long80 == 10L);
        org.junit.Assert.assertTrue("'" + int81 + "' != '" + 0 + "'", int81 == 0);
        org.junit.Assert.assertTrue("'" + int85 + "' != '" + (-1) + "'", int85 == (-1));
    }

    @Test
    public void test1100() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1100");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        logMark2.setLogMark((long) 0, (long) '#');
        long long13 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long18 = logMark17.getLogFileId();
        int int19 = logMark14.compare(logMark17);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 35L + "'", long13 == 35L);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 100L + "'", long18 == 100L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
    }

    @Test
    public void test1101() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1101");
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
        logMark10.setLogMark((long) 'a', (long) (byte) 1);
        logMark10.setLogMark((long) '#', (long) 1);
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 97L + "'", long9 == 97L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str11, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
    }

    @Test
    public void test1102() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1102");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark((long) 100, (long) 'a');
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long7 = logMark6.getLogFileOffset();
        int int8 = logMark0.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        java.lang.String str10 = logMark6.toString();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 97L + "'", long2 == 97L);
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str10, "LogMark: logFileId - 100 , logFileOffset - 97");
    }

    @Test
    public void test1103() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1103");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(32L, (long) (byte) -1);
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
    public void test1104() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1104");
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
        logMark32.setLogMark((long) (short) -1, (long) (short) -1);
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
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str36, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long51 + "' != '" + 10L + "'", long51 == 10L);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + (-1) + "'", int52 == (-1));
        org.junit.Assert.assertTrue("'" + int59 + "' != '" + (-1) + "'", int59 == (-1));
        org.junit.Assert.assertTrue("'" + int60 + "' != '" + (-1) + "'", int60 == (-1));
    }

    @Test
    public void test1105() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1105");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        logMark8.setLogMark(10L, 0L);
        logMark8.setLogMark((long) 100, (long) 'a');
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - -1" + "'", str10, "LogMark: logFileId - -1 , logFileOffset - -1");
    }

    @Test
    public void test1106() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1106");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 9223372036854775807L);
        logMark2.setLogMark((long) ' ', (long) (short) 0);
    }

    @Test
    public void test1107() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1107");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) (byte) 1);
    }

    @Test
    public void test1108() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1108");
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
        long long58 = logMark57.getLogFileId();
        java.lang.Class<?> wildcardClass59 = logMark57.getClass();
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
        org.junit.Assert.assertTrue("'" + long58 + "' != '" + 10L + "'", long58 == 10L);
        org.junit.Assert.assertNotNull(wildcardClass59);
    }

    @Test
    public void test1109() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1109");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 9223372036854775807L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark4.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 9223372036854775807L + "'", long3 == 9223372036854775807L);
    }

    @Test
    public void test1110() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1110");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark((long) 100, (long) 'a');
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long7 = logMark6.getLogFileOffset();
        int int8 = logMark0.compare(logMark6);
        long long9 = logMark6.getLogFileOffset();
        long long10 = logMark6.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + (-1L) + "'", long2 == (-1L));
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 97L + "'", long9 == 97L);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 97L + "'", long10 == 97L);
    }

    @Test
    public void test1111() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1111");
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
        long long30 = logMark13.getLogFileOffset();
        long long31 = logMark13.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        long long33 = logMark32.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer34 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark32.readLogMark(byteBuffer34);
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
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 100L + "'", long28 == 100L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 10L + "'", long30 == 10L);
        org.junit.Assert.assertTrue("'" + long31 + "' != '" + 10L + "'", long31 == 10L);
        org.junit.Assert.assertTrue("'" + long33 + "' != '" + 10L + "'", long33 == 10L);
    }

    @Test
    public void test1112() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1112");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark6);
        java.lang.String str8 = logMark6.toString();
        java.lang.Class<?> wildcardClass9 = logMark6.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str8, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test1113() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1113");
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
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark34 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int35 = logMark31.compare(logMark34);
        logMark31.setLogMark((long) (short) 10, 10L);
        long long39 = logMark31.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark42 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark45 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int46 = logMark42.compare(logMark45);
        logMark42.setLogMark((long) (short) 10, 10L);
        long long50 = logMark42.getLogFileId();
        int int51 = logMark31.compare(logMark42);
        long long52 = logMark42.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark55 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark56 = new org.apache.bookkeeper.bookie.LogMark(logMark55);
        java.lang.String str57 = logMark56.toString();
        logMark56.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark61 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark62 = new org.apache.bookkeeper.bookie.LogMark(logMark61);
        int int63 = logMark56.compare(logMark61);
        int int64 = logMark42.compare(logMark61);
        org.apache.bookkeeper.bookie.LogMark logMark67 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark70 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int71 = logMark67.compare(logMark70);
        org.apache.bookkeeper.bookie.LogMark logMark72 = new org.apache.bookkeeper.bookie.LogMark(logMark67);
        org.apache.bookkeeper.bookie.LogMark logMark73 = new org.apache.bookkeeper.bookie.LogMark(logMark72);
        int int74 = logMark42.compare(logMark73);
        long long75 = logMark42.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark78 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark79 = new org.apache.bookkeeper.bookie.LogMark(logMark78);
        java.lang.String str80 = logMark78.toString();
        int int81 = logMark42.compare(logMark78);
        java.lang.String str82 = logMark78.toString();
        java.lang.String str83 = logMark78.toString();
        int int84 = logMark16.compare(logMark78);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 0 + "'", int35 == 0);
        org.junit.Assert.assertTrue("'" + long39 + "' != '" + 10L + "'", long39 == 10L);
        org.junit.Assert.assertTrue("'" + int46 + "' != '" + 0 + "'", int46 == 0);
        org.junit.Assert.assertTrue("'" + long50 + "' != '" + 10L + "'", long50 == 10L);
        org.junit.Assert.assertTrue("'" + int51 + "' != '" + 0 + "'", int51 == 0);
        org.junit.Assert.assertTrue("'" + long52 + "' != '" + 10L + "'", long52 == 10L);
        org.junit.Assert.assertEquals("'" + str57 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str57, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark61);
        org.junit.Assert.assertTrue("'" + int63 + "' != '" + (-1) + "'", int63 == (-1));
        org.junit.Assert.assertTrue("'" + int64 + "' != '" + (-1) + "'", int64 == (-1));
        org.junit.Assert.assertTrue("'" + int71 + "' != '" + 0 + "'", int71 == 0);
        org.junit.Assert.assertTrue("'" + int74 + "' != '" + (-1) + "'", int74 == (-1));
        org.junit.Assert.assertTrue("'" + long75 + "' != '" + 10L + "'", long75 == 10L);
        org.junit.Assert.assertEquals("'" + str80 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str80, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int81 + "' != '" + 1 + "'", int81 == 1);
        org.junit.Assert.assertEquals("'" + str82 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str82, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str83 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str83, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int84 + "' != '" + 1 + "'", int84 == 1);
    }

    @Test
    public void test1114() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1114");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        long long13 = logMark8.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer14 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.writeLogMark(byteBuffer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str10, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 97L + "'", long13 == 97L);
    }

    @Test
    public void test1115() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1115");
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
        java.lang.String str19 = logMark3.toString();
        logMark3.setLogMark((long) (short) 0, (long) 100);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 0L + "'", long14 == 0L);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 0L + "'", long15 == 0L);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - -1" + "'", str19, "LogMark: logFileId - 1 , logFileOffset - -1");
    }

    @Test
    public void test1116() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1116");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, 35L);
        java.lang.String str3 = logMark2.toString();
        java.lang.Class<?> wildcardClass4 = logMark2.getClass();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 35" + "'", str3, "LogMark: logFileId - 97 , logFileOffset - 35");
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test1117() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1117");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        logMark8.setLogMark((long) '4', 0L);
        java.lang.Class<?> wildcardClass16 = logMark8.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str10, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(wildcardClass16);
    }

    @Test
    public void test1118() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1118");
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
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark(logMark22);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + (-1) + "'", int21 == (-1));
    }

    @Test
    public void test1119() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1119");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(32L, (long) 10);
        java.lang.String str3 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 10" + "'", str3, "LogMark: logFileId - 32 , logFileOffset - 10");
    }

    @Test
    public void test1120() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1120");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 100, (long) (byte) 0);
    }

    @Test
    public void test1121() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1121");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        java.lang.String str6 = logMark5.toString();
        int int7 = logMark3.compare(logMark5);
        logMark3.setLogMark((long) 100, 0L);
        long long11 = logMark3.getLogFileId();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 35L + "'", long4 == 35L);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str6, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 100L + "'", long11 == 100L);
    }

    @Test
    public void test1122() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1122");
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
        java.lang.Class<?> wildcardClass21 = logMark12.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 1L + "'", long14 == 1L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 35L + "'", long19 == 35L);
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 35L + "'", long20 == 35L);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test1123() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1123");
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
        java.lang.String str12 = logMark0.toString();
        long long13 = logMark0.getLogFileId();
        long long14 = logMark0.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 97L + "'", long9 == 97L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str12, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 100L + "'", long13 == 100L);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 97L + "'", long14 == 97L);
    }

    @Test
    public void test1124() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1124");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark1.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        logMark6.setLogMark((long) '4', (long) 10);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
    }

    @Test
    public void test1125() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1125");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 100, 35L);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test1126() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1126");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark9.readLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
    }

    @Test
    public void test1127() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1127");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long4 = logMark0.getLogFileOffset();
        long long5 = logMark0.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 97L + "'", long2 == 97L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 97L + "'", long4 == 97L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 100L + "'", long5 == 100L);
    }

    @Test
    public void test1128() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1128");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) (byte) 10);
        long long3 = logMark2.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 10L + "'", long3 == 10L);
    }

    @Test
    public void test1129() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1129");
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
        logMark35.setLogMark((long) '#', (long) (byte) -1);
        long long39 = logMark35.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertNotNull(logMark25);
        org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str27, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark29);
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 97L + "'", long30 == 97L);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 0 + "'", int33 == 0);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
        org.junit.Assert.assertTrue("'" + long39 + "' != '" + (-1L) + "'", long39 == (-1L));
    }

    @Test
    public void test1130() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1130");
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
        logMark27.setLogMark((long) 'a', (long) 100);
        java.lang.String str62 = logMark27.toString();
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + (-1) + "'", int7 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertNotNull(logMark15);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + (-1) + "'", int17 == (-1));
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
        org.junit.Assert.assertEquals("'" + str62 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 100" + "'", str62, "LogMark: logFileId - 97 , logFileOffset - 100");
    }

    @Test
    public void test1131() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1131");
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
        org.apache.bookkeeper.bookie.LogMark logMark60 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long61 = logMark60.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark62 = new org.apache.bookkeeper.bookie.LogMark(logMark60);
        org.apache.bookkeeper.bookie.LogMark logMark63 = new org.apache.bookkeeper.bookie.LogMark(logMark60);
        org.apache.bookkeeper.bookie.LogMark logMark66 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), (long) 10);
        int int67 = logMark60.compare(logMark66);
        int int68 = logMark34.compare(logMark60);
        org.apache.bookkeeper.bookie.LogMark logMark71 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark72 = new org.apache.bookkeeper.bookie.LogMark(logMark71);
        java.lang.String str73 = logMark72.toString();
        logMark72.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark77 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark78 = new org.apache.bookkeeper.bookie.LogMark(logMark77);
        int int79 = logMark72.compare(logMark77);
        long long80 = logMark77.getLogFileId();
        int int81 = logMark60.compare(logMark77);
        logMark60.setLogMark((long) (byte) 1, (long) (short) 1);
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 97L + "'", long4 == 97L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 97L + "'", long5 == 97L);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + (-1) + "'", int6 == (-1));
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
        org.junit.Assert.assertTrue("'" + int59 + "' != '" + 1 + "'", int59 == 1);
        org.junit.Assert.assertNotNull(logMark60);
        org.junit.Assert.assertTrue("'" + long61 + "' != '" + 97L + "'", long61 == 97L);
        org.junit.Assert.assertTrue("'" + int67 + "' != '" + 1 + "'", int67 == 1);
        org.junit.Assert.assertTrue("'" + int68 + "' != '" + (-1) + "'", int68 == (-1));
        org.junit.Assert.assertEquals("'" + str73 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str73, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark77);
        org.junit.Assert.assertTrue("'" + int79 + "' != '" + (-1) + "'", int79 == (-1));
        org.junit.Assert.assertTrue("'" + long80 + "' != '" + 100L + "'", long80 == 100L);
        org.junit.Assert.assertTrue("'" + int81 + "' != '" + 0 + "'", int81 == 0);
    }

    @Test
    public void test1132() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1132");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), (long) '#');
        long long3 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
    }

    @Test
    public void test1133() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1133");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str6 = logMark5.toString();
        logMark5.setLogMark((long) 10, (long) (byte) -1);
        long long10 = logMark5.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str6, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + (-1L) + "'", long10 == (-1L));
    }

    @Test
    public void test1134() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1134");
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
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        long long28 = logMark26.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark32.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark36 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        int int37 = logMark26.compare(logMark36);
        long long38 = logMark36.getLogFileId();
        int int39 = logMark2.compare(logMark36);
        long long40 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 0L + "'", long28 == 0L);
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + 1 + "'", int37 == 1);
        org.junit.Assert.assertTrue("'" + long38 + "' != '" + 1L + "'", long38 == 1L);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + 1 + "'", int39 == 1);
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + 10L + "'", long40 == 10L);
    }

    @Test
    public void test1135() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1135");
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
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark17);
        logMark19.setLogMark((long) (short) 1, 35L);
        logMark19.setLogMark((long) 100, (long) ' ');
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark9);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 1L + "'", long10 == 1L);
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 1" + "'", str12, "LogMark: logFileId - 1 , logFileOffset - 1");
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 32L + "'", long16 == 32L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
    }

    @Test
    public void test1136() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1136");
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
        logMark3.setLogMark((long) (-1), (long) '#');
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + (-1L) + "'", long13 == (-1L));
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
    }

    @Test
    public void test1137() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1137");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 0, (long) (byte) 0);
        logMark2.setLogMark(100L, (long) 0);
    }

    @Test
    public void test1138() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1138");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        java.lang.String str5 = logMark2.toString();
        java.lang.String str6 = logMark2.toString();
        logMark2.setLogMark(35L, (long) (short) 100);
        java.lang.String str10 = logMark2.toString();
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str5, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str6, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 100" + "'", str10, "LogMark: logFileId - 35 , logFileOffset - 100");
    }

    @Test
    public void test1139() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1139");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long5 = logMark4.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        int int8 = logMark3.compare(logMark7);
        logMark3.setLogMark((long) (short) 0, (long) (byte) 100);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 1" + "'", str2, "LogMark: logFileId - 1 , logFileOffset - 1");
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 1L + "'", long5 == 1L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
    }

    @Test
    public void test1140() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1140");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 'a');
        java.lang.String str3 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 97" + "'", str3, "LogMark: logFileId - 32 , logFileOffset - 97");
    }

    @Test
    public void test1141() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1141");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, 1L);
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
    public void test1142() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1142");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark5.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark();
        int int11 = logMark5.compare(logMark10);
        long long12 = logMark5.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + 1 + "'", int11 == 1);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 1L + "'", long12 == 1L);
    }

    @Test
    public void test1143() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1143");
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
        logMark3.setLogMark(100L, 10L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 0 + "'", int18 == 0);
        org.junit.Assert.assertNotNull(logMark21);
        org.junit.Assert.assertTrue("'" + long22 + "' != '" + 1L + "'", long22 == 1L);
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 1" + "'", str24, "LogMark: logFileId - 1 , logFileOffset - 1");
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 32L + "'", long28 == 32L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 1 + "'", int30 == 1);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + (-1) + "'", int31 == (-1));
    }

    @Test
    public void test1144() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1144");
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
        int int42 = logMark13.compare(logMark35);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 0L + "'", long29 == 0L);
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 1 + "'", int32 == 1);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + 0 + "'", int39 == 0);
        org.junit.Assert.assertTrue("'" + int41 + "' != '" + (-1) + "'", int41 == (-1));
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + (-1) + "'", int42 == (-1));
    }

    @Test
    public void test1145() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1145");
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
        long long36 = logMark32.getLogFileId();
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
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 1L + "'", long36 == 1L);
    }

    @Test
    public void test1146() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1146");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        java.lang.String str9 = logMark2.toString();
        logMark2.setLogMark((long) 10, 35L);
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
        long long36 = logMark26.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark39 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark40 = new org.apache.bookkeeper.bookie.LogMark(logMark39);
        java.lang.String str41 = logMark40.toString();
        logMark40.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark45 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark(logMark45);
        int int47 = logMark40.compare(logMark45);
        int int48 = logMark26.compare(logMark45);
        org.apache.bookkeeper.bookie.LogMark logMark51 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark54 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int55 = logMark51.compare(logMark54);
        org.apache.bookkeeper.bookie.LogMark logMark56 = new org.apache.bookkeeper.bookie.LogMark(logMark51);
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark(logMark56);
        int int58 = logMark26.compare(logMark57);
        long long59 = logMark26.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark62 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark63 = new org.apache.bookkeeper.bookie.LogMark(logMark62);
        java.lang.String str64 = logMark62.toString();
        int int65 = logMark26.compare(logMark62);
        java.lang.String str66 = logMark62.toString();
        long long67 = logMark62.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark70 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark71 = new org.apache.bookkeeper.bookie.LogMark(logMark70);
        java.lang.String str72 = logMark71.toString();
        logMark71.setLogMark((long) (-1), (long) (byte) 100);
        org.apache.bookkeeper.bookie.LogMark logMark76 = new org.apache.bookkeeper.bookie.LogMark(logMark71);
        int int77 = logMark62.compare(logMark76);
        int int78 = logMark2.compare(logMark76);
        java.nio.ByteBuffer byteBuffer79 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark76.writeLogMark(byteBuffer79);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str9, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 10L + "'", long34 == 10L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 0 + "'", int35 == 0);
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 10L + "'", long36 == 10L);
        org.junit.Assert.assertEquals("'" + str41 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str41, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark45);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + (-1) + "'", int47 == (-1));
        org.junit.Assert.assertTrue("'" + int48 + "' != '" + 1 + "'", int48 == 1);
        org.junit.Assert.assertTrue("'" + int55 + "' != '" + 0 + "'", int55 == 0);
        org.junit.Assert.assertTrue("'" + int58 + "' != '" + (-1) + "'", int58 == (-1));
        org.junit.Assert.assertTrue("'" + long59 + "' != '" + 10L + "'", long59 == 10L);
        org.junit.Assert.assertEquals("'" + str64 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str64, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int65 + "' != '" + 1 + "'", int65 == 1);
        org.junit.Assert.assertEquals("'" + str66 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str66, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long67 + "' != '" + 0L + "'", long67 == 0L);
        org.junit.Assert.assertEquals("'" + str72 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str72, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int77 + "' != '" + 1 + "'", int77 == 1);
        org.junit.Assert.assertTrue("'" + int78 + "' != '" + 1 + "'", int78 == 1);
    }

    @Test
    public void test1147() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1147");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 10, (long) (byte) 0);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test1148() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1148");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), (long) 10);
        int int7 = logMark0.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long9 = logMark8.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        logMark8.setLogMark((long) ' ', (long) ' ');
        long long14 = logMark8.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int16 = logMark6.compare(logMark15);
        logMark6.setLogMark((long) 10, 1L);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 1L + "'", long1 == 1L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 1L + "'", long9 == 1L);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 32L + "'", long14 == 32L);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
    }

    @Test
    public void test1149() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1149");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(52L, (long) 0);
    }

    @Test
    public void test1150() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1150");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), (long) ' ');
        logMark2.setLogMark((long) (byte) 0, 100L);
        long long6 = logMark2.getLogFileId();
        java.lang.Class<?> wildcardClass7 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test1151() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1151");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, 35L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long5 = logMark4.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        long long8 = logMark4.getLogFileId();
        long long9 = logMark4.getLogFileOffset();
        int int10 = logMark2.compare(logMark4);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 32L + "'", long5 == 32L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 32L + "'", long8 == 32L);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 32L + "'", long9 == 32L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
    }

    @Test
    public void test1152() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1152");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark2.setLogMark((long) (byte) 100, (long) (short) 0);
        logMark2.setLogMark(32L, 9223372036854775807L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, 35L);
        int int12 = logMark2.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        org.apache.bookkeeper.bookie.LogMark logMark17 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long18 = logMark17.getLogFileOffset();
        java.lang.String str19 = logMark17.toString();
        org.apache.bookkeeper.bookie.LogMark logMark20 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark20);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        long long23 = logMark22.getLogFileId();
        int int24 = logMark17.compare(logMark22);
        org.apache.bookkeeper.bookie.LogMark logMark25 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long26 = logMark25.getLogFileOffset();
        int int27 = logMark17.compare(logMark25);
        long long28 = logMark25.getLogFileOffset();
        int int29 = logMark15.compare(logMark25);
        int int30 = logMark11.compare(logMark15);
        java.nio.ByteBuffer byteBuffer31 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark15.readLogMark(byteBuffer31);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 1 + "'", int12 == 1);
        org.junit.Assert.assertNotNull(logMark17);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 32L + "'", long18 == 32L);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str19, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertNotNull(logMark20);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 32L + "'", long23 == 32L);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertNotNull(logMark25);
        org.junit.Assert.assertTrue("'" + long26 + "' != '" + 32L + "'", long26 == 32L);
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 0 + "'", int27 == 0);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 32L + "'", long28 == 32L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + (-1) + "'", int30 == (-1));
    }

    @Test
    public void test1153() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1153");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(97L, (long) (short) 0);
        int int10 = logMark6.compare(logMark9);
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark9.readLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
    }

    @Test
    public void test1154() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1154");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, (long) (short) 1);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test1155() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1155");
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
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark(logMark20);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 0 + "'", int18 == 0);
        org.junit.Assert.assertNotNull(logMark21);
        org.junit.Assert.assertTrue("'" + long22 + "' != '" + 32L + "'", long22 == 32L);
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str24, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 32L + "'", long28 == 32L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 1 + "'", int30 == 1);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + (-1) + "'", int31 == (-1));
    }

    @Test
    public void test1156() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1156");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark8.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int13 = logMark2.compare(logMark12);
        long long14 = logMark12.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark15 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long16 = logMark15.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        java.lang.String str19 = logMark18.toString();
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark18);
        int int21 = logMark12.compare(logMark18);
        long long22 = logMark18.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 1L + "'", long14 == 1L);
        org.junit.Assert.assertNotNull(logMark15);
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 32L + "'", long16 == 32L);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str19, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + (-1) + "'", int21 == (-1));
        org.junit.Assert.assertTrue("'" + long22 + "' != '" + 32L + "'", long22 == 32L);
    }

    @Test
    public void test1157() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1157");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 100, (long) ' ');
    }

    @Test
    public void test1158() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1158");
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
        org.apache.bookkeeper.bookie.LogMark logMark56 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark(logMark56);
        logMark57.setLogMark(10L, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark61 = new org.apache.bookkeeper.bookie.LogMark(logMark57);
        org.apache.bookkeeper.bookie.LogMark logMark62 = new org.apache.bookkeeper.bookie.LogMark(logMark57);
        int int63 = logMark53.compare(logMark57);
        java.nio.ByteBuffer byteBuffer64 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark53.readLogMark(byteBuffer64);
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
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str36, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long51 + "' != '" + 10L + "'", long51 == 10L);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + (-1) + "'", int52 == (-1));
        org.junit.Assert.assertTrue("'" + int63 + "' != '" + (-1) + "'", int63 == (-1));
    }

    @Test
    public void test1159() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1159");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 10, 100L);
        logMark2.setLogMark(0L, 9223372036854775807L);
    }

    @Test
    public void test1160() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1160");
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
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        long long25 = logMark10.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + (-1) + "'", int7 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertNotNull(logMark15);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 1 + "'", int17 == 1);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 35L + "'", long21 == 35L);
        org.junit.Assert.assertEquals("'" + str22 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str22, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 0 + "'", int23 == 0);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 35L + "'", long25 == 35L);
    }

    @Test
    public void test1161() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1161");
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
        logMark13.setLogMark((long) 'a', (long) (byte) 10);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertNotNull(logMark25);
        org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str27, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark29);
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 10L + "'", long30 == 10L);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 0 + "'", int33 == 0);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 1 + "'", int34 == 1);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
    }

    @Test
    public void test1162() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1162");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        long long5 = logMark1.getLogFileId();
        long long6 = logMark1.getLogFileOffset();
        java.lang.Class<?> wildcardClass7 = logMark1.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + (-1L) + "'", long5 == (-1L));
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 10L + "'", long6 == 10L);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test1163() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1163");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
    }

    @Test
    public void test1164() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1164");
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
        java.lang.String str29 = logMark16.toString();
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertEquals("'" + str29 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str29, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test1165() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1165");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 100L);
        logMark2.setLogMark(0L, (long) 'a');
        long long6 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
    }

    @Test
    public void test1166() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1166");
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
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str4, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark13);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + 0 + "'", int15 == 0);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 0 + "'", int16 == 0);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
    }

    @Test
    public void test1167() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1167");
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
        logMark8.setLogMark(1L, (long) 'a');
        logMark8.setLogMark(0L, (long) (byte) 1);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + (-1L) + "'", long13 == (-1L));
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
    }

    @Test
    public void test1168() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1168");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        java.lang.String str8 = logMark5.toString();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((-1L), 10L);
        logMark13.setLogMark((long) 100, (long) (short) 0);
        logMark13.setLogMark((long) 100, 0L);
        int int20 = logMark9.compare(logMark13);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str8, "LogMark: logFileId - -1 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + (-1) + "'", int20 == (-1));
    }

    @Test
    public void test1169() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1169");
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
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        java.nio.ByteBuffer byteBuffer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark1.writeLogMark(byteBuffer19);
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
    }

    @Test
    public void test1170() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1170");
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
        logMark2.setLogMark(52L, (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark25);
        long long27 = logMark25.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark25);
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark32 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long33 = logMark32.getLogFileOffset();
        long long34 = logMark32.getLogFileOffset();
        int int35 = logMark31.compare(logMark32);
        long long36 = logMark32.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark39 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 0);
        long long40 = logMark39.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int47 = logMark43.compare(logMark46);
        org.apache.bookkeeper.bookie.LogMark logMark48 = new org.apache.bookkeeper.bookie.LogMark(logMark43);
        org.apache.bookkeeper.bookie.LogMark logMark49 = new org.apache.bookkeeper.bookie.LogMark(logMark48);
        org.apache.bookkeeper.bookie.LogMark logMark50 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long51 = logMark50.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark52 = new org.apache.bookkeeper.bookie.LogMark(logMark50);
        java.lang.String str53 = logMark52.toString();
        logMark52.setLogMark(32L, 9223372036854775807L);
        long long57 = logMark52.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark58 = new org.apache.bookkeeper.bookie.LogMark(logMark52);
        int int59 = logMark49.compare(logMark58);
        org.apache.bookkeeper.bookie.LogMark logMark60 = new org.apache.bookkeeper.bookie.LogMark(logMark58);
        int int61 = logMark39.compare(logMark58);
        long long62 = logMark58.getLogFileId();
        int int63 = logMark32.compare(logMark58);
        int int64 = logMark28.compare(logMark58);
        long long65 = logMark58.getLogFileOffset();
        int int66 = logMark2.compare(logMark58);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 100L + "'", long7 == 100L);
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 10L + "'", long12 == 10L);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + 1 + "'", int15 == 1);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
        org.junit.Assert.assertTrue("'" + long27 + "' != '" + 0L + "'", long27 == 0L);
        org.junit.Assert.assertNotNull(logMark32);
        org.junit.Assert.assertTrue("'" + long33 + "' != '" + 10L + "'", long33 == 10L);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 10L + "'", long34 == 10L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 1 + "'", int35 == 1);
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + (-1L) + "'", long36 == (-1L));
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + 32L + "'", long40 == 32L);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertNotNull(logMark50);
        org.junit.Assert.assertTrue("'" + long51 + "' != '" + 10L + "'", long51 == 10L);
        org.junit.Assert.assertEquals("'" + str53 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str53, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long57 + "' != '" + 32L + "'", long57 == 32L);
        org.junit.Assert.assertTrue("'" + int59 + "' != '" + 1 + "'", int59 == 1);
        org.junit.Assert.assertTrue("'" + int61 + "' != '" + (-1) + "'", int61 == (-1));
        org.junit.Assert.assertTrue("'" + long62 + "' != '" + 32L + "'", long62 == 32L);
        org.junit.Assert.assertTrue("'" + int63 + "' != '" + (-1) + "'", int63 == (-1));
        org.junit.Assert.assertTrue("'" + int64 + "' != '" + 1 + "'", int64 == 1);
        org.junit.Assert.assertTrue("'" + long65 + "' != '" + 9223372036854775807L + "'", long65 == 9223372036854775807L);
        org.junit.Assert.assertTrue("'" + int66 + "' != '" + 1 + "'", int66 == 1);
    }

    @Test
    public void test1171() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1171");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        long long10 = logMark7.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 0L + "'", long10 == 0L);
    }

    @Test
    public void test1172() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1172");
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
        logMark14.setLogMark((long) (short) 10, (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 52L + "'", long18 == 52L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
    }

    @Test
    public void test1173() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1173");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        long long9 = logMark8.getLogFileId();
        long long10 = logMark8.getLogFileId();
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.writeLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + (-1L) + "'", long9 == (-1L));
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + (-1L) + "'", long10 == (-1L));
    }

    @Test
    public void test1174() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1174");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(10L, 1L);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test1175() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1175");
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
        logMark13.setLogMark(97L, (long) (short) -1);
        java.nio.ByteBuffer byteBuffer55 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark13.writeLogMark(byteBuffer55);
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
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long48 + "' != '" + 10L + "'", long48 == 10L);
        org.junit.Assert.assertTrue("'" + long49 + "' != '" + 10L + "'", long49 == 10L);
        org.junit.Assert.assertTrue("'" + long50 + "' != '" + 10L + "'", long50 == 10L);
        org.junit.Assert.assertTrue("'" + int51 + "' != '" + 0 + "'", int51 == 0);
    }

    @Test
    public void test1176() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1176");
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
        logMark7.setLogMark(10L, 10L);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 52L + "'", long18 == 52L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
    }

    @Test
    public void test1177() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1177");
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
        java.nio.ByteBuffer byteBuffer14 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.readLogMark(byteBuffer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str10, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str13, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test1178() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1178");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 0, 97L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass4 = logMark3.getClass();
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test1179() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1179");
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
        logMark26.setLogMark(1L, 32L);
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
        org.junit.Assert.assertEquals("'" + str50 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str50, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str53 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str53, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int54 + "' != '" + 1 + "'", int54 == 1);
    }

    @Test
    public void test1180() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1180");
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
        org.apache.bookkeeper.bookie.LogMark logMark18 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long19 = logMark18.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark18);
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark18);
        java.lang.String str22 = logMark21.toString();
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark25);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        org.apache.bookkeeper.bookie.LogMark logMark30 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark(logMark30);
        java.lang.String str32 = logMark31.toString();
        logMark31.setLogMark((long) (-1), (long) (byte) 100);
        long long36 = logMark31.getLogFileId();
        logMark31.setLogMark((long) '4', (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark40 = new org.apache.bookkeeper.bookie.LogMark(logMark31);
        int int41 = logMark26.compare(logMark31);
        int int42 = logMark21.compare(logMark31);
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        int int44 = logMark13.compare(logMark43);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 97L + "'", long14 == 97L);
        org.junit.Assert.assertNotNull(logMark18);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 10L + "'", long19 == 10L);
        org.junit.Assert.assertEquals("'" + str22 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str22, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str32 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str32, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + (-1L) + "'", long36 == (-1L));
        org.junit.Assert.assertTrue("'" + int41 + "' != '" + 1 + "'", int41 == 1);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + (-1) + "'", int42 == (-1));
        org.junit.Assert.assertTrue("'" + int44 + "' != '" + 1 + "'", int44 == 1);
    }

    @Test
    public void test1181() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1181");
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
        logMark2.setLogMark((long) (short) -1, (long) ' ');
        java.lang.Class<?> wildcardClass22 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 0L + "'", long14 == 0L);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + 1 + "'", int15 == 1);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + (-1L) + "'", long17 == (-1L));
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertNotNull(wildcardClass22);
    }

    @Test
    public void test1182() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1182");
        org.apache.bookkeeper.bookie.LogMark logMark0 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
    }

    @Test
    public void test1183() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1183");
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
        java.nio.ByteBuffer byteBuffer16 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark12.readLogMark(byteBuffer16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 1L + "'", long15 == 1L);
    }

    @Test
    public void test1184() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1184");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) 0);
    }

    @Test
    public void test1185() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1185");
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
        org.apache.bookkeeper.bookie.LogMark logMark37 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 0);
        long long38 = logMark37.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int45 = logMark41.compare(logMark44);
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark(logMark41);
        org.apache.bookkeeper.bookie.LogMark logMark47 = new org.apache.bookkeeper.bookie.LogMark(logMark46);
        org.apache.bookkeeper.bookie.LogMark logMark48 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long49 = logMark48.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark50 = new org.apache.bookkeeper.bookie.LogMark(logMark48);
        java.lang.String str51 = logMark50.toString();
        logMark50.setLogMark(32L, 9223372036854775807L);
        long long55 = logMark50.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark56 = new org.apache.bookkeeper.bookie.LogMark(logMark50);
        int int57 = logMark47.compare(logMark56);
        org.apache.bookkeeper.bookie.LogMark logMark58 = new org.apache.bookkeeper.bookie.LogMark(logMark56);
        int int59 = logMark37.compare(logMark56);
        int int60 = logMark13.compare(logMark56);
        logMark56.setLogMark((long) 100, (long) 'a');
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertNotNull(logMark25);
        org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str27, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark29);
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 10L + "'", long30 == 10L);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 0 + "'", int33 == 0);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 1 + "'", int34 == 1);
        org.junit.Assert.assertTrue("'" + long38 + "' != '" + 32L + "'", long38 == 32L);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + 0 + "'", int45 == 0);
        org.junit.Assert.assertNotNull(logMark48);
        org.junit.Assert.assertTrue("'" + long49 + "' != '" + 10L + "'", long49 == 10L);
        org.junit.Assert.assertEquals("'" + str51 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str51, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long55 + "' != '" + 32L + "'", long55 == 32L);
        org.junit.Assert.assertTrue("'" + int57 + "' != '" + 1 + "'", int57 == 1);
        org.junit.Assert.assertTrue("'" + int59 + "' != '" + (-1) + "'", int59 == (-1));
        org.junit.Assert.assertTrue("'" + int60 + "' != '" + (-1) + "'", int60 == (-1));
    }

    @Test
    public void test1186() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1186");
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
        long long46 = logMark44.getLogFileId();
        java.nio.ByteBuffer byteBuffer47 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark44.writeLogMark(byteBuffer47);
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
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + (-1) + "'", int45 == (-1));
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 97L + "'", long46 == 97L);
    }

    @Test
    public void test1187() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1187");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(10L, 32L);
    }

    @Test
    public void test1188() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1188");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), (long) 10);
        int int7 = logMark0.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long9 = logMark8.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        logMark8.setLogMark((long) ' ', (long) ' ');
        long long14 = logMark8.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int16 = logMark6.compare(logMark15);
        java.lang.Class<?> wildcardClass17 = logMark6.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 10L + "'", long9 == 10L);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 32L + "'", long14 == 32L);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test1189() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1189");
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
        java.lang.String str65 = logMark8.toString();
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
        org.junit.Assert.assertEquals("'" + str65 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str65, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test1190() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1190");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        java.lang.String str9 = logMark2.toString();
        logMark2.setLogMark((long) 10, 35L);
        logMark2.setLogMark((long) (byte) 100, (long) (short) 10);
        logMark2.setLogMark(0L, 35L);
        java.lang.Class<?> wildcardClass19 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str9, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test1191() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1191");
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
        long long40 = logMark2.getLogFileId();
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
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + 1L + "'", long40 == 1L);
    }

    @Test
    public void test1192() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1192");
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
        java.lang.Class<?> wildcardClass47 = logMark13.getClass();
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
        org.junit.Assert.assertNotNull(wildcardClass47);
    }

    @Test
    public void test1193() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1193");
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
        java.nio.ByteBuffer byteBuffer35 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark28.readLogMark(byteBuffer35);
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
        org.junit.Assert.assertNotNull(logMark23);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 32L + "'", long24 == 32L);
        org.junit.Assert.assertEquals("'" + str25 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str25, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertNotNull(logMark26);
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 32L + "'", long29 == 32L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
    }

    @Test
    public void test1194() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1194");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.String str6 = logMark5.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.writeLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 32L + "'", long1 == 32L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str4, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str6, "LogMark: logFileId - 32 , logFileOffset - 32");
    }

    @Test
    public void test1195() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1195");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        long long9 = logMark5.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.writeLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 0L + "'", long9 == 0L);
    }

    @Test
    public void test1196() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1196");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        long long3 = logMark0.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long5 = logMark0.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.readLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 32L + "'", long2 == 32L);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 32L + "'", long5 == 32L);
    }

    @Test
    public void test1197() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1197");
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
        logMark16.setLogMark(0L, (long) 100);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
    }

    @Test
    public void test1198() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1198");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        long long3 = logMark2.getLogFileId();
        java.lang.String str4 = logMark2.toString();
        long long5 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        logMark7.setLogMark((long) (short) -1, 0L);
        long long11 = logMark7.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        long long16 = logMark14.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark17);
        int int19 = logMark14.compare(logMark18);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int26 = logMark22.compare(logMark25);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark22);
        int int28 = logMark18.compare(logMark22);
        int int29 = logMark7.compare(logMark22);
        logMark22.setLogMark(52L, 35L);
        int int33 = logMark2.compare(logMark22);
        java.lang.String str34 = logMark22.toString();
        java.lang.String str35 = logMark22.toString();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 1" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 1");
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 0L + "'", long5 == 0L);
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + (-1L) + "'", long11 == (-1L));
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 0L + "'", long16 == 0L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 1 + "'", int19 == 1);
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + 0 + "'", int26 == 0);
        org.junit.Assert.assertTrue("'" + int28 + "' != '" + (-1) + "'", int28 == (-1));
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + (-1) + "'", int33 == (-1));
        org.junit.Assert.assertEquals("'" + str34 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 35" + "'", str34, "LogMark: logFileId - 52 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str35 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 35" + "'", str35, "LogMark: logFileId - 52 , logFileOffset - 35");
    }

    @Test
    public void test1199() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1199");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark5.setLogMark((long) (short) 100, 35L);
        java.lang.String str9 = logMark5.toString();
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.writeLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 35" + "'", str9, "LogMark: logFileId - 100 , logFileOffset - 35");
    }

    @Test
    public void test1200() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1200");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 0, 9223372036854775807L);
        java.lang.String str3 = logMark2.toString();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 9223372036854775807" + "'", str3, "LogMark: logFileId - 0 , logFileOffset - 9223372036854775807");
    }

    @Test
    public void test1201() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1201");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long3 = logMark2.getLogFileId();
        logMark2.setLogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long8 = logMark7.getLogFileOffset();
        java.lang.String str9 = logMark7.toString();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 1 , logFileOffset - 0");
    }

    @Test
    public void test1202() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1202");
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
        long long30 = logMark13.getLogFileOffset();
        long long31 = logMark13.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
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
        long long56 = logMark46.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark59 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark60 = new org.apache.bookkeeper.bookie.LogMark(logMark59);
        java.lang.String str61 = logMark60.toString();
        logMark60.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark65 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark66 = new org.apache.bookkeeper.bookie.LogMark(logMark65);
        int int67 = logMark60.compare(logMark65);
        int int68 = logMark46.compare(logMark65);
        org.apache.bookkeeper.bookie.LogMark logMark71 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark74 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int75 = logMark71.compare(logMark74);
        org.apache.bookkeeper.bookie.LogMark logMark76 = new org.apache.bookkeeper.bookie.LogMark(logMark71);
        org.apache.bookkeeper.bookie.LogMark logMark77 = new org.apache.bookkeeper.bookie.LogMark(logMark76);
        int int78 = logMark46.compare(logMark77);
        long long79 = logMark46.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark82 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark83 = new org.apache.bookkeeper.bookie.LogMark(logMark82);
        java.lang.String str84 = logMark82.toString();
        int int85 = logMark46.compare(logMark82);
        org.apache.bookkeeper.bookie.LogMark logMark86 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark87 = new org.apache.bookkeeper.bookie.LogMark(logMark86);
        java.lang.String str88 = logMark87.toString();
        org.apache.bookkeeper.bookie.LogMark logMark89 = new org.apache.bookkeeper.bookie.LogMark(logMark87);
        int int90 = logMark46.compare(logMark87);
        int int91 = logMark32.compare(logMark87);
        long long92 = logMark87.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str23, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str24, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark25);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 32L + "'", long28 == 32L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 10L + "'", long30 == 10L);
        org.junit.Assert.assertTrue("'" + long31 + "' != '" + 10L + "'", long31 == 10L);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + 0 + "'", int39 == 0);
        org.junit.Assert.assertTrue("'" + long43 + "' != '" + 10L + "'", long43 == 10L);
        org.junit.Assert.assertTrue("'" + int50 + "' != '" + 0 + "'", int50 == 0);
        org.junit.Assert.assertTrue("'" + long54 + "' != '" + 10L + "'", long54 == 10L);
        org.junit.Assert.assertTrue("'" + int55 + "' != '" + 0 + "'", int55 == 0);
        org.junit.Assert.assertTrue("'" + long56 + "' != '" + 10L + "'", long56 == 10L);
        org.junit.Assert.assertEquals("'" + str61 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str61, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark65);
        org.junit.Assert.assertTrue("'" + int67 + "' != '" + (-1) + "'", int67 == (-1));
        org.junit.Assert.assertTrue("'" + int68 + "' != '" + (-1) + "'", int68 == (-1));
        org.junit.Assert.assertTrue("'" + int75 + "' != '" + 0 + "'", int75 == 0);
        org.junit.Assert.assertTrue("'" + int78 + "' != '" + (-1) + "'", int78 == (-1));
        org.junit.Assert.assertTrue("'" + long79 + "' != '" + 10L + "'", long79 == 10L);
        org.junit.Assert.assertEquals("'" + str84 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str84, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int85 + "' != '" + 1 + "'", int85 == 1);
        org.junit.Assert.assertNotNull(logMark86);
        org.junit.Assert.assertEquals("'" + str88 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str88, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertTrue("'" + int90 + "' != '" + (-1) + "'", int90 == (-1));
        org.junit.Assert.assertTrue("'" + int91 + "' != '" + (-1) + "'", int91 == (-1));
        org.junit.Assert.assertTrue("'" + long92 + "' != '" + 32L + "'", long92 == 32L);
    }

    @Test
    public void test1203() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1203");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, (long) (short) 100);
    }

    @Test
    public void test1204() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1204");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        logMark3.setLogMark((long) 10, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        logMark11.setLogMark((long) (short) -1, (long) 'a');
        long long15 = logMark11.getLogFileOffset();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 97L + "'", long15 == 97L);
    }

    @Test
    public void test1205() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1205");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark(0L, (long) (byte) 0);
        long long8 = logMark3.getLogFileId();
        long long9 = logMark3.getLogFileId();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 32L + "'", long1 == 32L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str4, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 0L + "'", long9 == 0L);
    }

    @Test
    public void test1206() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1206");
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
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        long long17 = logMark11.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer18 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark11.writeLogMark(byteBuffer18);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str2, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 32L + "'", long17 == 32L);
    }

    @Test
    public void test1207() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1207");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 10L);
    }

    @Test
    public void test1208() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1208");
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
        long long25 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int32 = logMark28.compare(logMark31);
        logMark28.setLogMark((long) (short) 10, 10L);
        long long36 = logMark28.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark39 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark42 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int43 = logMark39.compare(logMark42);
        logMark39.setLogMark((long) (short) 10, 10L);
        long long47 = logMark39.getLogFileId();
        int int48 = logMark28.compare(logMark39);
        long long49 = logMark39.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark52 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark53 = new org.apache.bookkeeper.bookie.LogMark(logMark52);
        java.lang.String str54 = logMark53.toString();
        logMark53.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark58 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark59 = new org.apache.bookkeeper.bookie.LogMark(logMark58);
        int int60 = logMark53.compare(logMark58);
        int int61 = logMark39.compare(logMark58);
        org.apache.bookkeeper.bookie.LogMark logMark64 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark67 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int68 = logMark64.compare(logMark67);
        org.apache.bookkeeper.bookie.LogMark logMark69 = new org.apache.bookkeeper.bookie.LogMark(logMark64);
        org.apache.bookkeeper.bookie.LogMark logMark70 = new org.apache.bookkeeper.bookie.LogMark(logMark69);
        int int71 = logMark39.compare(logMark70);
        int int72 = logMark13.compare(logMark39);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 10L + "'", long25 == 10L);
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 0 + "'", int32 == 0);
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 10L + "'", long36 == 10L);
        org.junit.Assert.assertTrue("'" + int43 + "' != '" + 0 + "'", int43 == 0);
        org.junit.Assert.assertTrue("'" + long47 + "' != '" + 10L + "'", long47 == 10L);
        org.junit.Assert.assertTrue("'" + int48 + "' != '" + 0 + "'", int48 == 0);
        org.junit.Assert.assertTrue("'" + long49 + "' != '" + 10L + "'", long49 == 10L);
        org.junit.Assert.assertEquals("'" + str54 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str54, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark58);
        org.junit.Assert.assertTrue("'" + int60 + "' != '" + (-1) + "'", int60 == (-1));
        org.junit.Assert.assertTrue("'" + int61 + "' != '" + (-1) + "'", int61 == (-1));
        org.junit.Assert.assertTrue("'" + int68 + "' != '" + 0 + "'", int68 == 0);
        org.junit.Assert.assertTrue("'" + int71 + "' != '" + (-1) + "'", int71 == (-1));
        org.junit.Assert.assertTrue("'" + int72 + "' != '" + 0 + "'", int72 == 0);
    }

    @Test
    public void test1209() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1209");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark((long) 100, (long) 'a');
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long7 = logMark6.getLogFileOffset();
        int int8 = logMark0.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        java.lang.String str13 = logMark12.toString();
        logMark12.setLogMark((long) (-1), (long) (byte) 100);
        long long17 = logMark12.getLogFileId();
        logMark12.setLogMark((long) '4', (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        int int22 = logMark6.compare(logMark12);
        java.lang.String str23 = logMark12.toString();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 32L + "'", long2 == 32L);
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str13, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + (-1L) + "'", long17 == (-1L));
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 52" + "'", str23, "LogMark: logFileId - 52 , logFileOffset - 52");
    }

    @Test
    public void test1210() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1210");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        long long3 = logMark0.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        logMark4.setLogMark((long) 0, (-1L));
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 97L + "'", long2 == 97L);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
    }

    @Test
    public void test1211() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1211");
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
        logMark65.setLogMark((long) (short) 0, 9223372036854775807L);
        java.lang.Class<?> wildcardClass73 = logMark65.getClass();
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
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str36, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long51 + "' != '" + 10L + "'", long51 == 10L);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + (-1) + "'", int52 == (-1));
        org.junit.Assert.assertEquals("'" + str58 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str58, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int69 + "' != '" + (-1) + "'", int69 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass73);
    }

    @Test
    public void test1212() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1212");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark((long) 100, (long) 'a');
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long7 = logMark6.getLogFileOffset();
        int int8 = logMark0.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        long long10 = logMark6.getLogFileOffset();
        java.lang.Class<?> wildcardClass11 = logMark6.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 10L + "'", long2 == 10L);
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 97L + "'", long10 == 97L);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test1213() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1213");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        long long3 = logMark2.getLogFileId();
        java.lang.String str4 = logMark2.toString();
        long long5 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        logMark7.setLogMark((long) (short) -1, 0L);
        long long11 = logMark7.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        long long16 = logMark14.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark17);
        int int19 = logMark14.compare(logMark18);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int26 = logMark22.compare(logMark25);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark22);
        int int28 = logMark18.compare(logMark22);
        int int29 = logMark7.compare(logMark22);
        logMark22.setLogMark(52L, 35L);
        int int33 = logMark2.compare(logMark22);
        java.lang.String str34 = logMark22.toString();
        java.lang.Class<?> wildcardClass35 = logMark22.getClass();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 1" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 1");
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 0L + "'", long5 == 0L);
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + (-1L) + "'", long11 == (-1L));
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 0L + "'", long16 == 0L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 1 + "'", int19 == 1);
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + 0 + "'", int26 == 0);
        org.junit.Assert.assertTrue("'" + int28 + "' != '" + (-1) + "'", int28 == (-1));
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + (-1) + "'", int33 == (-1));
        org.junit.Assert.assertEquals("'" + str34 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 35" + "'", str34, "LogMark: logFileId - 52 , logFileOffset - 35");
        org.junit.Assert.assertNotNull(wildcardClass35);
    }

    @Test
    public void test1214() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1214");
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
        long long19 = logMark8.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        long long23 = logMark22.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int30 = logMark26.compare(logMark29);
        logMark29.setLogMark(1L, (long) (byte) 1);
        long long34 = logMark29.getLogFileId();
        int int35 = logMark22.compare(logMark29);
        long long36 = logMark22.getLogFileOffset();
        int int37 = logMark8.compare(logMark22);
        java.lang.Class<?> wildcardClass38 = logMark22.getClass();
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + (-1L) + "'", long13 == (-1L));
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 52L + "'", long19 == 52L);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 0L + "'", long23 == 0L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 1L + "'", long34 == 1L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 1L + "'", long36 == 1L);
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + 1 + "'", int37 == 1);
        org.junit.Assert.assertNotNull(wildcardClass38);
    }

    @Test
    public void test1215() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1215");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        long long5 = logMark3.getLogFileOffset();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 0L + "'", long5 == 0L);
    }

    @Test
    public void test1216() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1216");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) 10);
        java.lang.String str3 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark7);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 10" + "'", str3, "LogMark: logFileId - 0 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
    }

    @Test
    public void test1217() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1217");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 100, (long) (byte) -1);
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
    public void test1218() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1218");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long4 = logMark3.getLogFileOffset();
        long long5 = logMark3.getLogFileOffset();
        int int6 = logMark2.compare(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 97L + "'", long4 == 97L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 97L + "'", long5 == 97L);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + (-1) + "'", int6 == (-1));
    }

    @Test
    public void test1219() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1219");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark((long) 100, (long) 'a');
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.readLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 97L + "'", long2 == 97L);
    }

    @Test
    public void test1220() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1220");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long11 = logMark10.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        java.nio.ByteBuffer byteBuffer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark12.readLogMark(byteBuffer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + (-1L) + "'", long11 == (-1L));
    }

    @Test
    public void test1221() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1221");
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
        long long24 = logMark3.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 97L + "'", long5 == 97L);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str7, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 0L + "'", long13 == 0L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 0 + "'", int23 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 0L + "'", long24 == 0L);
    }

    @Test
    public void test1222() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1222");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        java.lang.String str5 = logMark1.toString();
        long long6 = logMark1.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str5, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 97L + "'", long6 == 97L);
    }

    @Test
    public void test1223() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1223");
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
        logMark10.setLogMark((long) 10, (long) (byte) -1);
        logMark10.setLogMark((long) ' ', (long) (byte) -1);
        long long25 = logMark10.getLogFileId();
        java.lang.String str26 = logMark10.toString();
        java.lang.Class<?> wildcardClass27 = logMark10.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 0L + "'", long17 == 0L);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 32L + "'", long25 == 32L);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - -1" + "'", str26, "LogMark: logFileId - 32 , logFileOffset - -1");
        org.junit.Assert.assertNotNull(wildcardClass27);
    }

    @Test
    public void test1224() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1224");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long5 = logMark3.getLogFileOffset();
        long long6 = logMark3.getLogFileId();
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 0L + "'", long5 == 0L);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 97L + "'", long6 == 97L);
    }

    @Test
    public void test1225() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1225");
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
        org.apache.bookkeeper.bookie.LogMark logMark66 = new org.apache.bookkeeper.bookie.LogMark(logMark65);
        long long67 = logMark66.getLogFileId();
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
        org.junit.Assert.assertTrue("'" + long67 + "' != '" + 10L + "'", long67 == 10L);
    }

    @Test
    public void test1226() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1226");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark((long) 100, (long) 'a');
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long7 = logMark6.getLogFileOffset();
        int int8 = logMark0.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        java.lang.String str13 = logMark12.toString();
        logMark12.setLogMark((long) (-1), (long) (byte) 100);
        long long17 = logMark12.getLogFileId();
        logMark12.setLogMark((long) '4', (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        int int22 = logMark6.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        long long24 = logMark23.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 97L + "'", long2 == 97L);
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str13, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + (-1L) + "'", long17 == (-1L));
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 52L + "'", long24 == 52L);
    }

    @Test
    public void test1227() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1227");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), (long) ' ');
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        java.lang.String str6 = logMark5.toString();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        java.lang.String str11 = logMark10.toString();
        logMark10.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark15 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        int int17 = logMark10.compare(logMark15);
        int int18 = logMark5.compare(logMark15);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        org.apache.bookkeeper.bookie.LogMark logMark20 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark20);
        java.lang.String str22 = logMark21.toString();
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark25);
        java.lang.String str27 = logMark26.toString();
        logMark26.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark31 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark(logMark31);
        int int33 = logMark26.compare(logMark31);
        int int34 = logMark21.compare(logMark31);
        int int35 = logMark19.compare(logMark31);
        org.apache.bookkeeper.bookie.LogMark logMark36 = new org.apache.bookkeeper.bookie.LogMark(logMark19);
        int int37 = logMark2.compare(logMark36);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + (-1L) + "'", long3 == (-1L));
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str6, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str11, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark15);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + (-1) + "'", int17 == (-1));
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 0 + "'", int18 == 0);
        org.junit.Assert.assertNotNull(logMark20);
        org.junit.Assert.assertEquals("'" + str22 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str22, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str27, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark31);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + (-1) + "'", int33 == (-1));
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 0 + "'", int35 == 0);
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + (-1) + "'", int37 == (-1));
    }

    @Test
    public void test1228() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1228");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, (long) 100);
        java.lang.String str3 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 100" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 100");
    }

    @Test
    public void test1229() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1229");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, (long) (byte) 100);
    }

    @Test
    public void test1230() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1230");
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
        long long30 = logMark13.getLogFileOffset();
        long long31 = logMark13.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
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
        long long56 = logMark46.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark59 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark60 = new org.apache.bookkeeper.bookie.LogMark(logMark59);
        java.lang.String str61 = logMark60.toString();
        logMark60.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark65 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark66 = new org.apache.bookkeeper.bookie.LogMark(logMark65);
        int int67 = logMark60.compare(logMark65);
        int int68 = logMark46.compare(logMark65);
        org.apache.bookkeeper.bookie.LogMark logMark71 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark74 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int75 = logMark71.compare(logMark74);
        org.apache.bookkeeper.bookie.LogMark logMark76 = new org.apache.bookkeeper.bookie.LogMark(logMark71);
        org.apache.bookkeeper.bookie.LogMark logMark77 = new org.apache.bookkeeper.bookie.LogMark(logMark76);
        int int78 = logMark46.compare(logMark77);
        long long79 = logMark46.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark82 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark83 = new org.apache.bookkeeper.bookie.LogMark(logMark82);
        java.lang.String str84 = logMark82.toString();
        int int85 = logMark46.compare(logMark82);
        org.apache.bookkeeper.bookie.LogMark logMark86 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark87 = new org.apache.bookkeeper.bookie.LogMark(logMark86);
        java.lang.String str88 = logMark87.toString();
        org.apache.bookkeeper.bookie.LogMark logMark89 = new org.apache.bookkeeper.bookie.LogMark(logMark87);
        int int90 = logMark46.compare(logMark87);
        int int91 = logMark32.compare(logMark87);
        logMark32.setLogMark(100L, 1L);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str23, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str24, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark25);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 100L + "'", long28 == 100L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 10L + "'", long30 == 10L);
        org.junit.Assert.assertTrue("'" + long31 + "' != '" + 10L + "'", long31 == 10L);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + 0 + "'", int39 == 0);
        org.junit.Assert.assertTrue("'" + long43 + "' != '" + 10L + "'", long43 == 10L);
        org.junit.Assert.assertTrue("'" + int50 + "' != '" + 0 + "'", int50 == 0);
        org.junit.Assert.assertTrue("'" + long54 + "' != '" + 10L + "'", long54 == 10L);
        org.junit.Assert.assertTrue("'" + int55 + "' != '" + 0 + "'", int55 == 0);
        org.junit.Assert.assertTrue("'" + long56 + "' != '" + 10L + "'", long56 == 10L);
        org.junit.Assert.assertEquals("'" + str61 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str61, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark65);
        org.junit.Assert.assertTrue("'" + int67 + "' != '" + (-1) + "'", int67 == (-1));
        org.junit.Assert.assertTrue("'" + int68 + "' != '" + (-1) + "'", int68 == (-1));
        org.junit.Assert.assertTrue("'" + int75 + "' != '" + 0 + "'", int75 == 0);
        org.junit.Assert.assertTrue("'" + int78 + "' != '" + (-1) + "'", int78 == (-1));
        org.junit.Assert.assertTrue("'" + long79 + "' != '" + 10L + "'", long79 == 10L);
        org.junit.Assert.assertEquals("'" + str84 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str84, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int85 + "' != '" + 1 + "'", int85 == 1);
        org.junit.Assert.assertNotNull(logMark86);
        org.junit.Assert.assertEquals("'" + str88 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str88, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int90 + "' != '" + (-1) + "'", int90 == (-1));
        org.junit.Assert.assertTrue("'" + int91 + "' != '" + (-1) + "'", int91 == (-1));
    }

    @Test
    public void test1231() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1231");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        java.lang.String str8 = logMark5.toString();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str10 = logMark9.toString();
        long long11 = logMark9.getLogFileId();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str8, "LogMark: logFileId - -1 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str10, "LogMark: logFileId - -1 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + (-1L) + "'", long11 == (-1L));
    }

    @Test
    public void test1232() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1232");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(1L, (long) (short) -1);
    }

    @Test
    public void test1233() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1233");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 0, 97L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileId();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
    }

    @Test
    public void test1234() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1234");
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
        org.apache.bookkeeper.bookie.LogMark logMark24 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long25 = logMark24.getLogFileOffset();
        java.lang.String str26 = logMark24.toString();
        org.apache.bookkeeper.bookie.LogMark logMark27 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark27);
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark(logMark28);
        long long30 = logMark29.getLogFileId();
        int int31 = logMark24.compare(logMark29);
        int int32 = logMark19.compare(logMark24);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str4, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 97L + "'", long6 == 97L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 1L + "'", long17 == 1L);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 1L + "'", long18 == 1L);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 1 + "'", int23 == 1);
        org.junit.Assert.assertNotNull(logMark24);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 97L + "'", long25 == 97L);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str26, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark27);
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 100L + "'", long30 == 100L);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + (-1) + "'", int32 == (-1));
    }

    @Test
    public void test1235() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1235");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark((long) 'a', 35L);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int13 = logMark9.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark15 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        int int16 = logMark9.compare(logMark15);
        long long17 = logMark15.getLogFileId();
        int int18 = logMark3.compare(logMark15);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertNotNull(logMark15);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 100L + "'", long17 == 100L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + (-1) + "'", int18 == (-1));
    }

    @Test
    public void test1236() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1236");
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
        org.apache.bookkeeper.bookie.LogMark logMark40 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark(logMark40);
        long long42 = logMark40.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark(logMark43);
        int int45 = logMark40.compare(logMark44);
        org.apache.bookkeeper.bookie.LogMark logMark48 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark51 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int52 = logMark48.compare(logMark51);
        org.apache.bookkeeper.bookie.LogMark logMark53 = new org.apache.bookkeeper.bookie.LogMark(logMark48);
        int int54 = logMark44.compare(logMark48);
        long long55 = logMark48.getLogFileId();
        java.lang.String str56 = logMark48.toString();
        int int57 = logMark2.compare(logMark48);
        org.apache.bookkeeper.bookie.LogMark logMark58 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
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
        org.junit.Assert.assertTrue("'" + long42 + "' != '" + 0L + "'", long42 == 0L);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + 1 + "'", int45 == 1);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 0 + "'", int52 == 0);
        org.junit.Assert.assertTrue("'" + int54 + "' != '" + (-1) + "'", int54 == (-1));
        org.junit.Assert.assertTrue("'" + long55 + "' != '" + 97L + "'", long55 == 97L);
        org.junit.Assert.assertEquals("'" + str56 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str56, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int57 + "' != '" + 0 + "'", int57 == 0);
    }

    @Test
    public void test1237() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1237");
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
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) '4', 9223372036854775807L);
        int int13 = logMark3.compare(logMark12);
        logMark3.setLogMark((-1L), 1L);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 97L + "'", long5 == 97L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
    }

    @Test
    public void test1238() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1238");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long3 = logMark2.getLogFileId();
        logMark2.setLogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long8 = logMark7.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int11 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        java.lang.String str16 = logMark14.toString();
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        int int18 = logMark2.compare(logMark17);
        long long19 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 97L + "'", long8 == 97L);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + (-1) + "'", int11 == (-1));
        org.junit.Assert.assertEquals("'" + str16 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str16, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + (-1) + "'", int18 == (-1));
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 1L + "'", long19 == 1L);
    }

    @Test
    public void test1239() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1239");
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
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        java.nio.ByteBuffer byteBuffer28 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark26.readLogMark(byteBuffer28);
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
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 100L + "'", long15 == 100L);
        org.junit.Assert.assertEquals("'" + str22 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str22, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 1 + "'", int25 == 1);
    }

    @Test
    public void test1240() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1240");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark5.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark();
        int int11 = logMark5.compare(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int18 = logMark14.compare(logMark17);
        logMark14.setLogMark((long) (short) 10, 10L);
        long long22 = logMark14.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int29 = logMark25.compare(logMark28);
        logMark25.setLogMark((long) (short) 10, 10L);
        long long33 = logMark25.getLogFileId();
        int int34 = logMark14.compare(logMark25);
        long long35 = logMark25.getLogFileOffset();
        long long36 = logMark25.getLogFileId();
        java.lang.String str37 = logMark25.toString();
        int int38 = logMark10.compare(logMark25);
        long long39 = logMark25.getLogFileOffset();
        long long40 = logMark25.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        logMark43.setLogMark((long) (-1), 9223372036854775807L);
        java.lang.String str47 = logMark43.toString();
        logMark43.setLogMark(35L, (long) (byte) 10);
        logMark43.setLogMark((long) (byte) 10, (long) (byte) 0);
        int int54 = logMark25.compare(logMark43);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + 1 + "'", int11 == 1);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 0 + "'", int18 == 0);
        org.junit.Assert.assertTrue("'" + long22 + "' != '" + 10L + "'", long22 == 10L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + 0 + "'", int29 == 0);
        org.junit.Assert.assertTrue("'" + long33 + "' != '" + 10L + "'", long33 == 10L);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 10L + "'", long36 == 10L);
        org.junit.Assert.assertEquals("'" + str37 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str37, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + (-1) + "'", int38 == (-1));
        org.junit.Assert.assertTrue("'" + long39 + "' != '" + 10L + "'", long39 == 10L);
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + 10L + "'", long40 == 10L);
        org.junit.Assert.assertEquals("'" + str47 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807" + "'", str47, "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807");
        org.junit.Assert.assertTrue("'" + int54 + "' != '" + 1 + "'", int54 == 1);
    }

    @Test
    public void test1241() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1241");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        java.lang.String str3 = logMark1.toString();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark1.readLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str3, "LogMark: logFileId - 100 , logFileOffset - 97");
    }

    @Test
    public void test1242() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1242");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        long long7 = logMark5.getLogFileId();
        long long8 = logMark5.getLogFileId();
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.writeLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 97L + "'", long8 == 97L);
    }

    @Test
    public void test1243() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1243");
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
        logMark12.setLogMark((long) (short) 100, (long) (short) 100);
        java.lang.Class<?> wildcardClass46 = logMark12.getClass();
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
        org.junit.Assert.assertNotNull(wildcardClass46);
    }

    @Test
    public void test1244() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1244");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        long long9 = logMark3.getLogFileId();
        logMark3.setLogMark(35L, (long) 100);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + (-1L) + "'", long9 == (-1L));
    }

    @Test
    public void test1245() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1245");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 100, 0L);
    }

    @Test
    public void test1246() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1246");
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
        logMark13.setLogMark(35L, (long) (short) 100);
        long long27 = logMark13.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long27 + "' != '" + 35L + "'", long27 == 35L);
    }

    @Test
    public void test1247() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1247");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        java.lang.String str2 = logMark0.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        long long6 = logMark5.getLogFileId();
        int int7 = logMark0.compare(logMark5);
        logMark5.setLogMark((long) '4', 52L);
        long long11 = logMark5.getLogFileId();
        java.lang.String str12 = logMark5.toString();
        java.lang.String str13 = logMark5.toString();
        java.lang.Class<?> wildcardClass14 = logMark5.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 52L + "'", long11 == 52L);
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 52" + "'", str12, "LogMark: logFileId - 52 , logFileOffset - 52");
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 52" + "'", str13, "LogMark: logFileId - 52 , logFileOffset - 52");
        org.junit.Assert.assertNotNull(wildcardClass14);
    }

    @Test
    public void test1248() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1248");
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
        java.nio.ByteBuffer byteBuffer32 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark15.readLogMark(byteBuffer32);
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
        org.junit.Assert.assertNotNull(logMark16);
        org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str18, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark27);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
    }

    @Test
    public void test1249() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1249");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), (long) ' ');
        logMark2.setLogMark((long) (byte) 0, 100L);
        long long6 = logMark2.getLogFileId();
        long long7 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 100L + "'", long7 == 100L);
    }

    @Test
    public void test1250() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1250");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
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
    public void test1251() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1251");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(9223372036854775807L, (long) 'a');
    }

    @Test
    public void test1252() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1252");
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
        java.lang.Class<?> wildcardClass38 = logMark37.getClass();
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
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str36, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(wildcardClass38);
    }

    @Test
    public void test1253() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1253");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, (long) ' ');
        java.lang.String str3 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 32" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 32");
    }

    @Test
    public void test1254() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1254");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        java.lang.String str6 = logMark5.toString();
        int int7 = logMark3.compare(logMark5);
        logMark5.setLogMark((long) 'a', 97L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 35L + "'", long4 == 35L);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str6, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
    }

    @Test
    public void test1255() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1255");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, (long) (-1));
        long long3 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + (-1L) + "'", long3 == (-1L));
    }

    @Test
    public void test1256() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1256");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 100, (long) '#');
        java.lang.String str3 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 35" + "'", str3, "LogMark: logFileId - 100 , logFileOffset - 35");
    }

    @Test
    public void test1257() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1257");
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
        logMark12.setLogMark((long) (-1), 97L);
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
    }

    @Test
    public void test1258() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1258");
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
        java.lang.String str32 = logMark20.toString();
        java.lang.String str33 = logMark20.toString();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 0 + "'", int18 == 0);
        org.junit.Assert.assertNotNull(logMark21);
        org.junit.Assert.assertTrue("'" + long22 + "' != '" + 97L + "'", long22 == 97L);
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str24, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 32L + "'", long28 == 32L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 1 + "'", int30 == 1);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + (-1) + "'", int31 == (-1));
        org.junit.Assert.assertEquals("'" + str32 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str32, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str33 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str33, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test1259() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1259");
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
        logMark49.setLogMark((long) (short) 1, (long) (short) -1);
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
        org.junit.Assert.assertEquals("'" + str53 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str53, "LogMark: logFileId - 0 , logFileOffset - 35");
    }

    @Test
    public void test1260() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1260");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 9223372036854775807L);
        long long3 = logMark2.getLogFileId();
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
    public void test1261() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1261");
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
        logMark53.setLogMark((long) 100, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark60 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long61 = logMark60.getLogFileId();
        logMark60.setLogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark65 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long66 = logMark65.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark67 = new org.apache.bookkeeper.bookie.LogMark(logMark65);
        org.apache.bookkeeper.bookie.LogMark logMark68 = new org.apache.bookkeeper.bookie.LogMark(logMark65);
        int int69 = logMark60.compare(logMark65);
        java.lang.String str70 = logMark60.toString();
        int int71 = logMark53.compare(logMark60);
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
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str36, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long51 + "' != '" + 10L + "'", long51 == 10L);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + (-1) + "'", int52 == (-1));
        org.junit.Assert.assertEquals("'" + str54 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str54, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long61 + "' != '" + 100L + "'", long61 == 100L);
        org.junit.Assert.assertNotNull(logMark65);
        org.junit.Assert.assertTrue("'" + long66 + "' != '" + 10L + "'", long66 == 10L);
        org.junit.Assert.assertTrue("'" + int69 + "' != '" + 1 + "'", int69 == 1);
        org.junit.Assert.assertEquals("'" + str70 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 0" + "'", str70, "LogMark: logFileId - 1 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int71 + "' != '" + 1 + "'", int71 == 1);
    }

    @Test
    public void test1262() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1262");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, 0L);
        logMark2.setLogMark((long) (byte) 0, (long) (byte) 100);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long7 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 0L + "'", long7 == 0L);
    }

    @Test
    public void test1263() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1263");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        java.lang.String str5 = logMark4.toString();
        java.lang.String str6 = logMark4.toString();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str5, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str6, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test1264() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1264");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark((long) 100, (long) 'a');
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long7 = logMark6.getLogFileOffset();
        int int8 = logMark0.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        java.lang.String str10 = logMark6.toString();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        java.nio.ByteBuffer byteBuffer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark11.readLogMark(byteBuffer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 10L + "'", long2 == 10L);
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str10, "LogMark: logFileId - 100 , logFileOffset - 97");
    }

    @Test
    public void test1265() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1265");
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
        long long30 = logMark13.getLogFileOffset();
        long long31 = logMark13.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
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
        long long56 = logMark46.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark59 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark60 = new org.apache.bookkeeper.bookie.LogMark(logMark59);
        java.lang.String str61 = logMark60.toString();
        logMark60.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark65 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark66 = new org.apache.bookkeeper.bookie.LogMark(logMark65);
        int int67 = logMark60.compare(logMark65);
        int int68 = logMark46.compare(logMark65);
        org.apache.bookkeeper.bookie.LogMark logMark71 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark74 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int75 = logMark71.compare(logMark74);
        org.apache.bookkeeper.bookie.LogMark logMark76 = new org.apache.bookkeeper.bookie.LogMark(logMark71);
        org.apache.bookkeeper.bookie.LogMark logMark77 = new org.apache.bookkeeper.bookie.LogMark(logMark76);
        int int78 = logMark46.compare(logMark77);
        long long79 = logMark46.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark82 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark83 = new org.apache.bookkeeper.bookie.LogMark(logMark82);
        java.lang.String str84 = logMark82.toString();
        int int85 = logMark46.compare(logMark82);
        org.apache.bookkeeper.bookie.LogMark logMark86 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark87 = new org.apache.bookkeeper.bookie.LogMark(logMark86);
        java.lang.String str88 = logMark87.toString();
        org.apache.bookkeeper.bookie.LogMark logMark89 = new org.apache.bookkeeper.bookie.LogMark(logMark87);
        int int90 = logMark46.compare(logMark87);
        int int91 = logMark32.compare(logMark87);
        java.lang.String str92 = logMark32.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str23, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str24, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark25);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 100L + "'", long28 == 100L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 10L + "'", long30 == 10L);
        org.junit.Assert.assertTrue("'" + long31 + "' != '" + 10L + "'", long31 == 10L);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + 0 + "'", int39 == 0);
        org.junit.Assert.assertTrue("'" + long43 + "' != '" + 10L + "'", long43 == 10L);
        org.junit.Assert.assertTrue("'" + int50 + "' != '" + 0 + "'", int50 == 0);
        org.junit.Assert.assertTrue("'" + long54 + "' != '" + 10L + "'", long54 == 10L);
        org.junit.Assert.assertTrue("'" + int55 + "' != '" + 0 + "'", int55 == 0);
        org.junit.Assert.assertTrue("'" + long56 + "' != '" + 10L + "'", long56 == 10L);
        org.junit.Assert.assertEquals("'" + str61 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str61, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark65);
        org.junit.Assert.assertTrue("'" + int67 + "' != '" + (-1) + "'", int67 == (-1));
        org.junit.Assert.assertTrue("'" + int68 + "' != '" + (-1) + "'", int68 == (-1));
        org.junit.Assert.assertTrue("'" + int75 + "' != '" + 0 + "'", int75 == 0);
        org.junit.Assert.assertTrue("'" + int78 + "' != '" + (-1) + "'", int78 == (-1));
        org.junit.Assert.assertTrue("'" + long79 + "' != '" + 10L + "'", long79 == 10L);
        org.junit.Assert.assertEquals("'" + str84 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str84, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int85 + "' != '" + 1 + "'", int85 == 1);
        org.junit.Assert.assertNotNull(logMark86);
        org.junit.Assert.assertEquals("'" + str88 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str88, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int90 + "' != '" + (-1) + "'", int90 == (-1));
        org.junit.Assert.assertTrue("'" + int91 + "' != '" + (-1) + "'", int91 == (-1));
        org.junit.Assert.assertEquals("'" + str92 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str92, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test1266() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1266");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 9223372036854775807L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark4.setLogMark((long) '#', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 9223372036854775807L + "'", long3 == 9223372036854775807L);
    }

    @Test
    public void test1267() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1267");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        logMark3.setLogMark((long) 1, 97L);
        long long8 = logMark3.getLogFileId();
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 1L + "'", long8 == 1L);
    }

    @Test
    public void test1268() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1268");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark2.setLogMark((long) (-1), (long) '#');
    }

    @Test
    public void test1269() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1269");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) (byte) 100);
    }

    @Test
    public void test1270() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1270");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        long long3 = logMark2.getLogFileOffset();
        long long4 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long8 = logMark7.getLogFileId();
        int int9 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark10.setLogMark(1L, 100L);
        logMark10.setLogMark((long) (short) 10, (long) (short) 0);
        long long17 = logMark10.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 1L + "'", long4 == 1L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 100L + "'", long8 == 100L);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 10L + "'", long17 == 10L);
    }

    @Test
    public void test1271() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1271");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        java.lang.String str3 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long5 = logMark4.getLogFileOffset();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str3, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 35L + "'", long5 == 35L);
    }

    @Test
    public void test1272() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1272");
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
        java.lang.Class<?> wildcardClass36 = logMark13.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertNotNull(logMark25);
        org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str27, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark29);
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 97L + "'", long30 == 97L);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 0 + "'", int33 == 0);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertNotNull(wildcardClass36);
    }

    @Test
    public void test1273() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1273");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), (long) ' ');
        logMark2.setLogMark((long) ' ', 0L);
    }

    @Test
    public void test1274() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1274");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 100, 1L);
    }

    @Test
    public void test1275() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1275");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        java.lang.String str2 = logMark0.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        long long6 = logMark5.getLogFileId();
        int int7 = logMark0.compare(logMark5);
        long long8 = logMark5.getLogFileOffset();
        logMark5.setLogMark((long) (short) -1, 9223372036854775807L);
        long long12 = logMark5.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 97L + "'", long8 == 97L);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 9223372036854775807L + "'", long12 == 9223372036854775807L);
    }

    @Test
    public void test1276() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1276");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) (byte) -1);
    }

    @Test
    public void test1277() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1277");
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
        java.lang.String str25 = logMark13.toString();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertEquals("'" + str25 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str25, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test1278() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1278");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark5.toString();
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test1279() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1279");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 52L);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test1280() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1280");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 100, (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        int int11 = logMark5.compare(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int13 = logMark2.compare(logMark12);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + (-1) + "'", int11 == (-1));
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
    }

    @Test
    public void test1281() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1281");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        int int9 = logMark2.compare(logMark8);
        long long10 = logMark2.getLogFileOffset();
        java.lang.String str11 = logMark2.toString();
        long long12 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 0L + "'", long10 == 0L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str11, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 97L + "'", long12 == 97L);
    }

    @Test
    public void test1282() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1282");
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
        logMark22.setLogMark(35L, 97L);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertNotNull(logMark2);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str4, "LogMark: logFileId - 100 , logFileOffset - 97");
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
    public void test1283() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1283");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) (byte) 10);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) 10);
        java.lang.String str8 = logMark7.toString();
        int int9 = logMark3.compare(logMark7);
        long long10 = logMark3.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 97L + "'", long4 == 97L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 10" + "'", str8, "LogMark: logFileId - 0 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
    }

    @Test
    public void test1284() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1284");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark2.setLogMark((long) (byte) 100, (long) (short) 0);
        logMark2.setLogMark(32L, 9223372036854775807L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, 35L);
        int int12 = logMark2.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        long long17 = logMark16.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark();
        java.lang.String str19 = logMark18.toString();
        int int20 = logMark16.compare(logMark18);
        logMark16.setLogMark((long) 100, 0L);
        int int24 = logMark2.compare(logMark16);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 1 + "'", int12 == 1);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 35L + "'", long17 == 35L);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str19, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 1 + "'", int20 == 1);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + (-1) + "'", int24 == (-1));
    }

    @Test
    public void test1285() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1285");
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
        logMark19.setLogMark((long) (short) 0, (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark(logMark19);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertNotNull(logMark18);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + (-1) + "'", int20 == (-1));
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + (-1) + "'", int21 == (-1));
    }

    @Test
    public void test1286() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1286");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        long long8 = logMark3.getLogFileId();
        long long9 = logMark3.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 10L + "'", long9 == 10L);
    }

    @Test
    public void test1287() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1287");
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
        long long19 = logMark8.getLogFileOffset();
        long long20 = logMark8.getLogFileOffset();
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + (-1L) + "'", long13 == (-1L));
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 52L + "'", long19 == 52L);
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 52L + "'", long20 == 52L);
    }

    @Test
    public void test1288() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1288");
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
        org.apache.bookkeeper.bookie.LogMark logMark19 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark19);
        logMark20.setLogMark((long) (short) -1, 0L);
        int int24 = logMark1.compare(logMark20);
        long long25 = logMark1.getLogFileOffset();
        logMark1.setLogMark((long) (-1), 97L);
        java.lang.Class<?> wildcardClass29 = logMark1.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 100L + "'", long15 == 100L);
        org.junit.Assert.assertNotNull(logMark19);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 1 + "'", int24 == 1);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 35L + "'", long25 == 35L);
        org.junit.Assert.assertNotNull(wildcardClass29);
    }

    @Test
    public void test1289() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1289");
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
        org.apache.bookkeeper.bookie.LogMark logMark40 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark(logMark40);
        long long42 = logMark40.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark(logMark43);
        int int45 = logMark40.compare(logMark44);
        org.apache.bookkeeper.bookie.LogMark logMark48 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark51 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int52 = logMark48.compare(logMark51);
        org.apache.bookkeeper.bookie.LogMark logMark53 = new org.apache.bookkeeper.bookie.LogMark(logMark48);
        int int54 = logMark44.compare(logMark48);
        long long55 = logMark48.getLogFileId();
        java.lang.String str56 = logMark48.toString();
        int int57 = logMark2.compare(logMark48);
        long long58 = logMark48.getLogFileOffset();
        long long59 = logMark48.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
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
        org.junit.Assert.assertTrue("'" + long42 + "' != '" + 0L + "'", long42 == 0L);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + 1 + "'", int45 == 1);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 0 + "'", int52 == 0);
        org.junit.Assert.assertTrue("'" + int54 + "' != '" + (-1) + "'", int54 == (-1));
        org.junit.Assert.assertTrue("'" + long55 + "' != '" + 97L + "'", long55 == 97L);
        org.junit.Assert.assertEquals("'" + str56 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str56, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int57 + "' != '" + 0 + "'", int57 == 0);
        org.junit.Assert.assertTrue("'" + long58 + "' != '" + 0L + "'", long58 == 0L);
        org.junit.Assert.assertTrue("'" + long59 + "' != '" + 97L + "'", long59 == 97L);
    }

    @Test
    public void test1290() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1290");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark2.setLogMark((long) (byte) 100, (long) (short) 0);
        logMark2.setLogMark(32L, 9223372036854775807L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, 35L);
        int int12 = logMark2.compare(logMark11);
        java.lang.String str13 = logMark2.toString();
        logMark2.setLogMark((long) 0, 52L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 1 + "'", int12 == 1);
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 9223372036854775807" + "'", str13, "LogMark: logFileId - 32 , logFileOffset - 9223372036854775807");
    }

    @Test
    public void test1291() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1291");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, 35L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        java.lang.String str8 = logMark7.toString();
        int int9 = logMark2.compare(logMark7);
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark7.readLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str8, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
    }

    @Test
    public void test1292() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1292");
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
        long long26 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + long26 + "' != '" + 10L + "'", long26 == 10L);
    }

    @Test
    public void test1293() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1293");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        logMark8.setLogMark((long) 10, (long) (-1));
        long long12 = logMark8.getLogFileId();
        java.lang.String str13 = logMark8.toString();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 10L + "'", long12 == 10L);
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - -1" + "'", str13, "LogMark: logFileId - 10 , logFileOffset - -1");
    }

    @Test
    public void test1294() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1294");
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
        long long20 = logMark3.getLogFileOffset();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + (-1) + "'", int18 == (-1));
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + (-1L) + "'", long19 == (-1L));
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 10L + "'", long20 == 10L);
    }

    @Test
    public void test1295() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1295");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), (long) '4');
        java.lang.String str3 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        long long8 = logMark7.getLogFileOffset();
        long long9 = logMark7.getLogFileId();
        int int10 = logMark2.compare(logMark7);
        java.lang.String str11 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 52" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 52");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 35L + "'", long8 == 35L);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 0L + "'", long9 == 0L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 52" + "'", str11, "LogMark: logFileId - -1 , logFileOffset - 52");
    }

    @Test
    public void test1296() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1296");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark6);
        java.lang.String str8 = logMark6.toString();
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark6.writeLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str8, "LogMark: logFileId - 0 , logFileOffset - 0");
    }

    @Test
    public void test1297() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1297");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 100, 0L);
        long long3 = logMark2.getLogFileOffset();
        long long4 = logMark2.getLogFileOffset();
        long long5 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 0L + "'", long5 == 0L);
    }

    @Test
    public void test1298() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1298");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, 52L);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test1299() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1299");
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
        long long37 = logMark2.getLogFileId();
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
        org.junit.Assert.assertTrue("'" + long37 + "' != '" + 10L + "'", long37 == 10L);
    }

    @Test
    public void test1300() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1300");
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
        long long23 = logMark10.getLogFileId();
        java.lang.String str24 = logMark10.toString();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str3, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + 1 + "'", int11 == 1);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 0 + "'", int18 == 0);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + (-1) + "'", int20 == (-1));
        org.junit.Assert.assertEquals("'" + str21 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str21, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 0L + "'", long23 == 0L);
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str24, "LogMark: logFileId - 0 , logFileOffset - 0");
    }

    @Test
    public void test1301() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1301");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileId();
        java.lang.Class<?> wildcardClass3 = logMark0.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 100L + "'", long2 == 100L);
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test1302() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1302");
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
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark(logMark31);
        long long42 = logMark41.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 100L + "'", long15 == 100L);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 35L + "'", long19 == 35L);
        org.junit.Assert.assertTrue("'" + int28 + "' != '" + (-1) + "'", int28 == (-1));
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 0 + "'", int32 == 0);
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 97L + "'", long36 == 97L);
        org.junit.Assert.assertTrue("'" + int40 + "' != '" + 1 + "'", int40 == 1);
        org.junit.Assert.assertTrue("'" + long42 + "' != '" + 100L + "'", long42 == 100L);
    }

    @Test
    public void test1303() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1303");
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
        org.apache.bookkeeper.bookie.LogMark logMark18 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long19 = logMark18.getLogFileOffset();
        java.lang.String str20 = logMark18.toString();
        org.apache.bookkeeper.bookie.LogMark logMark21 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark(logMark22);
        long long24 = logMark23.getLogFileId();
        int int25 = logMark18.compare(logMark23);
        logMark23.setLogMark((long) (byte) 100, (long) (byte) 100);
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark(logMark23);
        int int30 = logMark0.compare(logMark29);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + (-1) + "'", int11 == (-1));
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str12, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
        org.junit.Assert.assertEquals("'" + str17 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str17, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark18);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 97L + "'", long19 == 97L);
        org.junit.Assert.assertEquals("'" + str20 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str20, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark21);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 100L + "'", long24 == 100L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + (-1) + "'", int30 == (-1));
    }

    @Test
    public void test1304() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1304");
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
        java.lang.String str19 = logMark3.toString();
        java.nio.ByteBuffer byteBuffer20 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 0L + "'", long14 == 0L);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 0L + "'", long15 == 0L);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - -1" + "'", str19, "LogMark: logFileId - 1 , logFileOffset - -1");
    }

    @Test
    public void test1305() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1305");
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
        long long27 = logMark2.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer28 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer28);
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
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long27 + "' != '" + 10L + "'", long27 == 10L);
    }

    @Test
    public void test1306() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1306");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        java.lang.String str9 = logMark8.toString();
        logMark8.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark13 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        int int15 = logMark8.compare(logMark13);
        int int16 = logMark2.compare(logMark8);
        long long17 = logMark8.getLogFileId();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark13);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + (-1) + "'", int15 == (-1));
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + (-1L) + "'", long17 == (-1L));
    }

    @Test
    public void test1307() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1307");
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
        org.apache.bookkeeper.bookie.LogMark logMark40 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long41 = logMark40.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark42 = new org.apache.bookkeeper.bookie.LogMark(logMark40);
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark(logMark40);
        java.lang.String str44 = logMark43.toString();
        org.apache.bookkeeper.bookie.LogMark logMark45 = new org.apache.bookkeeper.bookie.LogMark(logMark43);
        logMark45.setLogMark((long) (-1), (long) '4');
        int int49 = logMark16.compare(logMark45);
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
        org.junit.Assert.assertNotNull(logMark40);
        org.junit.Assert.assertTrue("'" + long41 + "' != '" + 97L + "'", long41 == 97L);
        org.junit.Assert.assertEquals("'" + str44 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str44, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int49 + "' != '" + (-1) + "'", int49 == (-1));
    }

    @Test
    public void test1308() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1308");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(10L, (long) ' ');
    }

    @Test
    public void test1309() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1309");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, (long) (short) 0);
        java.lang.String str3 = logMark2.toString();
        long long4 = logMark2.getLogFileId();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 0" + "'", str3, "LogMark: logFileId - 1 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 1L + "'", long4 == 1L);
    }

    @Test
    public void test1310() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1310");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        logMark2.setLogMark((long) (-1), 9223372036854775807L);
        long long6 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 9223372036854775807L + "'", long6 == 9223372036854775807L);
    }

    @Test
    public void test1311() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1311");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        java.lang.String str6 = logMark5.toString();
        int int7 = logMark3.compare(logMark5);
        logMark3.setLogMark((long) 100, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 35L + "'", long4 == 35L);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str6, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
    }

    @Test
    public void test1312() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1312");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        java.lang.String str4 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        long long9 = logMark7.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        int int12 = logMark7.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int19 = logMark15.compare(logMark18);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        int int21 = logMark11.compare(logMark15);
        long long22 = logMark15.getLogFileOffset();
        long long23 = logMark15.getLogFileId();
        logMark15.setLogMark((long) (short) -1, (long) (byte) 0);
        int int27 = logMark1.compare(logMark15);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str4, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 0L + "'", long9 == 0L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 1 + "'", int12 == 1);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + (-1) + "'", int21 == (-1));
        org.junit.Assert.assertTrue("'" + long22 + "' != '" + 0L + "'", long22 == 0L);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 97L + "'", long23 == 97L);
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
    }

    @Test
    public void test1313() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1313");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark(100L, (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int14 = logMark10.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        logMark15.setLogMark((long) (short) 100, 35L);
        logMark15.setLogMark(32L, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
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
        java.lang.String str48 = logMark38.toString();
        int int49 = logMark24.compare(logMark38);
        long long50 = logMark38.getLogFileId();
        int int51 = logMark15.compare(logMark38);
        int int52 = logMark7.compare(logMark38);
        java.nio.ByteBuffer byteBuffer53 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark38.writeLogMark(byteBuffer53);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertEquals("'" + str48 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str48, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int49 + "' != '" + (-1) + "'", int49 == (-1));
        org.junit.Assert.assertTrue("'" + long50 + "' != '" + 10L + "'", long50 == 10L);
        org.junit.Assert.assertTrue("'" + int51 + "' != '" + 1 + "'", int51 == 1);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 1 + "'", int52 == 1);
    }

    @Test
    public void test1314() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1314");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        java.lang.String str5 = logMark2.toString();
        java.lang.String str6 = logMark2.toString();
        logMark2.setLogMark(35L, (long) (short) 100);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (short) -1);
        int int13 = logMark2.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark14 = null;
        // The following exception was thrown during execution in test generation
        try {
            int int15 = logMark12.compare(logMark14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str5, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str6, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
    }

    @Test
    public void test1315() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1315");
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
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str9, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str14, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + (-1) + "'", int21 == (-1));
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
    }

    @Test
    public void test1316() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1316");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (short) 1);
    }

    @Test
    public void test1317() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1317");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark1.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        long long13 = logMark11.getLogFileOffset();
        java.lang.String str14 = logMark11.toString();
        java.lang.String str15 = logMark11.toString();
        logMark11.setLogMark(35L, (long) (short) 100);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark19);
        int int21 = logMark6.compare(logMark19);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 0L + "'", long13 == 0L);
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str14, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str15, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + 1 + "'", int21 == 1);
    }

    @Test
    public void test1318() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1318");
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
        java.lang.String str31 = logMark6.toString();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertNotNull(logMark5);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str7, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str12, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark16);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + (-1) + "'", int18 == (-1));
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 100L + "'", long20 == 100L);
        org.junit.Assert.assertNotNull(logMark24);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + 1 + "'", int29 == 1);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + (-1) + "'", int30 == (-1));
        org.junit.Assert.assertEquals("'" + str31 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 35" + "'", str31, "LogMark: logFileId - 100 , logFileOffset - 35");
    }

    @Test
    public void test1319() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1319");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        long long4 = logMark3.getLogFileId();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 100L + "'", long4 == 100L);
    }

    @Test
    public void test1320() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1320");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 0, (long) 1);
    }

    @Test
    public void test1321() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1321");
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
        org.apache.bookkeeper.bookie.LogMark logMark19 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark19);
        logMark20.setLogMark((long) (short) -1, 0L);
        int int24 = logMark1.compare(logMark20);
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark(logMark20);
        org.apache.bookkeeper.bookie.LogMark logMark26 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        long long28 = logMark26.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark(logMark32);
        java.lang.String str34 = logMark33.toString();
        logMark33.setLogMark((long) (-1), (long) (byte) 100);
        long long38 = logMark33.getLogFileId();
        logMark33.setLogMark((long) '4', (long) '4');
        logMark33.setLogMark(35L, (long) 10);
        int int45 = logMark26.compare(logMark33);
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        int int47 = logMark20.compare(logMark46);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 100L + "'", long15 == 100L);
        org.junit.Assert.assertNotNull(logMark19);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 1 + "'", int24 == 1);
        org.junit.Assert.assertNotNull(logMark26);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 97L + "'", long28 == 97L);
        org.junit.Assert.assertEquals("'" + str34 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str34, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long38 + "' != '" + (-1L) + "'", long38 == (-1L));
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + 1 + "'", int45 == 1);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + (-1) + "'", int47 == (-1));
    }

    @Test
    public void test1322() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1322");
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
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark45 = new org.apache.bookkeeper.bookie.LogMark(logMark44);
        org.apache.bookkeeper.bookie.LogMark logMark48 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark49 = new org.apache.bookkeeper.bookie.LogMark(logMark48);
        int int50 = logMark44.compare(logMark49);
        java.lang.String str51 = logMark44.toString();
        org.apache.bookkeeper.bookie.LogMark logMark54 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark55 = new org.apache.bookkeeper.bookie.LogMark(logMark54);
        java.lang.String str56 = logMark54.toString();
        org.apache.bookkeeper.bookie.LogMark logMark59 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark59.setLogMark((long) (byte) 100, (long) (short) 0);
        int int63 = logMark54.compare(logMark59);
        int int64 = logMark44.compare(logMark54);
        org.apache.bookkeeper.bookie.LogMark logMark67 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark68 = new org.apache.bookkeeper.bookie.LogMark(logMark67);
        java.lang.String str69 = logMark68.toString();
        org.apache.bookkeeper.bookie.LogMark logMark70 = new org.apache.bookkeeper.bookie.LogMark(logMark68);
        long long71 = logMark70.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark74 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark77 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int78 = logMark74.compare(logMark77);
        org.apache.bookkeeper.bookie.LogMark logMark79 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark80 = new org.apache.bookkeeper.bookie.LogMark(logMark79);
        int int81 = logMark74.compare(logMark80);
        int int82 = logMark70.compare(logMark74);
        org.apache.bookkeeper.bookie.LogMark logMark83 = new org.apache.bookkeeper.bookie.LogMark(logMark70);
        org.apache.bookkeeper.bookie.LogMark logMark84 = new org.apache.bookkeeper.bookie.LogMark(logMark83);
        int int85 = logMark54.compare(logMark84);
        int int86 = logMark12.compare(logMark54);
        org.apache.bookkeeper.bookie.LogMark logMark87 = new org.apache.bookkeeper.bookie.LogMark(logMark54);
        java.nio.ByteBuffer byteBuffer88 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark87.writeLogMark(byteBuffer88);
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
        org.junit.Assert.assertEquals("'" + str25 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str25, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark29);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + (-1) + "'", int31 == (-1));
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + 0 + "'", int38 == 0);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + (-1) + "'", int39 == (-1));
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + (-1L) + "'", long40 == (-1L));
        org.junit.Assert.assertTrue("'" + int41 + "' != '" + 1 + "'", int41 == 1);
        org.junit.Assert.assertTrue("'" + int50 + "' != '" + (-1) + "'", int50 == (-1));
        org.junit.Assert.assertEquals("'" + str51 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str51, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str56 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str56, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int63 + "' != '" + (-1) + "'", int63 == (-1));
        org.junit.Assert.assertTrue("'" + int64 + "' != '" + 0 + "'", int64 == 0);
        org.junit.Assert.assertEquals("'" + str69 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str69, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long71 + "' != '" + 0L + "'", long71 == 0L);
        org.junit.Assert.assertTrue("'" + int78 + "' != '" + 0 + "'", int78 == 0);
        org.junit.Assert.assertNotNull(logMark79);
        org.junit.Assert.assertTrue("'" + int81 + "' != '" + (-1) + "'", int81 == (-1));
        org.junit.Assert.assertTrue("'" + int82 + "' != '" + 0 + "'", int82 == 0);
        org.junit.Assert.assertTrue("'" + int85 + "' != '" + (-1) + "'", int85 == (-1));
        org.junit.Assert.assertTrue("'" + int86 + "' != '" + 1 + "'", int86 == 1);
    }

    @Test
    public void test1323() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1323");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long8 = logMark2.getLogFileOffset();
        logMark2.setLogMark((long) (byte) 1, (long) (short) 10);
        long long12 = logMark2.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 10L + "'", long12 == 10L);
    }

    @Test
    public void test1324() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1324");
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
        logMark8.setLogMark(1L, (long) 'a');
        java.lang.String str22 = logMark8.toString();
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + (-1L) + "'", long13 == (-1L));
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertEquals("'" + str22 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 97" + "'", str22, "LogMark: logFileId - 1 , logFileOffset - 97");
    }

    @Test
    public void test1325() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1325");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        logMark2.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long12 = logMark11.getLogFileOffset();
        int int13 = logMark8.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        long long15 = logMark11.getLogFileOffset();
        int int16 = logMark2.compare(logMark11);
        long long17 = logMark2.getLogFileId();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 0L + "'", long12 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 0L + "'", long15 == 0L);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 1L + "'", long17 == 1L);
    }

    @Test
    public void test1326() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1326");
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
        java.lang.Class<?> wildcardClass30 = logMark13.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str23, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str24, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark25);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 100L + "'", long28 == 100L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass30);
    }

    @Test
    public void test1327() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1327");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) '#');
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
    public void test1328() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1328");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long8 = logMark7.getLogFileId();
        long long9 = logMark7.getLogFileOffset();
        java.lang.String str10 = logMark7.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 97L + "'", long8 == 97L);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 0L + "'", long9 == 0L);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str10, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test1329() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1329");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark(100L, (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int14 = logMark10.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        logMark15.setLogMark((long) (short) 100, 35L);
        logMark15.setLogMark(32L, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
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
        java.lang.String str48 = logMark38.toString();
        int int49 = logMark24.compare(logMark38);
        long long50 = logMark38.getLogFileId();
        int int51 = logMark15.compare(logMark38);
        int int52 = logMark7.compare(logMark38);
        org.apache.bookkeeper.bookie.LogMark logMark55 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark58 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark61 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int62 = logMark58.compare(logMark61);
        logMark58.setLogMark((long) (short) 10, 10L);
        long long66 = logMark58.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark69 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark72 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int73 = logMark69.compare(logMark72);
        logMark69.setLogMark((long) (short) 10, 10L);
        long long77 = logMark69.getLogFileId();
        int int78 = logMark58.compare(logMark69);
        java.lang.String str79 = logMark69.toString();
        int int80 = logMark55.compare(logMark69);
        long long81 = logMark69.getLogFileId();
        int int82 = logMark38.compare(logMark69);
        long long83 = logMark38.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertEquals("'" + str48 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str48, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int49 + "' != '" + (-1) + "'", int49 == (-1));
        org.junit.Assert.assertTrue("'" + long50 + "' != '" + 10L + "'", long50 == 10L);
        org.junit.Assert.assertTrue("'" + int51 + "' != '" + 1 + "'", int51 == 1);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 1 + "'", int52 == 1);
        org.junit.Assert.assertTrue("'" + int62 + "' != '" + 0 + "'", int62 == 0);
        org.junit.Assert.assertTrue("'" + long66 + "' != '" + 10L + "'", long66 == 10L);
        org.junit.Assert.assertTrue("'" + int73 + "' != '" + 0 + "'", int73 == 0);
        org.junit.Assert.assertTrue("'" + long77 + "' != '" + 10L + "'", long77 == 10L);
        org.junit.Assert.assertTrue("'" + int78 + "' != '" + 0 + "'", int78 == 0);
        org.junit.Assert.assertEquals("'" + str79 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str79, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int80 + "' != '" + (-1) + "'", int80 == (-1));
        org.junit.Assert.assertTrue("'" + long81 + "' != '" + 10L + "'", long81 == 10L);
        org.junit.Assert.assertTrue("'" + int82 + "' != '" + 0 + "'", int82 == 0);
        org.junit.Assert.assertTrue("'" + long83 + "' != '" + 10L + "'", long83 == 10L);
    }

    @Test
    public void test1330() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1330");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        long long13 = logMark12.getLogFileOffset();
        long long14 = logMark12.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long18 = logMark17.getLogFileId();
        int int19 = logMark12.compare(logMark17);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        logMark20.setLogMark(1L, 100L);
        int int24 = logMark9.compare(logMark20);
        java.lang.String str25 = logMark20.toString();
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(9223372036854775807L, (-1L));
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark(logMark28);
        int int30 = logMark20.compare(logMark28);
        logMark28.setLogMark((long) '#', (long) (byte) -1);
        java.nio.ByteBuffer byteBuffer34 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark28.readLogMark(byteBuffer34);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 35L + "'", long13 == 35L);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 1L + "'", long14 == 1L);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 100L + "'", long18 == 100L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + (-1) + "'", int24 == (-1));
        org.junit.Assert.assertEquals("'" + str25 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 100" + "'", str25, "LogMark: logFileId - 1 , logFileOffset - 100");
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + (-1) + "'", int30 == (-1));
    }

    @Test
    public void test1331() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1331");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 10, 10L);
    }

    @Test
    public void test1332() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1332");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 10, (long) (short) 0);
    }

    @Test
    public void test1333() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1333");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark((long) 100, (long) 'a');
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.writeLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 97L + "'", long2 == 97L);
    }

    @Test
    public void test1334() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1334");
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
        java.lang.Class<?> wildcardClass19 = logMark12.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 1L + "'", long14 == 1L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test1335() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1335");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        long long11 = logMark9.getLogFileId();
        int int12 = logMark3.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long14 = logMark3.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
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
    }

    @Test
    public void test1336() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1336");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark7.setLogMark((long) 10, (long) '4');
        logMark7.setLogMark(10L, (long) (short) 10);
        logMark7.setLogMark((long) '4', (-1L));
        java.nio.ByteBuffer byteBuffer17 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark7.writeLogMark(byteBuffer17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
    }

    @Test
    public void test1337() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1337");
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
        org.apache.bookkeeper.bookie.LogMark logMark24 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark(logMark24);
        long long26 = logMark24.getLogFileOffset();
        logMark24.setLogMark((long) (byte) 10, (long) '#');
        int int30 = logMark21.compare(logMark24);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 100L + "'", long15 == 100L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 10" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark24);
        org.junit.Assert.assertTrue("'" + long26 + "' != '" + 97L + "'", long26 == 97L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 1 + "'", int30 == 1);
    }

    @Test
    public void test1338() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1338");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark((long) 100, (long) 'a');
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long7 = logMark6.getLogFileOffset();
        int int8 = logMark0.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        logMark9.setLogMark((long) 0, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long19 = logMark18.getLogFileOffset();
        int int20 = logMark15.compare(logMark18);
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark18);
        int int22 = logMark9.compare(logMark21);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 35L + "'", long2 == 35L);
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 0L + "'", long19 == 0L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 1 + "'", int20 == 1);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
    }

    @Test
    public void test1339() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1339");
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
        long long83 = logMark65.getLogFileOffset();
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
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str36, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long51 + "' != '" + 10L + "'", long51 == 10L);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + (-1) + "'", int52 == (-1));
        org.junit.Assert.assertEquals("'" + str58 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str58, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int69 + "' != '" + (-1) + "'", int69 == (-1));
        org.junit.Assert.assertNotNull(logMark70);
        org.junit.Assert.assertTrue("'" + long71 + "' != '" + 10L + "'", long71 == 10L);
        org.junit.Assert.assertEquals("'" + str74 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str74, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int82 + "' != '" + (-1) + "'", int82 == (-1));
        org.junit.Assert.assertTrue("'" + long83 + "' != '" + 97L + "'", long83 == 97L);
    }

    @Test
    public void test1340() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1340");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        java.lang.String str9 = logMark2.toString();
        logMark2.setLogMark((long) 10, 35L);
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
        long long36 = logMark26.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark39 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark40 = new org.apache.bookkeeper.bookie.LogMark(logMark39);
        java.lang.String str41 = logMark40.toString();
        logMark40.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark45 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark46 = new org.apache.bookkeeper.bookie.LogMark(logMark45);
        int int47 = logMark40.compare(logMark45);
        int int48 = logMark26.compare(logMark45);
        org.apache.bookkeeper.bookie.LogMark logMark51 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark54 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int55 = logMark51.compare(logMark54);
        org.apache.bookkeeper.bookie.LogMark logMark56 = new org.apache.bookkeeper.bookie.LogMark(logMark51);
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark(logMark56);
        int int58 = logMark26.compare(logMark57);
        long long59 = logMark26.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark62 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark63 = new org.apache.bookkeeper.bookie.LogMark(logMark62);
        java.lang.String str64 = logMark62.toString();
        int int65 = logMark26.compare(logMark62);
        java.lang.String str66 = logMark62.toString();
        long long67 = logMark62.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark70 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark71 = new org.apache.bookkeeper.bookie.LogMark(logMark70);
        java.lang.String str72 = logMark71.toString();
        logMark71.setLogMark((long) (-1), (long) (byte) 100);
        org.apache.bookkeeper.bookie.LogMark logMark76 = new org.apache.bookkeeper.bookie.LogMark(logMark71);
        int int77 = logMark62.compare(logMark76);
        int int78 = logMark2.compare(logMark76);
        org.apache.bookkeeper.bookie.LogMark logMark79 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str9, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 10L + "'", long34 == 10L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 0 + "'", int35 == 0);
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 10L + "'", long36 == 10L);
        org.junit.Assert.assertEquals("'" + str41 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str41, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark45);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + int48 + "' != '" + 1 + "'", int48 == 1);
        org.junit.Assert.assertTrue("'" + int55 + "' != '" + 0 + "'", int55 == 0);
        org.junit.Assert.assertTrue("'" + int58 + "' != '" + (-1) + "'", int58 == (-1));
        org.junit.Assert.assertTrue("'" + long59 + "' != '" + 10L + "'", long59 == 10L);
        org.junit.Assert.assertEquals("'" + str64 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str64, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int65 + "' != '" + 1 + "'", int65 == 1);
        org.junit.Assert.assertEquals("'" + str66 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str66, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long67 + "' != '" + 0L + "'", long67 == 0L);
        org.junit.Assert.assertEquals("'" + str72 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str72, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int77 + "' != '" + 1 + "'", int77 == 1);
        org.junit.Assert.assertTrue("'" + int78 + "' != '" + 1 + "'", int78 == 1);
    }

    @Test
    public void test1341() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1341");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark((long) 100, (long) 'a');
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long7 = logMark6.getLogFileOffset();
        int int8 = logMark0.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        java.lang.String str13 = logMark12.toString();
        logMark12.setLogMark((long) (-1), (long) (byte) 100);
        long long17 = logMark12.getLogFileId();
        logMark12.setLogMark((long) '4', (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        int int22 = logMark6.compare(logMark12);
        long long23 = logMark6.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 10L + "'", long2 == 10L);
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str13, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + (-1L) + "'", long17 == (-1L));
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 97L + "'", long23 == 97L);
    }

    @Test
    public void test1342() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1342");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 10, (long) 100);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
    }

    @Test
    public void test1343() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1343");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark7.compare(logMark11);
        logMark11.setLogMark(97L, 97L);
        long long16 = logMark11.getLogFileOffset();
        logMark11.setLogMark((long) (short) 0, (long) (byte) 100);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        long long21 = logMark20.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 97L + "'", long16 == 97L);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 100L + "'", long21 == 100L);
    }

    @Test
    public void test1344() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1344");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long8 = logMark2.getLogFileOffset();
        logMark2.setLogMark((long) (byte) 1, (long) (short) 10);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 10, (long) (byte) 100);
        int int15 = logMark2.compare(logMark14);
        org.apache.bookkeeper.bookie.LogMark logMark16 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark16);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark17);
        logMark18.setLogMark(1L, (long) (byte) 1);
        logMark18.setLogMark((long) (byte) 0, 0L);
        logMark18.setLogMark((long) 10, (long) (byte) 10);
        int int28 = logMark14.compare(logMark18);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + (-1) + "'", int15 == (-1));
        org.junit.Assert.assertNotNull(logMark16);
        org.junit.Assert.assertTrue("'" + int28 + "' != '" + 1 + "'", int28 == 1);
    }

    @Test
    public void test1345() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1345");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.String str12 = logMark11.toString();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 100" + "'", str12, "LogMark: logFileId - -1 , logFileOffset - 100");
    }

    @Test
    public void test1346() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1346");
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
        java.lang.String str30 = logMark11.toString();
        java.nio.ByteBuffer byteBuffer31 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark11.readLogMark(byteBuffer31);
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
        org.junit.Assert.assertEquals("'" + str20 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str20, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 100L + "'", long28 == 100L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + 1 + "'", int29 == 1);
        org.junit.Assert.assertEquals("'" + str30 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str30, "LogMark: logFileId - 100 , logFileOffset - 97");
    }

    @Test
    public void test1347() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1347");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark7.setLogMark((long) (byte) 100, (long) (short) 0);
        int int11 = logMark2.compare(logMark7);
        long long12 = logMark7.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + (-1) + "'", int11 == (-1));
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 100L + "'", long12 == 100L);
    }

    @Test
    public void test1348() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1348");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long6 = logMark2.getLogFileId();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - -1" + "'", str4, "LogMark: logFileId - 52 , logFileOffset - -1");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 52L + "'", long6 == 52L);
    }

    @Test
    public void test1349() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1349");
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
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass21 = logMark20.getClass();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 100L + "'", long7 == 100L);
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 97L + "'", long12 == 97L);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + (-1) + "'", int15 == (-1));
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test1350() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1350");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(32L, (long) (byte) 100);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
    }

    @Test
    public void test1351() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1351");
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
        org.apache.bookkeeper.bookie.LogMark logMark19 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark19);
        logMark20.setLogMark((long) (short) -1, 0L);
        int int24 = logMark1.compare(logMark20);
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 100L + "'", long15 == 100L);
        org.junit.Assert.assertNotNull(logMark19);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 1 + "'", int24 == 1);
    }

    @Test
    public void test1352() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1352");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(32L, (long) ' ');
    }

    @Test
    public void test1353() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1353");
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
        java.lang.String str46 = logMark13.toString();
        long long47 = logMark13.getLogFileOffset();
        long long48 = logMark13.getLogFileId();
        logMark13.setLogMark((long) (short) 1, 35L);
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
        org.junit.Assert.assertEquals("'" + str46 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str46, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long47 + "' != '" + 10L + "'", long47 == 10L);
        org.junit.Assert.assertTrue("'" + long48 + "' != '" + 10L + "'", long48 == 10L);
    }

    @Test
    public void test1354() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1354");
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
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int26 = logMark22.compare(logMark25);
        logMark25.setLogMark(1L, (long) (byte) 1);
        long long30 = logMark25.getLogFileId();
        long long31 = logMark25.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark(logMark25);
        int int33 = logMark7.compare(logMark25);
        java.nio.ByteBuffer byteBuffer34 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark7.writeLogMark(byteBuffer34);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 97L + "'", long2 == 97L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str8, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + (-1L) + "'", long12 == (-1L));
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 1 + "'", int19 == 1);
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + 0 + "'", int26 == 0);
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 1L + "'", long30 == 1L);
        org.junit.Assert.assertTrue("'" + long31 + "' != '" + 1L + "'", long31 == 1L);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 1 + "'", int33 == 1);
    }

    @Test
    public void test1355() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1355");
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
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 0L + "'", long17 == 0L);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 97L + "'", long19 == 97L);
    }

    @Test
    public void test1356() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1356");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) (short) 10);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test1357() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1357");
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
        logMark32.setLogMark((long) ' ', (long) (byte) -1);
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
    }

    @Test
    public void test1358() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1358");
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
        java.lang.String str38 = logMark37.toString();
        java.nio.ByteBuffer byteBuffer39 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark37.writeLogMark(byteBuffer39);
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
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str36, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str38 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str38, "LogMark: logFileId - 100 , logFileOffset - 97");
    }

    @Test
    public void test1359() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1359");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 0);
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark13 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long14 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        java.lang.String str16 = logMark15.toString();
        logMark15.setLogMark(32L, 9223372036854775807L);
        long long20 = logMark15.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        int int22 = logMark12.compare(logMark21);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        int int24 = logMark2.compare(logMark21);
        long long25 = logMark21.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int32 = logMark28.compare(logMark31);
        logMark28.setLogMark((long) (short) 10, 10L);
        long long36 = logMark28.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark39 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark42 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int43 = logMark39.compare(logMark42);
        logMark39.setLogMark((long) (short) 10, 10L);
        long long47 = logMark39.getLogFileId();
        int int48 = logMark28.compare(logMark39);
        logMark39.setLogMark(97L, (long) '#');
        int int52 = logMark21.compare(logMark39);
        org.apache.bookkeeper.bookie.LogMark logMark53 = new org.apache.bookkeeper.bookie.LogMark(logMark39);
        java.lang.Class<?> wildcardClass54 = logMark53.getClass();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertNotNull(logMark13);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 97L + "'", long14 == 97L);
        org.junit.Assert.assertEquals("'" + str16 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str16, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 32L + "'", long20 == 32L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + (-1) + "'", int24 == (-1));
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 32L + "'", long25 == 32L);
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 0 + "'", int32 == 0);
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 10L + "'", long36 == 10L);
        org.junit.Assert.assertTrue("'" + int43 + "' != '" + 0 + "'", int43 == 0);
        org.junit.Assert.assertTrue("'" + long47 + "' != '" + 10L + "'", long47 == 10L);
        org.junit.Assert.assertTrue("'" + int48 + "' != '" + 0 + "'", int48 == 0);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + (-1) + "'", int52 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass54);
    }

    @Test
    public void test1360() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1360");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) (byte) 10);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileId();
        long long5 = logMark3.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 97L + "'", long4 == 97L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 97L + "'", long5 == 97L);
    }

    @Test
    public void test1361() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1361");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        long long6 = logMark5.getLogFileId();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
    }

    @Test
    public void test1362() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1362");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark7.compare(logMark11);
        long long13 = logMark7.getLogFileOffset();
        logMark7.setLogMark((long) 100, (long) (byte) 100);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 0L + "'", long13 == 0L);
    }

    @Test
    public void test1363() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1363");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 10, (long) ' ');
    }

    @Test
    public void test1364() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1364");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) 10);
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
    public void test1365() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1365");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 100, (long) (byte) 0);
    }

    @Test
    public void test1366() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1366");
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
        java.lang.String str17 = logMark5.toString();
        long long18 = logMark5.getLogFileId();
        java.lang.String str19 = logMark5.toString();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 97L + "'", long8 == 97L);
        org.junit.Assert.assertNotNull(logMark12);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 97L + "'", long13 == 97L);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 97L + "'", long15 == 97L);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertEquals("'" + str17 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str17, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 97L + "'", long18 == 97L);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str19, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test1367() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1367");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, 10L);
        logMark2.setLogMark((long) (-1), (long) (byte) 1);
        java.lang.String str6 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 1" + "'", str6, "LogMark: logFileId - -1 , logFileOffset - 1");
    }

    @Test
    public void test1368() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1368");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 100, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
    }

    @Test
    public void test1369() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1369");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        logMark2.setLogMark((long) ' ', (long) (byte) -1);
        long long8 = logMark2.getLogFileOffset();
        logMark2.setLogMark((long) (short) -1, 32L);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
    }

    @Test
    public void test1370() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1370");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        logMark2.setLogMark((long) (short) 0, (long) '#');
        java.lang.String str6 = logMark2.toString();
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str6, "LogMark: logFileId - 0 , logFileOffset - 35");
    }

    @Test
    public void test1371() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1371");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(35L, (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 10, 0L);
        int int6 = logMark2.compare(logMark5);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 1 + "'", int6 == 1);
    }

    @Test
    public void test1372() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1372");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', 0L);
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 100, (long) '#');
        logMark6.setLogMark((long) (byte) 10, (long) (byte) 100);
        long long10 = logMark6.getLogFileId();
        int int11 = logMark2.compare(logMark6);
        long long12 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + 1 + "'", int11 == 1);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 0L + "'", long12 == 0L);
    }

    @Test
    public void test1373() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1373");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        java.lang.String str5 = logMark2.toString();
        java.lang.String str6 = logMark2.toString();
        logMark2.setLogMark(35L, (long) (short) 100);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass11 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str5, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str6, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test1374() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1374");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 100, (long) 1);
    }

    @Test
    public void test1375() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1375");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int10 = logMark3.compare(logMark8);
        long long11 = logMark8.getLogFileOffset();
        logMark8.setLogMark(10L, (long) (short) 0);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
    }

    @Test
    public void test1376() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1376");
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
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark(logMark17);
        long long20 = logMark19.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 1L + "'", long14 == 1L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 52L + "'", long20 == 52L);
    }

    @Test
    public void test1377() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1377");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        java.lang.Class<?> wildcardClass13 = logMark12.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 0" + "'", str10, "LogMark: logFileId - 10 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test1378() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1378");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 100, (long) '#');
        logMark2.setLogMark((long) (byte) 10, (long) (byte) 100);
        long long6 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = null;
        // The following exception was thrown during execution in test generation
        try {
            int int9 = logMark2.compare(logMark8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 10L + "'", long6 == 10L);
    }

    @Test
    public void test1379() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1379");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        logMark2.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long12 = logMark11.getLogFileOffset();
        int int13 = logMark8.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        long long15 = logMark11.getLogFileOffset();
        int int16 = logMark2.compare(logMark11);
        java.nio.ByteBuffer byteBuffer17 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark11.writeLogMark(byteBuffer17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 0L + "'", long12 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 0L + "'", long15 == 0L);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
    }

    @Test
    public void test1380() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1380");
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
        long long18 = logMark10.getLogFileId();
        java.lang.String str19 = logMark10.toString();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 0L + "'", long17 == 0L);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 97L + "'", long18 == 97L);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str19, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test1381() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1381");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 0);
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long6 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
    }

    @Test
    public void test1382() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1382");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long3 = logMark2.getLogFileId();
        logMark2.setLogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long8 = logMark7.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 1L + "'", long8 == 1L);
    }

    @Test
    public void test1383() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1383");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        logMark1.setLogMark(100L, (long) (byte) 1);
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test1384() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1384");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 100, (long) (byte) -1);
    }

    @Test
    public void test1385() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1385");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark2.setLogMark((long) (byte) 1, (long) '#');
        logMark2.setLogMark((long) 10, (long) ' ');
        long long9 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 32L + "'", long9 == 32L);
    }

    @Test
    public void test1386() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1386");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 10" + "'", str4, "LogMark: logFileId - 1 , logFileOffset - 10");
    }

    @Test
    public void test1387() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1387");
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
        java.nio.ByteBuffer byteBuffer14 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 0L + "'", long9 == 0L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 0" + "'", str11, "LogMark: logFileId - 10 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 1 + "'", int12 == 1);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
    }

    @Test
    public void test1388() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1388");
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
        logMark2.setLogMark((long) 100, (-1L));
        java.nio.ByteBuffer byteBuffer26 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer26);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str9, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str14, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + (-1) + "'", int21 == (-1));
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
    }

    @Test
    public void test1389() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1389");
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
        java.nio.ByteBuffer byteBuffer53 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark52.readLogMark(byteBuffer53);
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
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long48 + "' != '" + 10L + "'", long48 == 10L);
        org.junit.Assert.assertTrue("'" + long49 + "' != '" + 10L + "'", long49 == 10L);
        org.junit.Assert.assertTrue("'" + long50 + "' != '" + 10L + "'", long50 == 10L);
        org.junit.Assert.assertTrue("'" + int51 + "' != '" + 0 + "'", int51 == 0);
    }

    @Test
    public void test1390() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1390");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) (byte) 10);
        long long3 = logMark2.getLogFileOffset();
        long long4 = logMark2.getLogFileId();
        java.lang.Class<?> wildcardClass5 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 10L + "'", long3 == 10L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 97L + "'", long4 == 97L);
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test1391() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1391");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        java.lang.String str3 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str3, "LogMark: logFileId - 0 , logFileOffset - 35");
    }

    @Test
    public void test1392() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1392");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        java.lang.String str9 = logMark2.toString();
        logMark2.setLogMark((long) 10, 35L);
        logMark2.setLogMark((long) (byte) 100, (long) (short) 10);
        logMark2.setLogMark(10L, (long) (short) 0);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str9, "LogMark: logFileId - 0 , logFileOffset - 35");
    }

    @Test
    public void test1393() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1393");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), 9223372036854775807L);
    }

    @Test
    public void test1394() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1394");
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
        java.lang.String str42 = logMark24.toString();
        java.nio.ByteBuffer byteBuffer43 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark24.writeLogMark(byteBuffer43);
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
        org.junit.Assert.assertEquals("'" + str25 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str25, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark29);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + (-1) + "'", int31 == (-1));
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + 0 + "'", int38 == 0);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + (-1) + "'", int39 == (-1));
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + (-1L) + "'", long40 == (-1L));
        org.junit.Assert.assertTrue("'" + int41 + "' != '" + 1 + "'", int41 == 1);
        org.junit.Assert.assertEquals("'" + str42 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str42, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test1395() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1395");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark8.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int13 = logMark2.compare(logMark12);
        long long14 = logMark12.getLogFileOffset();
        long long15 = logMark12.getLogFileOffset();
        java.lang.Class<?> wildcardClass16 = logMark12.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 35L + "'", long14 == 35L);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 35L + "'", long15 == 35L);
        org.junit.Assert.assertNotNull(wildcardClass16);
    }

    @Test
    public void test1396() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1396");
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
        java.nio.ByteBuffer byteBuffer24 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.writeLogMark(byteBuffer24);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + (-1) + "'", int15 == (-1));
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 1 + "'", int23 == 1);
    }

    @Test
    public void test1397() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1397");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        long long5 = logMark3.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 10 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 0L + "'", long5 == 0L);
    }

    @Test
    public void test1398() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1398");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        logMark3.setLogMark((long) 10, 1L);
        logMark3.setLogMark(9223372036854775807L, 0L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test1399() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1399");
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
        java.lang.Class<?> wildcardClass28 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass28);
    }

    @Test
    public void test1400() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1400");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, 35L);
        long long3 = logMark2.getLogFileOffset();
        java.lang.String str4 = logMark2.toString();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 35" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 35");
    }

    @Test
    public void test1401() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1401");
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
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark45 = new org.apache.bookkeeper.bookie.LogMark(logMark44);
        org.apache.bookkeeper.bookie.LogMark logMark48 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark49 = new org.apache.bookkeeper.bookie.LogMark(logMark48);
        int int50 = logMark44.compare(logMark49);
        java.lang.String str51 = logMark44.toString();
        org.apache.bookkeeper.bookie.LogMark logMark54 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark55 = new org.apache.bookkeeper.bookie.LogMark(logMark54);
        java.lang.String str56 = logMark54.toString();
        org.apache.bookkeeper.bookie.LogMark logMark59 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark59.setLogMark((long) (byte) 100, (long) (short) 0);
        int int63 = logMark54.compare(logMark59);
        int int64 = logMark44.compare(logMark54);
        org.apache.bookkeeper.bookie.LogMark logMark67 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark68 = new org.apache.bookkeeper.bookie.LogMark(logMark67);
        java.lang.String str69 = logMark68.toString();
        org.apache.bookkeeper.bookie.LogMark logMark70 = new org.apache.bookkeeper.bookie.LogMark(logMark68);
        long long71 = logMark70.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark74 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark77 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int78 = logMark74.compare(logMark77);
        org.apache.bookkeeper.bookie.LogMark logMark79 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark80 = new org.apache.bookkeeper.bookie.LogMark(logMark79);
        int int81 = logMark74.compare(logMark80);
        int int82 = logMark70.compare(logMark74);
        org.apache.bookkeeper.bookie.LogMark logMark83 = new org.apache.bookkeeper.bookie.LogMark(logMark70);
        org.apache.bookkeeper.bookie.LogMark logMark84 = new org.apache.bookkeeper.bookie.LogMark(logMark83);
        int int85 = logMark54.compare(logMark84);
        int int86 = logMark12.compare(logMark54);
        java.nio.ByteBuffer byteBuffer87 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark54.readLogMark(byteBuffer87);
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
        org.junit.Assert.assertEquals("'" + str25 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str25, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark29);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + (-1) + "'", int31 == (-1));
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + 0 + "'", int38 == 0);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + (-1) + "'", int39 == (-1));
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + (-1L) + "'", long40 == (-1L));
        org.junit.Assert.assertTrue("'" + int41 + "' != '" + 1 + "'", int41 == 1);
        org.junit.Assert.assertTrue("'" + int50 + "' != '" + (-1) + "'", int50 == (-1));
        org.junit.Assert.assertEquals("'" + str51 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str51, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str56 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str56, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int63 + "' != '" + (-1) + "'", int63 == (-1));
        org.junit.Assert.assertTrue("'" + int64 + "' != '" + 0 + "'", int64 == 0);
        org.junit.Assert.assertEquals("'" + str69 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str69, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long71 + "' != '" + 0L + "'", long71 == 0L);
        org.junit.Assert.assertTrue("'" + int78 + "' != '" + 0 + "'", int78 == 0);
        org.junit.Assert.assertNotNull(logMark79);
        org.junit.Assert.assertTrue("'" + int81 + "' != '" + 1 + "'", int81 == 1);
        org.junit.Assert.assertTrue("'" + int82 + "' != '" + 0 + "'", int82 == 0);
        org.junit.Assert.assertTrue("'" + int85 + "' != '" + (-1) + "'", int85 == (-1));
        org.junit.Assert.assertTrue("'" + int86 + "' != '" + 1 + "'", int86 == 1);
    }

    @Test
    public void test1402() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1402");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        logMark2.setLogMark((long) (byte) 0, (long) (byte) 0);
        logMark2.setLogMark(0L, 97L);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test1403() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1403");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark9 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long10 = logMark9.getLogFileOffset();
        long long11 = logMark9.getLogFileOffset();
        int int12 = logMark8.compare(logMark9);
        long long13 = logMark9.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 0);
        long long17 = logMark16.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int24 = logMark20.compare(logMark23);
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark(logMark20);
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark25);
        org.apache.bookkeeper.bookie.LogMark logMark27 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long28 = logMark27.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark(logMark27);
        java.lang.String str30 = logMark29.toString();
        logMark29.setLogMark(32L, 9223372036854775807L);
        long long34 = logMark29.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark35 = new org.apache.bookkeeper.bookie.LogMark(logMark29);
        int int36 = logMark26.compare(logMark35);
        org.apache.bookkeeper.bookie.LogMark logMark37 = new org.apache.bookkeeper.bookie.LogMark(logMark35);
        int int38 = logMark16.compare(logMark35);
        long long39 = logMark35.getLogFileId();
        int int40 = logMark9.compare(logMark35);
        int int41 = logMark5.compare(logMark35);
        long long42 = logMark5.getLogFileId();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertNotNull(logMark9);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 0L + "'", long10 == 0L);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 0L + "'", long11 == 0L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 32L + "'", long17 == 32L);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertNotNull(logMark27);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 0L + "'", long28 == 0L);
        org.junit.Assert.assertEquals("'" + str30 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 0" + "'", str30, "LogMark: logFileId - 10 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 32L + "'", long34 == 32L);
        org.junit.Assert.assertTrue("'" + int36 + "' != '" + 1 + "'", int36 == 1);
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + (-1) + "'", int38 == (-1));
        org.junit.Assert.assertTrue("'" + long39 + "' != '" + 32L + "'", long39 == 32L);
        org.junit.Assert.assertTrue("'" + int40 + "' != '" + (-1) + "'", int40 == (-1));
        org.junit.Assert.assertTrue("'" + int41 + "' != '" + 1 + "'", int41 == 1);
        org.junit.Assert.assertTrue("'" + long42 + "' != '" + 97L + "'", long42 == 97L);
    }

    @Test
    public void test1404() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1404");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 100, (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
    }

    @Test
    public void test1405() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1405");
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
        java.lang.String str60 = logMark3.toString();
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 0L + "'", long5 == 0L);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + (-1) + "'", int6 == (-1));
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
        org.junit.Assert.assertEquals("'" + str60 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 0" + "'", str60, "LogMark: logFileId - 10 , logFileOffset - 0");
    }

    @Test
    public void test1406() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1406");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 0, (long) '4');
    }

    @Test
    public void test1407() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1407");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 100, 100L);
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
    public void test1408() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1408");
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
        java.lang.String str30 = logMark16.toString();
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 10L + "'", long29 == 10L);
        org.junit.Assert.assertEquals("'" + str30 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str30, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test1409() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1409");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 100, 35L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1410() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1410");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        java.lang.String str14 = logMark13.toString();
        logMark13.setLogMark((long) (-1), (long) (byte) 100);
        long long18 = logMark13.getLogFileId();
        logMark13.setLogMark((long) '4', (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        int int23 = logMark8.compare(logMark13);
        int int24 = logMark3.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.String str27 = logMark3.toString();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 0L + "'", long1 == 0L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 10 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str14, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + (-1L) + "'", long18 == (-1L));
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 1 + "'", int23 == 1);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + (-1) + "'", int24 == (-1));
        org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 0" + "'", str27, "LogMark: logFileId - 10 , logFileOffset - 0");
    }

    @Test
    public void test1411() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1411");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 10, (long) (short) 0);
    }

    @Test
    public void test1412() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1412");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 100, 100L);
    }

    @Test
    public void test1413() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1413");
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
        long long40 = logMark26.getLogFileId();
        java.nio.ByteBuffer byteBuffer41 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark26.readLogMark(byteBuffer41);
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
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + 10L + "'", long40 == 10L);
    }

    @Test
    public void test1414() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1414");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), (long) ' ');
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
    public void test1415() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1415");
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
        long long25 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark(logMark28);
        long long30 = logMark28.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark(logMark31);
        int int33 = logMark28.compare(logMark32);
        org.apache.bookkeeper.bookie.LogMark logMark36 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark39 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int40 = logMark36.compare(logMark39);
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark(logMark36);
        int int42 = logMark32.compare(logMark36);
        long long43 = logMark36.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark(logMark36);
        logMark36.setLogMark((long) 10, (long) (byte) -1);
        logMark36.setLogMark((long) ' ', (long) (byte) -1);
        long long51 = logMark36.getLogFileId();
        int int52 = logMark13.compare(logMark36);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 10L + "'", long25 == 10L);
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 0L + "'", long30 == 0L);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 1 + "'", int33 == 1);
        org.junit.Assert.assertTrue("'" + int40 + "' != '" + 0 + "'", int40 == 0);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + (-1) + "'", int42 == (-1));
        org.junit.Assert.assertTrue("'" + long43 + "' != '" + 0L + "'", long43 == 0L);
        org.junit.Assert.assertTrue("'" + long51 + "' != '" + 32L + "'", long51 == 32L);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + (-1) + "'", int52 == (-1));
    }

    @Test
    public void test1416() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1416");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) 10);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test1417() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1417");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 100, (long) 100);
    }

    @Test
    public void test1418() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1418");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 10, (long) (byte) 10);
    }

    @Test
    public void test1419() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1419");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', 9223372036854775807L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
    }

    @Test
    public void test1420() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1420");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str9 = logMark5.toString();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int16 = logMark12.compare(logMark15);
        org.apache.bookkeeper.bookie.LogMark logMark17 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark17);
        int int19 = logMark12.compare(logMark18);
        logMark12.setLogMark((long) 0, (long) '#');
        logMark12.setLogMark((long) (byte) 100, 35L);
        int int26 = logMark5.compare(logMark12);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - -1 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 0 + "'", int16 == 0);
        org.junit.Assert.assertNotNull(logMark17);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 1 + "'", int19 == 1);
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + (-1) + "'", int26 == (-1));
    }

    @Test
    public void test1421() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1421");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 10, (long) 10);
    }

    @Test
    public void test1422() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1422");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long3 = logMark2.getLogFileId();
        logMark2.setLogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long8 = logMark7.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int11 = logMark2.compare(logMark7);
        java.lang.String str12 = logMark7.toString();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + (-1) + "'", int11 == (-1));
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 0" + "'", str12, "LogMark: logFileId - 10 , logFileOffset - 0");
    }

    @Test
    public void test1423() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1423");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 9223372036854775807L);
        java.lang.String str3 = logMark2.toString();
        java.lang.Class<?> wildcardClass4 = logMark2.getClass();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 9223372036854775807" + "'", str3, "LogMark: logFileId - 35 , logFileOffset - 9223372036854775807");
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test1424() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1424");
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
        java.lang.String str30 = logMark11.toString();
        logMark11.setLogMark((long) (byte) 1, (long) '4');
        logMark11.setLogMark((long) 10, (long) (byte) 10);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 0" + "'", str2, "LogMark: logFileId - 10 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertEquals("'" + str20 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str20, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 100L + "'", long28 == 100L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertEquals("'" + str30 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 0" + "'", str30, "LogMark: logFileId - 10 , logFileOffset - 0");
    }

    @Test
    public void test1425() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1425");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, 97L);
        long long3 = logMark2.getLogFileOffset();
        long long4 = logMark2.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 97L + "'", long3 == 97L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 97L + "'", long4 == 97L);
    }

    @Test
    public void test1426() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1426");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, (long) (byte) 100);
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
    public void test1427() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1427");
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
        logMark23.setLogMark((long) 10, (long) (short) 10);
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
    public void test1428() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1428");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        logMark2.setLogMark((long) (short) 0, (long) '#');
        long long6 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long8 = logMark7.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        java.lang.String str11 = logMark10.toString();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        long long13 = logMark10.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int20 = logMark16.compare(logMark19);
        logMark19.setLogMark(1L, (long) (byte) 1);
        long long24 = logMark19.getLogFileId();
        long long25 = logMark19.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark19);
        logMark26.setLogMark(35L, (long) '4');
        int int30 = logMark10.compare(logMark26);
        int int31 = logMark2.compare(logMark10);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 10L + "'", long8 == 10L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str11, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 1L + "'", long24 == 1L);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 1L + "'", long25 == 1L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + (-1) + "'", int30 == (-1));
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + (-1) + "'", int31 == (-1));
    }

    @Test
    public void test1429() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1429");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark(10L, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        logMark3.setLogMark((long) 0, (long) 10);
    }

    @Test
    public void test1430() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1430");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, (long) 100);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark4 = null;
        // The following exception was thrown during execution in test generation
        try {
            int int5 = logMark2.compare(logMark4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
    }

    @Test
    public void test1431() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1431");
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
        java.lang.String str19 = logMark8.toString();
        long long20 = logMark8.getLogFileOffset();
        java.lang.String str21 = logMark8.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark9);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str12, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 32L + "'", long16 == 32L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str19, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 0L + "'", long20 == 0L);
        org.junit.Assert.assertEquals("'" + str21 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str21, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test1432() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1432");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark((long) (byte) 10, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        java.lang.String str8 = logMark7.toString();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        java.lang.String str13 = logMark12.toString();
        logMark12.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark17 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark17);
        int int19 = logMark12.compare(logMark17);
        int int20 = logMark7.compare(logMark17);
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark17);
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark(logMark24);
        java.lang.String str26 = logMark24.toString();
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark29.setLogMark((long) (byte) 100, (long) (short) 0);
        int int33 = logMark24.compare(logMark29);
        long long34 = logMark29.getLogFileId();
        int int35 = logMark17.compare(logMark29);
        long long36 = logMark17.getLogFileId();
        java.lang.String str37 = logMark17.toString();
        int int38 = logMark0.compare(logMark17);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 10L + "'", long2 == 10L);
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 35" + "'", str8, "LogMark: logFileId - 10 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str13, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark17);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str26, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + (-1) + "'", int33 == (-1));
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 100L + "'", long34 == 100L);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 10L + "'", long36 == 10L);
        org.junit.Assert.assertEquals("'" + str37 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 35" + "'", str37, "LogMark: logFileId - 10 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + 0 + "'", int38 == 0);
    }

    @Test
    public void test1433() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1433");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int10 = logMark3.compare(logMark8);
        long long11 = logMark3.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 10L + "'", long11 == 10L);
    }

    @Test
    public void test1434() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1434");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        long long3 = logMark2.getLogFileId();
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark7.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark12.compare(logMark16);
        long long18 = logMark12.getLogFileOffset();
        int int19 = logMark2.compare(logMark12);
        java.lang.String str20 = logMark12.toString();
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 1" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 1");
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 0L + "'", long18 == 0L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
        org.junit.Assert.assertEquals("'" + str20 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str20, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test1435() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1435");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark8.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int13 = logMark2.compare(logMark12);
        java.lang.String str14 = logMark2.toString();
        logMark2.setLogMark((long) 100, 32L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str14, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test1436() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1436");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), 10L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark2.setLogMark((long) (byte) -1, (long) '#');
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 10L + "'", long3 == 10L);
    }

    @Test
    public void test1437() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1437");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(9223372036854775807L, (long) '4');
    }

    @Test
    public void test1438() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1438");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        long long13 = logMark12.getLogFileOffset();
        long long14 = logMark12.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long18 = logMark17.getLogFileId();
        int int19 = logMark12.compare(logMark17);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        logMark20.setLogMark(1L, 100L);
        int int24 = logMark9.compare(logMark20);
        java.lang.String str25 = logMark20.toString();
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(9223372036854775807L, (-1L));
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark(logMark28);
        int int30 = logMark20.compare(logMark28);
        java.nio.ByteBuffer byteBuffer31 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark20.writeLogMark(byteBuffer31);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 35L + "'", long13 == 35L);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 1L + "'", long14 == 1L);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 100L + "'", long18 == 100L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + (-1) + "'", int24 == (-1));
        org.junit.Assert.assertEquals("'" + str25 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 100" + "'", str25, "LogMark: logFileId - 1 , logFileOffset - 100");
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + (-1) + "'", int30 == (-1));
    }

    @Test
    public void test1439() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1439");
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
        org.apache.bookkeeper.bookie.LogMark logMark60 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long61 = logMark60.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark62 = new org.apache.bookkeeper.bookie.LogMark(logMark60);
        org.apache.bookkeeper.bookie.LogMark logMark63 = new org.apache.bookkeeper.bookie.LogMark(logMark60);
        org.apache.bookkeeper.bookie.LogMark logMark66 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), (long) 10);
        int int67 = logMark60.compare(logMark66);
        int int68 = logMark34.compare(logMark60);
        org.apache.bookkeeper.bookie.LogMark logMark69 = new org.apache.bookkeeper.bookie.LogMark(logMark34);
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 35L + "'", long4 == 35L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 35L + "'", long5 == 35L);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + (-1) + "'", int6 == (-1));
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
        org.junit.Assert.assertTrue("'" + int59 + "' != '" + 1 + "'", int59 == 1);
        org.junit.Assert.assertNotNull(logMark60);
        org.junit.Assert.assertTrue("'" + long61 + "' != '" + 35L + "'", long61 == 35L);
        org.junit.Assert.assertTrue("'" + int67 + "' != '" + 1 + "'", int67 == 1);
        org.junit.Assert.assertTrue("'" + int68 + "' != '" + (-1) + "'", int68 == (-1));
    }

    @Test
    public void test1440() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1440");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        java.lang.String str5 = logMark2.toString();
        long long6 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str5, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 97L + "'", long6 == 97L);
    }

    @Test
    public void test1441() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1441");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 0, 9223372036854775807L);
        java.lang.String str3 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str5 = logMark4.toString();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 9223372036854775807" + "'", str3, "LogMark: logFileId - 0 , logFileOffset - 9223372036854775807");
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 9223372036854775807" + "'", str5, "LogMark: logFileId - 0 , logFileOffset - 9223372036854775807");
    }

    @Test
    public void test1442() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1442");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(10L, (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass4 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test1443() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1443");
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
        org.apache.bookkeeper.bookie.LogMark logMark32 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
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
    }

    @Test
    public void test1444() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1444");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int14 = logMark10.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        logMark15.setLogMark((long) (short) 100, 35L);
        logMark15.setLogMark(32L, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
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
        java.lang.String str48 = logMark38.toString();
        int int49 = logMark24.compare(logMark38);
        long long50 = logMark38.getLogFileId();
        int int51 = logMark15.compare(logMark38);
        int int52 = logMark5.compare(logMark38);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertEquals("'" + str48 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str48, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int49 + "' != '" + (-1) + "'", int49 == (-1));
        org.junit.Assert.assertTrue("'" + long50 + "' != '" + 10L + "'", long50 == 10L);
        org.junit.Assert.assertTrue("'" + int51 + "' != '" + 1 + "'", int51 == 1);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 1 + "'", int52 == 1);
    }

    @Test
    public void test1445() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1445");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) -1, 1L);
    }

    @Test
    public void test1446() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1446");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) -1, (long) (byte) 0);
    }

    @Test
    public void test1447() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1447");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        java.lang.String str8 = logMark7.toString();
        logMark7.setLogMark((long) (-1), (long) (byte) 100);
        long long12 = logMark7.getLogFileId();
        logMark7.setLogMark((long) '4', (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        long long17 = logMark7.getLogFileId();
        int int18 = logMark3.compare(logMark7);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str8, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + (-1L) + "'", long12 == (-1L));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 52L + "'", long17 == 52L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + (-1) + "'", int18 == (-1));
    }

    @Test
    public void test1448() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1448");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 10, (long) (short) -1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
    }

    @Test
    public void test1449() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1449");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 100, 52L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass4 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test1450() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1450");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 9223372036854775807L);
        long long3 = logMark2.getLogFileId();
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 9223372036854775807L + "'", long4 == 9223372036854775807L);
    }

    @Test
    public void test1451() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1451");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        long long5 = logMark4.getLogFileId();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 35" + "'", str2, "LogMark: logFileId - 10 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 10L + "'", long5 == 10L);
    }

    @Test
    public void test1452() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1452");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        long long12 = logMark8.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 35" + "'", str10, "LogMark: logFileId - 10 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 35L + "'", long12 == 35L);
    }

    @Test
    public void test1453() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1453");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (byte) -1, (long) (-1));
        java.lang.String str10 = logMark2.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - -1" + "'", str10, "LogMark: logFileId - -1 , logFileOffset - -1");
    }

    @Test
    public void test1454() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1454");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(97L, 35L);
        long long7 = logMark6.getLogFileOffset();
        int int8 = logMark2.compare(logMark6);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark6.readLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 35L + "'", long7 == 35L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 1 + "'", int8 == 1);
    }

    @Test
    public void test1455() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1455");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        long long3 = logMark2.getLogFileId();
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark7.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int17 = logMark12.compare(logMark16);
        long long18 = logMark12.getLogFileOffset();
        int int19 = logMark2.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark(logMark22);
        org.apache.bookkeeper.bookie.LogMark logMark24 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long25 = logMark24.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark24);
        java.lang.String str27 = logMark26.toString();
        int int28 = logMark23.compare(logMark26);
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        int int30 = logMark12.compare(logMark26);
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark(logMark26);
        long long32 = logMark26.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 1" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 1");
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 0L + "'", long18 == 0L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
        org.junit.Assert.assertNotNull(logMark24);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 35L + "'", long25 == 35L);
        org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 35" + "'", str27, "LogMark: logFileId - 10 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int28 + "' != '" + 1 + "'", int28 == 1);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 1 + "'", int30 == 1);
        org.junit.Assert.assertTrue("'" + long32 + "' != '" + 10L + "'", long32 == 10L);
    }

    @Test
    public void test1456() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1456");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark1.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 35" + "'", str2, "LogMark: logFileId - 10 , logFileOffset - 35");
    }

    @Test
    public void test1457() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1457");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long7 = logMark2.getLogFileOffset();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 0L + "'", long7 == 0L);
    }

    @Test
    public void test1458() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1458");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 100, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark5);
        java.lang.String str8 = logMark2.toString();
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 35" + "'", str8, "LogMark: logFileId - 100 , logFileOffset - 35");
    }

    @Test
    public void test1459() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1459");
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
        long long19 = logMark5.getLogFileId();
        java.nio.ByteBuffer byteBuffer20 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer20);
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
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 97L + "'", long19 == 97L);
    }

    @Test
    public void test1460() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1460");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark(100L, (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int14 = logMark10.compare(logMark13);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        logMark15.setLogMark((long) (short) 100, 35L);
        logMark15.setLogMark(32L, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
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
        java.lang.String str48 = logMark38.toString();
        int int49 = logMark24.compare(logMark38);
        long long50 = logMark38.getLogFileId();
        int int51 = logMark15.compare(logMark38);
        int int52 = logMark7.compare(logMark38);
        java.nio.ByteBuffer byteBuffer53 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark38.readLogMark(byteBuffer53);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertEquals("'" + str48 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str48, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int49 + "' != '" + (-1) + "'", int49 == (-1));
        org.junit.Assert.assertTrue("'" + long50 + "' != '" + 10L + "'", long50 == 10L);
        org.junit.Assert.assertTrue("'" + int51 + "' != '" + 1 + "'", int51 == 1);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 1 + "'", int52 == 1);
    }

    @Test
    public void test1461() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1461");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long8 = logMark7.getLogFileId();
        long long9 = logMark7.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        logMark7.setLogMark((long) (short) 100, (long) (byte) 1);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 97L + "'", long8 == 97L);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 97L + "'", long9 == 97L);
    }

    @Test
    public void test1462() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1462");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark5.setLogMark((long) (short) 100, 35L);
        java.lang.String str9 = logMark5.toString();
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 35" + "'", str9, "LogMark: logFileId - 100 , logFileOffset - 35");
    }

    @Test
    public void test1463() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1463");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark((long) 100, (long) 'a');
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long7 = logMark6.getLogFileOffset();
        int int8 = logMark0.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        logMark9.setLogMark((long) 0, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int19 = logMark15.compare(logMark18);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark20);
        org.apache.bookkeeper.bookie.LogMark logMark22 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long23 = logMark22.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark(logMark22);
        java.lang.String str25 = logMark24.toString();
        logMark24.setLogMark(32L, 9223372036854775807L);
        long long29 = logMark24.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark30 = new org.apache.bookkeeper.bookie.LogMark(logMark24);
        int int31 = logMark21.compare(logMark30);
        long long32 = logMark30.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark35 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark36 = new org.apache.bookkeeper.bookie.LogMark(logMark35);
        long long37 = logMark35.getLogFileOffset();
        java.lang.String str38 = logMark35.toString();
        java.lang.String str39 = logMark35.toString();
        logMark35.setLogMark(35L, (long) (short) 100);
        org.apache.bookkeeper.bookie.LogMark logMark45 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (short) -1);
        int int46 = logMark35.compare(logMark45);
        long long47 = logMark45.getLogFileId();
        int int48 = logMark30.compare(logMark45);
        int int49 = logMark9.compare(logMark45);
        org.apache.bookkeeper.bookie.LogMark logMark50 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        org.junit.Assert.assertNotNull(logMark0);
// flaky "31) test1463(Regression3Test)":         org.junit.Assert.assertTrue("'" + long2 + "' != '" + 97L + "'", long2 == 97L);
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertNotNull(logMark22);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 97L + "'", long23 == 97L);
        org.junit.Assert.assertEquals("'" + str25 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str25, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 32L + "'", long29 == 32L);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 1 + "'", int31 == 1);
        org.junit.Assert.assertTrue("'" + long32 + "' != '" + 32L + "'", long32 == 32L);
        org.junit.Assert.assertTrue("'" + long37 + "' != '" + 0L + "'", long37 == 0L);
        org.junit.Assert.assertEquals("'" + str38 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str38, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str39 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str39, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int46 + "' != '" + 1 + "'", int46 == 1);
        org.junit.Assert.assertTrue("'" + long47 + "' != '" + 0L + "'", long47 == 0L);
        org.junit.Assert.assertTrue("'" + int48 + "' != '" + 1 + "'", int48 == 1);
        org.junit.Assert.assertTrue("'" + int49 + "' != '" + 1 + "'", int49 == 1);
    }

    @Test
    public void test1464() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1464");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long4 = logMark3.getLogFileOffset();
        long long5 = logMark3.getLogFileOffset();
        int int6 = logMark2.compare(logMark3);
        java.lang.String str7 = logMark3.toString();
        java.lang.String str8 = logMark3.toString();
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 97L + "'", long4 == 97L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 97L + "'", long5 == 97L);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + (-1) + "'", int6 == (-1));
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str7, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str8, "LogMark: logFileId - 100 , logFileOffset - 97");
    }

    @Test
    public void test1465() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1465");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 10, (long) 0);
        java.lang.String str3 = logMark2.toString();
        long long4 = logMark2.getLogFileOffset();
        java.lang.String str5 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 0" + "'", str3, "LogMark: logFileId - 10 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 0" + "'", str5, "LogMark: logFileId - 10 , logFileOffset - 0");
    }

    @Test
    public void test1466() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1466");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        logMark2.setLogMark((long) 0, (long) '#');
        java.lang.String str13 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long15 = logMark2.getLogFileOffset();
        java.lang.Class<?> wildcardClass16 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str13, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 35L + "'", long15 == 35L);
        org.junit.Assert.assertNotNull(wildcardClass16);
    }

    @Test
    public void test1467() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1467");
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
        org.apache.bookkeeper.bookie.LogMark logMark49 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark52 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int53 = logMark49.compare(logMark52);
        logMark49.setLogMark((long) (short) 10, 10L);
        long long57 = logMark49.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark58 = new org.apache.bookkeeper.bookie.LogMark(logMark49);
        int int59 = logMark41.compare(logMark49);
        java.lang.Class<?> wildcardClass60 = logMark49.getClass();
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
        org.junit.Assert.assertTrue("'" + int53 + "' != '" + 0 + "'", int53 == 0);
        org.junit.Assert.assertTrue("'" + long57 + "' != '" + 10L + "'", long57 == 10L);
        org.junit.Assert.assertTrue("'" + int59 + "' != '" + (-1) + "'", int59 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass60);
    }

    @Test
    public void test1468() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1468");
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
        logMark1.setLogMark(32L, 0L);
        logMark1.setLogMark((long) '4', (long) (short) 1);
        long long24 = logMark1.getLogFileOffset();
        java.lang.Class<?> wildcardClass25 = logMark1.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 1L + "'", long24 == 1L);
        org.junit.Assert.assertNotNull(wildcardClass25);
    }

    @Test
    public void test1469() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1469");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        logMark2.setLogMark(32L, 9223372036854775807L);
        long long7 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long10 = logMark9.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str3, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 32L + "'", long7 == 32L);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 9223372036854775807L + "'", long10 == 9223372036854775807L);
    }

    @Test
    public void test1470() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1470");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 10, (long) (short) -1);
    }

    @Test
    public void test1471() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1471");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        long long6 = logMark5.getLogFileId();
        long long7 = logMark5.getLogFileId();
        int int8 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long10 = logMark2.getLogFileId();
        long long11 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 0L + "'", long7 == 0L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 1 + "'", int8 == 1);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 52L + "'", long10 == 52L);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 52L + "'", long11 == 52L);
    }

    @Test
    public void test1472() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1472");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 9223372036854775807L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str5 = logMark2.toString();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 9223372036854775807L + "'", long3 == 9223372036854775807L);
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 9223372036854775807" + "'", str5, "LogMark: logFileId - 1 , logFileOffset - 9223372036854775807");
    }

    @Test
    public void test1473() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1473");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        long long6 = logMark5.getLogFileId();
        long long7 = logMark5.getLogFileId();
        int int8 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 0L + "'", long7 == 0L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 1 + "'", int8 == 1);
    }

    @Test
    public void test1474() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1474");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, (long) (byte) 10);
    }

    @Test
    public void test1475() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1475");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        long long3 = logMark2.getLogFileId();
        java.lang.String str4 = logMark2.toString();
        long long5 = logMark2.getLogFileId();
        java.lang.String str6 = logMark2.toString();
        java.lang.String str7 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        java.lang.String str12 = logMark10.toString();
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark17);
        java.lang.String str19 = logMark18.toString();
        logMark18.setLogMark((long) (-1), (long) (byte) 100);
        logMark18.setLogMark((long) ' ', 9223372036854775807L);
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int32 = logMark28.compare(logMark31);
        org.apache.bookkeeper.bookie.LogMark logMark33 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark34 = new org.apache.bookkeeper.bookie.LogMark(logMark33);
        int int35 = logMark28.compare(logMark34);
        int int36 = logMark18.compare(logMark34);
        long long37 = logMark34.getLogFileId();
        long long38 = logMark34.getLogFileOffset();
        int int39 = logMark10.compare(logMark34);
        int int40 = logMark2.compare(logMark34);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 1" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 1");
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 0L + "'", long5 == 0L);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 1" + "'", str6, "LogMark: logFileId - 0 , logFileOffset - 1");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 1" + "'", str7, "LogMark: logFileId - 0 , logFileOffset - 1");
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str12, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str19, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 0 + "'", int32 == 0);
        org.junit.Assert.assertNotNull(logMark33);
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
        org.junit.Assert.assertTrue("'" + int36 + "' != '" + (-1) + "'", int36 == (-1));
        org.junit.Assert.assertTrue("'" + long37 + "' != '" + 100L + "'", long37 == 100L);
        org.junit.Assert.assertTrue("'" + long38 + "' != '" + 97L + "'", long38 == 97L);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + (-1) + "'", int39 == (-1));
        org.junit.Assert.assertTrue("'" + int40 + "' != '" + (-1) + "'", int40 == (-1));
    }

    @Test
    public void test1476() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1476");
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
        java.lang.String str15 = logMark11.toString();
        logMark11.setLogMark(97L, (long) '#');
        java.nio.ByteBuffer byteBuffer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark11.readLogMark(byteBuffer19);
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
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str15, "LogMark: logFileId - 100 , logFileOffset - 97");
    }

    @Test
    public void test1477() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1477");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, 32L);
        long long3 = logMark2.getLogFileOffset();
        long long4 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 32L + "'", long4 == 32L);
    }

    @Test
    public void test1478() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1478");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        logMark2.setLogMark((long) 0, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark14 = null;
        // The following exception was thrown during execution in test generation
        try {
            int int15 = logMark2.compare(logMark14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
    }

    @Test
    public void test1479() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1479");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark5.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark();
        int int11 = logMark5.compare(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int18 = logMark14.compare(logMark17);
        logMark14.setLogMark((long) (short) 10, 10L);
        long long22 = logMark14.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int29 = logMark25.compare(logMark28);
        logMark25.setLogMark((long) (short) 10, 10L);
        long long33 = logMark25.getLogFileId();
        int int34 = logMark14.compare(logMark25);
        long long35 = logMark25.getLogFileOffset();
        long long36 = logMark25.getLogFileId();
        java.lang.String str37 = logMark25.toString();
        int int38 = logMark10.compare(logMark25);
        java.lang.String str39 = logMark10.toString();
        logMark10.setLogMark(1L, 100L);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + 1 + "'", int11 == 1);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 0 + "'", int18 == 0);
        org.junit.Assert.assertTrue("'" + long22 + "' != '" + 10L + "'", long22 == 10L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + 0 + "'", int29 == 0);
        org.junit.Assert.assertTrue("'" + long33 + "' != '" + 10L + "'", long33 == 10L);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 10L + "'", long36 == 10L);
        org.junit.Assert.assertEquals("'" + str37 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str37, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + (-1) + "'", int38 == (-1));
        org.junit.Assert.assertEquals("'" + str39 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str39, "LogMark: logFileId - 0 , logFileOffset - 0");
    }

    @Test
    public void test1480() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1480");
        org.apache.bookkeeper.bookie.LogMark logMark0 = new org.apache.bookkeeper.bookie.LogMark();
        java.lang.String str1 = logMark0.toString();
        java.lang.String str2 = logMark0.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str7 = logMark6.toString();
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
        long long23 = logMark9.getLogFileId();
        logMark9.setLogMark((long) 100, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark27 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark27);
        logMark28.setLogMark((long) (short) -1, 0L);
        int int32 = logMark9.compare(logMark28);
        int int33 = logMark6.compare(logMark9);
        int int34 = logMark0.compare(logMark6);
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str1, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str2, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str7, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 35" + "'", str10, "LogMark: logFileId - 97 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str15, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark19);
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + (-1) + "'", int21 == (-1));
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 97L + "'", long23 == 97L);
        org.junit.Assert.assertNotNull(logMark27);
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 1 + "'", int32 == 1);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + (-1) + "'", int33 == (-1));
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
    }

    @Test
    public void test1481() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1481");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        java.lang.String str3 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        logMark4.setLogMark((long) (short) 0, (long) '#');
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 35" + "'", str3, "LogMark: logFileId - 97 , logFileOffset - 35");
    }

    @Test
    public void test1482() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1482");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        java.lang.Class<?> wildcardClass4 = logMark1.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 35" + "'", str2, "LogMark: logFileId - 97 , logFileOffset - 35");
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test1483() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1483");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, 35L);
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
    public void test1484() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1484");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark7.setLogMark((long) (short) 100, 35L);
        logMark7.setLogMark(32L, 1L);
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
        int int43 = logMark7.compare(logMark30);
        java.nio.ByteBuffer byteBuffer44 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark30.readLogMark(byteBuffer44);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 0 + "'", int23 == 0);
        org.junit.Assert.assertTrue("'" + long27 + "' != '" + 10L + "'", long27 == 10L);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
        org.junit.Assert.assertTrue("'" + long38 + "' != '" + 10L + "'", long38 == 10L);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + 0 + "'", int39 == 0);
        org.junit.Assert.assertEquals("'" + str40 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str40, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int41 + "' != '" + (-1) + "'", int41 == (-1));
        org.junit.Assert.assertTrue("'" + long42 + "' != '" + 10L + "'", long42 == 10L);
        org.junit.Assert.assertTrue("'" + int43 + "' != '" + 1 + "'", int43 == 1);
    }

    @Test
    public void test1485() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1485");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(97L, (long) (short) 0);
        int int10 = logMark6.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark((-1L), 100L);
        int int15 = logMark11.compare(logMark14);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + 1 + "'", int15 == 1);
    }

    @Test
    public void test1486() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1486");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        logMark2.setLogMark((long) (short) 10, (long) (short) 1);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - -1" + "'", str4, "LogMark: logFileId - 52 , logFileOffset - -1");
    }

    @Test
    public void test1487() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1487");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        logMark3.setLogMark((long) '4', (long) '4');
        java.lang.String str12 = logMark3.toString();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 52" + "'", str12, "LogMark: logFileId - 52 , logFileOffset - 52");
    }

    @Test
    public void test1488() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1488");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long6 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long8 = logMark7.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        java.lang.String str11 = logMark10.toString();
        int int12 = logMark2.compare(logMark10);
        java.lang.Class<?> wildcardClass13 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 97L + "'", long6 == 97L);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 35L + "'", long8 == 35L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 35" + "'", str11, "LogMark: logFileId - 97 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test1489() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1489");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) -1, 0L);
        java.lang.String str3 = logMark2.toString();
        long long4 = logMark2.getLogFileId();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + (-1L) + "'", long4 == (-1L));
    }

    @Test
    public void test1490() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1490");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark2.setLogMark((long) (byte) 100, (long) (short) 0);
        logMark2.setLogMark(32L, 9223372036854775807L);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1491() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1491");
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
        java.nio.ByteBuffer byteBuffer30 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark13.readLogMark(byteBuffer30);
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
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 97L + "'", long28 == 97L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
    }

    @Test
    public void test1492() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1492");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        long long8 = logMark3.getLogFileId();
        java.lang.Class<?> wildcardClass9 = logMark3.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test1493() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1493");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        long long7 = logMark6.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 0);
        long long11 = logMark10.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        int int14 = logMark6.compare(logMark10);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 32L + "'", long11 == 32L);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 1 + "'", int14 == 1);
    }

    @Test
    public void test1494() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1494");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark((long) 100, (long) 'a');
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long7 = logMark6.getLogFileOffset();
        int int8 = logMark0.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        java.lang.String str10 = logMark6.toString();
        java.lang.String str11 = logMark6.toString();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 35L + "'", long2 == 35L);
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str10, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str11, "LogMark: logFileId - 100 , logFileOffset - 97");
    }

    @Test
    public void test1495() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1495");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 10, (long) 0);
        long long3 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
    }

    @Test
    public void test1496() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1496");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 100, (long) (short) 1);
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
    public void test1497() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1497");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        java.lang.String str5 = logMark2.toString();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 0" + "'", str5, "LogMark: logFileId - 1 , logFileOffset - 0");
    }

    @Test
    public void test1498() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1498");
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
        long long52 = logMark27.getLogFileOffset();
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
        org.junit.Assert.assertTrue("'" + long52 + "' != '" + 10L + "'", long52 == 10L);
    }

    @Test
    public void test1499() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1499");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark13 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long14 = logMark13.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        java.lang.String str16 = logMark15.toString();
        logMark15.setLogMark(32L, 9223372036854775807L);
        long long20 = logMark15.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark15);
        int int22 = logMark12.compare(logMark21);
        long long23 = logMark12.getLogFileOffset();
        java.lang.String str24 = logMark12.toString();
        org.apache.bookkeeper.bookie.LogMark logMark25 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        int int26 = logMark0.compare(logMark25);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertNotNull(logMark13);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 97L + "'", long14 == 97L);
        org.junit.Assert.assertEquals("'" + str16 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str16, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 32L + "'", long20 == 32L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 0L + "'", long23 == 0L);
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str24, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + 1 + "'", int26 == 1);
    }

    @Test
    public void test1500() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1500");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long3 = logMark2.getLogFileId();
        logMark2.setLogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long8 = logMark7.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int11 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        java.lang.String str16 = logMark14.toString();
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        int int18 = logMark2.compare(logMark17);
        java.lang.String str19 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark20 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long21 = logMark20.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark20);
        java.lang.String str23 = logMark22.toString();
        long long24 = logMark22.getLogFileOffset();
        logMark22.setLogMark(52L, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark22);
        int int29 = logMark2.compare(logMark28);
        logMark2.setLogMark((long) '4', 0L);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 97L + "'", long8 == 97L);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + (-1) + "'", int11 == (-1));
        org.junit.Assert.assertEquals("'" + str16 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str16, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + (-1) + "'", int18 == (-1));
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 0" + "'", str19, "LogMark: logFileId - 1 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark20);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 97L + "'", long21 == 97L);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str23, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 97L + "'", long24 == 97L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
    }
}
