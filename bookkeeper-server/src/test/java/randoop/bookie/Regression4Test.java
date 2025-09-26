package randoop.bookie;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class Regression4Test {

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
    public void test1501() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1501");
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
        java.lang.String str19 = logMark12.toString();
        java.lang.String str20 = logMark12.toString();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 1L + "'", long14 == 1L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str19, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str20 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str20, "LogMark: logFileId - 1 , logFileOffset - 35");
    }

    @Test
    public void test1502() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1502");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long3 = logMark2.getLogFileId();
        logMark2.setLogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long8 = logMark7.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int11 = logMark2.compare(logMark7);
        java.lang.String str12 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark13 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        java.lang.String str15 = logMark14.toString();
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        int int17 = logMark2.compare(logMark16);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
        org.junit.Assert.assertNotNull(logMark7);
// flaky "1) test1502(Regression4Test)":         org.junit.Assert.assertTrue("'" + long8 + "' != '" + 97L + "'", long8 == 97L);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + (-1) + "'", int11 == (-1));
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 0" + "'", str12, "LogMark: logFileId - 1 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark13);
// flaky "1) test1502(Regression4Test)":         org.junit.Assert.assertEquals("'" + str15 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str15, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + (-1) + "'", int17 == (-1));
    }

    @Test
    public void test1503() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1503");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(10L, (long) 1);
        long long3 = logMark2.getLogFileOffset();
        long long4 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 1L + "'", long3 == 1L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 10L + "'", long4 == 10L);
    }

    @Test
    public void test1504() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1504");
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
        org.apache.bookkeeper.bookie.LogMark logMark53 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        java.lang.Class<?> wildcardClass54 = logMark21.getClass();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertNotNull(logMark13);
// flaky "2) test1504(Regression4Test)":         org.junit.Assert.assertTrue("'" + long14 + "' != '" + 97L + "'", long14 == 97L);
// flaky "2) test1504(Regression4Test)":         org.junit.Assert.assertEquals("'" + str16 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str16, "LogMark: logFileId - 100 , logFileOffset - 97");
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
    public void test1505() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1505");
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
        java.lang.String str23 = logMark22.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + (-1) + "'", int21 == (-1));
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test1506() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1506");
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
        long long13 = logMark10.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark10);
        long long15 = logMark10.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 97L + "'", long6 == 97L);
        org.junit.Assert.assertNotNull(logMark7);
// flaky "3) test1506(Regression4Test)":         org.junit.Assert.assertTrue("'" + long8 + "' != '" + 97L + "'", long8 == 97L);
// flaky "3) test1506(Regression4Test)":         org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str11, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
// flaky "1) test1506(Regression4Test)":         org.junit.Assert.assertTrue("'" + long13 + "' != '" + 100L + "'", long13 == 100L);
// flaky "1) test1506(Regression4Test)":         org.junit.Assert.assertTrue("'" + long15 + "' != '" + 97L + "'", long15 == 97L);
    }

    @Test
    public void test1507() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1507");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        java.lang.String str3 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark1.compare(logMark6);
        long long9 = logMark1.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "4) test1507(Regression4Test)":         org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str3, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
// flaky "4) test1507(Regression4Test)":         org.junit.Assert.assertTrue("'" + long9 + "' != '" + 97L + "'", long9 == 97L);
    }

    @Test
    public void test1508() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1508");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 0);
        logMark2.setLogMark((long) '#', 1L);
    }

    @Test
    public void test1509() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1509");
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
        java.lang.String str47 = logMark45.toString();
        org.apache.bookkeeper.bookie.LogMark logMark50 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark51 = new org.apache.bookkeeper.bookie.LogMark(logMark50);
        java.lang.String str52 = logMark51.toString();
        logMark51.setLogMark((long) (short) -1, 10L);
        long long56 = logMark51.getLogFileId();
        int int57 = logMark45.compare(logMark51);
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
        org.junit.Assert.assertEquals("'" + str47 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 100" + "'", str47, "LogMark: logFileId - 10 , logFileOffset - 100");
        org.junit.Assert.assertEquals("'" + str52 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str52, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long56 + "' != '" + (-1L) + "'", long56 == (-1L));
        org.junit.Assert.assertTrue("'" + int57 + "' != '" + 1 + "'", int57 == 1);
    }

    @Test
    public void test1510() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1510");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(52L, (long) (byte) 100);
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
        long long26 = logMark16.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark29 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark30 = new org.apache.bookkeeper.bookie.LogMark(logMark29);
        java.lang.String str31 = logMark30.toString();
        logMark30.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark35 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark36 = new org.apache.bookkeeper.bookie.LogMark(logMark35);
        int int37 = logMark30.compare(logMark35);
        int int38 = logMark16.compare(logMark35);
        org.apache.bookkeeper.bookie.LogMark logMark39 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long40 = logMark39.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark(logMark39);
        org.apache.bookkeeper.bookie.LogMark logMark42 = new org.apache.bookkeeper.bookie.LogMark(logMark39);
        java.lang.String str43 = logMark42.toString();
        logMark42.setLogMark(0L, (long) (byte) 0);
        logMark42.setLogMark(100L, (long) 1);
        int int50 = logMark35.compare(logMark42);
        int int51 = logMark2.compare(logMark35);
        org.apache.bookkeeper.bookie.LogMark logMark54 = new org.apache.bookkeeper.bookie.LogMark(97L, 35L);
        long long55 = logMark54.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark56 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        int int57 = logMark54.compare(logMark56);
        int int58 = logMark2.compare(logMark54);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertTrue("'" + long26 + "' != '" + 10L + "'", long26 == 10L);
        org.junit.Assert.assertEquals("'" + str31 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str31, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark35);
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + (-1) + "'", int37 == (-1));
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + (-1) + "'", int38 == (-1));
        org.junit.Assert.assertNotNull(logMark39);
// flaky "5) test1510(Regression4Test)":         org.junit.Assert.assertTrue("'" + long40 + "' != '" + 97L + "'", long40 == 97L);
// flaky "5) test1510(Regression4Test)":         org.junit.Assert.assertEquals("'" + str43 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str43, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int50 + "' != '" + 1 + "'", int50 == 1);
        org.junit.Assert.assertTrue("'" + int51 + "' != '" + (-1) + "'", int51 == (-1));
        org.junit.Assert.assertTrue("'" + long55 + "' != '" + 35L + "'", long55 == 35L);
        org.junit.Assert.assertNotNull(logMark56);
        org.junit.Assert.assertTrue("'" + int57 + "' != '" + (-1) + "'", int57 == (-1));
        org.junit.Assert.assertTrue("'" + int58 + "' != '" + (-1) + "'", int58 == (-1));
    }

    @Test
    public void test1511() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1511");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, 35L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
    }

    @Test
    public void test1512() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1512");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) '4');
    }

    @Test
    public void test1513() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1513");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        logMark8.setLogMark(10L, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark14 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        java.lang.String str16 = logMark15.toString();
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark20 = new org.apache.bookkeeper.bookie.LogMark(logMark19);
        java.lang.String str21 = logMark20.toString();
        logMark20.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark25 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark26 = new org.apache.bookkeeper.bookie.LogMark(logMark25);
        int int27 = logMark20.compare(logMark25);
        int int28 = logMark15.compare(logMark25);
        logMark15.setLogMark(97L, (long) '4');
        logMark15.setLogMark(32L, 0L);
        java.lang.String str35 = logMark15.toString();
        long long36 = logMark15.getLogFileOffset();
        int int37 = logMark8.compare(logMark15);
        java.nio.ByteBuffer byteBuffer38 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.readLogMark(byteBuffer38);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
// flaky "6) test1513(Regression4Test)":         org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str10, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark14);
// flaky "6) test1513(Regression4Test)":         org.junit.Assert.assertEquals("'" + str16 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str16, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str21 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str21, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark25);
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + int28 + "' != '" + 0 + "'", int28 == 0);
        org.junit.Assert.assertEquals("'" + str35 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 0" + "'", str35, "LogMark: logFileId - 32 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long36 + "' != '" + 0L + "'", long36 == 0L);
        org.junit.Assert.assertTrue("'" + int37 + "' != '" + (-1) + "'", int37 == (-1));
    }

    @Test
    public void test1514() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1514");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), 10L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        long long7 = logMark5.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int9 = logMark2.compare(logMark5);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 0L + "'", long7 == 0L);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
    }

    @Test
    public void test1515() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1515");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
    }

    @Test
    public void test1516() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1516");
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
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark27);
        java.lang.String str29 = logMark28.toString();
        logMark28.setLogMark((long) (-1), (long) (byte) 100);
        logMark28.setLogMark((long) 10, 1L);
        org.apache.bookkeeper.bookie.LogMark logMark36 = new org.apache.bookkeeper.bookie.LogMark(logMark28);
        logMark36.setLogMark((long) (short) -1, (long) 'a');
        org.apache.bookkeeper.bookie.LogMark logMark42 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark(logMark42);
        int int44 = logMark36.compare(logMark42);
        org.apache.bookkeeper.bookie.LogMark logMark45 = new org.apache.bookkeeper.bookie.LogMark(logMark42);
        int int46 = logMark24.compare(logMark45);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + (-1) + "'", int7 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertNotNull(logMark15);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + (-1) + "'", int17 == (-1));
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 35L + "'", long21 == 35L);
        org.junit.Assert.assertEquals("'" + str22 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str22, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + 0 + "'", int23 == 0);
        org.junit.Assert.assertEquals("'" + str29 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str29, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int44 + "' != '" + (-1) + "'", int44 == (-1));
        org.junit.Assert.assertTrue("'" + int46 + "' != '" + (-1) + "'", int46 == (-1));
    }

    @Test
    public void test1517() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1517");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(32L, (long) (short) 10);
        long long3 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
    }

    @Test
    public void test1518() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1518");
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
        java.lang.String str34 = logMark3.toString();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + (-1) + "'", int18 == (-1));
        org.junit.Assert.assertEquals("'" + str22 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 100" + "'", str22, "LogMark: logFileId - 100 , logFileOffset - 100");
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + (-1) + "'", int31 == (-1));
        org.junit.Assert.assertTrue("'" + long32 + "' != '" + 0L + "'", long32 == 0L);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 1 + "'", int33 == 1);
        org.junit.Assert.assertEquals("'" + str34 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 100" + "'", str34, "LogMark: logFileId - 100 , logFileOffset - 100");
    }

    @Test
    public void test1519() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1519");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
    }

    @Test
    public void test1520() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1520");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        long long3 = logMark2.getLogFileOffset();
        long long4 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long8 = logMark7.getLogFileId();
        int int9 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        java.lang.String str14 = logMark13.toString();
        logMark13.setLogMark((long) (-1), (long) (byte) 100);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark(logMark13);
        int int19 = logMark7.compare(logMark13);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 1L + "'", long4 == 1L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 100L + "'", long8 == 100L);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str14, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 1 + "'", int19 == 1);
    }

    @Test
    public void test1521() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1521");
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
        org.apache.bookkeeper.bookie.LogMark logMark27 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark30 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int34 = logMark30.compare(logMark33);
        logMark30.setLogMark((long) (short) 10, 10L);
        long long38 = logMark30.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark41 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int45 = logMark41.compare(logMark44);
        logMark41.setLogMark((long) (short) 10, 10L);
        long long49 = logMark41.getLogFileId();
        int int50 = logMark30.compare(logMark41);
        java.lang.String str51 = logMark41.toString();
        int int52 = logMark27.compare(logMark41);
        org.apache.bookkeeper.bookie.LogMark logMark53 = new org.apache.bookkeeper.bookie.LogMark(logMark27);
        long long54 = logMark27.getLogFileOffset();
        logMark27.setLogMark((-1L), (long) (short) 0);
        int int58 = logMark2.compare(logMark27);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertNotNull(logMark13);
// flaky "7) test1521(Regression4Test)":         org.junit.Assert.assertTrue("'" + long14 + "' != '" + 97L + "'", long14 == 97L);
// flaky "7) test1521(Regression4Test)":         org.junit.Assert.assertEquals("'" + str16 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str16, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 32L + "'", long20 == 32L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + (-1) + "'", int24 == (-1));
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
        org.junit.Assert.assertTrue("'" + long38 + "' != '" + 10L + "'", long38 == 10L);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + 0 + "'", int45 == 0);
        org.junit.Assert.assertTrue("'" + long49 + "' != '" + 10L + "'", long49 == 10L);
        org.junit.Assert.assertTrue("'" + int50 + "' != '" + 0 + "'", int50 == 0);
        org.junit.Assert.assertEquals("'" + str51 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str51, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + (-1) + "'", int52 == (-1));
        org.junit.Assert.assertTrue("'" + long54 + "' != '" + 0L + "'", long54 == 0L);
        org.junit.Assert.assertTrue("'" + int58 + "' != '" + 1 + "'", int58 == 1);
    }

    @Test
    public void test1522() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1522");
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
        long long19 = logMark8.getLogFileOffset();
        java.lang.String str20 = logMark8.toString();
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark9);
// flaky "8) test1522(Regression4Test)":         org.junit.Assert.assertTrue("'" + long10 + "' != '" + 97L + "'", long10 == 97L);
// flaky "8) test1522(Regression4Test)":         org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str12, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 32L + "'", long16 == 32L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 0L + "'", long19 == 0L);
        org.junit.Assert.assertEquals("'" + str20 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str20, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test1523() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1523");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 0, (long) 'a');
        long long3 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 97L + "'", long3 == 97L);
    }

    @Test
    public void test1524() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1524");
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
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) 0, (long) 10);
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        java.lang.String str23 = logMark22.toString();
        logMark22.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark27 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark27);
        int int29 = logMark22.compare(logMark27);
        org.apache.bookkeeper.bookie.LogMark logMark30 = new org.apache.bookkeeper.bookie.LogMark(logMark22);
        int int31 = logMark18.compare(logMark22);
        int int32 = logMark11.compare(logMark18);
        org.junit.Assert.assertNotNull(logMark0);
// flaky "9) test1524(Regression4Test)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark27);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 1 + "'", int31 == 1);
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 1 + "'", int32 == 1);
    }

    @Test
    public void test1525() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1525");
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
            logMark6.readLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
    }

    @Test
    public void test1526() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1526");
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
        long long27 = logMark17.getLogFileOffset();
        long long28 = logMark17.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark31 = new org.apache.bookkeeper.bookie.LogMark(10L, 1L);
        int int32 = logMark17.compare(logMark31);
        int int33 = logMark2.compare(logMark17);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 10L + "'", long14 == 10L);
        org.junit.Assert.assertTrue("'" + int21 + "' != '" + 0 + "'", int21 == 0);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 10L + "'", long25 == 10L);
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + 0 + "'", int26 == 0);
        org.junit.Assert.assertTrue("'" + long27 + "' != '" + 10L + "'", long27 == 10L);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 1 + "'", int32 == 1);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 1 + "'", int33 == 1);
    }

    @Test
    public void test1527() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression4Test.test1527");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), (long) (byte) 0);
    }
}
