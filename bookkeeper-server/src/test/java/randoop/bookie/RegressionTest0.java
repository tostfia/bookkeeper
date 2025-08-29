package randoop.bookie;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RegressionTest0 {

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
    public void test0001() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0001");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark4 = null;
        // The following exception was thrown during execution in test generation
        try {
            int int5 = logMark2.compare(logMark4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0002() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0002");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.nio.ByteBuffer byteBuffer2 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark1.readLogMark(byteBuffer2);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test0003() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0003");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        java.lang.Class<?> wildcardClass2 = logMark0.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 9223372036854775807L + "'", long1 == 9223372036854775807L);
        org.junit.Assert.assertNotNull(wildcardClass2);
    }

    @Test
    public void test0004() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0004");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark1.writeLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test0005() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0005");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
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
    public void test0006() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0006");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
    }

    @Test
    public void test0007() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0007");
        org.apache.bookkeeper.bookie.LogMark logMark0 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0008() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0008");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
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
    public void test0009() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0009");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.nio.ByteBuffer byteBuffer2 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.readLogMark(byteBuffer2);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test0010() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0010");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0011() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0011");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0012() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0012");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.Class<?> wildcardClass10 = logMark8.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass10);
    }

    @Test
    public void test0013() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0013");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.writeLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
    }

    @Test
    public void test0014() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0014");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        java.lang.Class<?> wildcardClass9 = logMark8.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0015() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0015");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        java.lang.Class<?> wildcardClass5 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0016() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0016");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0017() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0017");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
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
    public void test0018() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0018");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int10 = logMark3.compare(logMark8);
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.writeLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
    }

    @Test
    public void test0019() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0019");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
    }

    @Test
    public void test0020() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0020");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass4 = logMark3.getClass();
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test0021() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0021");
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
        java.nio.ByteBuffer byteBuffer24 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark13.writeLogMark(byteBuffer24);
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
    }

    @Test
    public void test0022() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0022");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        logMark2.setLogMark((long) 0, (long) '#');
        java.nio.ByteBuffer byteBuffer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
    }

    @Test
    public void test0023() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0023");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.readLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
    }

    @Test
    public void test0024() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0024");
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
        java.nio.ByteBuffer byteBuffer15 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark11.readLogMark(byteBuffer15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 9223372036854775807 , logFileOffset - 9223372036854775807" + "'", str2, "LogMark: logFileId - 9223372036854775807 , logFileOffset - 9223372036854775807");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
    }

    @Test
    public void test0025() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0025");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
    }

    @Test
    public void test0026() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0026");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark2.setLogMark((long) (-1), 0L);
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
    }

    @Test
    public void test0027() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0027");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0028() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0028");
        org.apache.bookkeeper.bookie.LogMark logMark0 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.nio.ByteBuffer byteBuffer2 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark1.writeLogMark(byteBuffer2);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0029() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0029");
        org.apache.bookkeeper.bookie.LogMark logMark0 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.Class<?> wildcardClass2 = logMark1.getClass();
        org.junit.Assert.assertNotNull(wildcardClass2);
    }

    @Test
    public void test0030() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0030");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        long long3 = logMark2.getLogFileOffset();
        java.lang.Class<?> wildcardClass4 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test0031() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0031");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
    }

    @Test
    public void test0032() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0032");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int10 = logMark3.compare(logMark8);
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
    }

    @Test
    public void test0033() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0033");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 35");
    }

    @Test
    public void test0034() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0034");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long4 = logMark3.getLogFileOffset();
        long long5 = logMark3.getLogFileOffset();
        int int6 = logMark2.compare(logMark3);
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 9223372036854775807L + "'", long4 == 9223372036854775807L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 9223372036854775807L + "'", long5 == 9223372036854775807L);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + (-1) + "'", int6 == (-1));
    }

    @Test
    public void test0035() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0035");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
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
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
    }

    @Test
    public void test0036() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0036");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        long long2 = logMark0.getLogFileOffset();
        java.lang.Class<?> wildcardClass3 = logMark0.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 9223372036854775807L + "'", long1 == 9223372036854775807L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 9223372036854775807L + "'", long2 == 9223372036854775807L);
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0037() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0037");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        java.lang.Class<?> wildcardClass8 = logMark3.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(wildcardClass8);
    }

    @Test
    public void test0038() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0038");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
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
    public void test0039() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0039");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark6);
        java.lang.Class<?> wildcardClass8 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertNotNull(wildcardClass8);
    }

    @Test
    public void test0040() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0040");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
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
    public void test0041() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0041");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
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
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 9223372036854775807L + "'", long1 == 9223372036854775807L);
    }

    @Test
    public void test0042() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0042");
        org.apache.bookkeeper.bookie.LogMark logMark0 = new org.apache.bookkeeper.bookie.LogMark();
        java.lang.Class<?> wildcardClass1 = logMark0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass1);
    }

    @Test
    public void test0043() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0043");
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
        java.nio.ByteBuffer byteBuffer18 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark1.readLogMark(byteBuffer18);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertNotNull(logMark2);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 9223372036854775807 , logFileOffset - 9223372036854775807" + "'", str4, "LogMark: logFileId - 9223372036854775807 , logFileOffset - 9223372036854775807");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark13);
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + (-1) + "'", int15 == (-1));
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 0 + "'", int16 == 0);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
    }

    @Test
    public void test0044() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0044");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long3 = logMark2.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
    }

    @Test
    public void test0045() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0045");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
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
    public void test0046() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0046");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
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
    public void test0047() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0047");
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
        java.nio.ByteBuffer byteBuffer25 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark13.readLogMark(byteBuffer25);
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
    }

    @Test
    public void test0048() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0048");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
    }

    @Test
    public void test0049() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0049");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 97L + "'", long4 == 97L);
    }

    @Test
    public void test0050() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0050");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "1) test0050(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
    }

    @Test
    public void test0051() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0051");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test0052() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0052");
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
        java.nio.ByteBuffer byteBuffer18 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark9.writeLogMark(byteBuffer18);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertNotNull(logMark14);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
    }

    @Test
    public void test0053() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0053");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0054() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0054");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 0);
        logMark2.setLogMark((long) (short) 100, 10L);
    }

    @Test
    public void test0055() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0055");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 10, 0L);
    }

    @Test
    public void test0056() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0056");
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
        java.nio.ByteBuffer byteBuffer28 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer28);
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
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
    }

    @Test
    public void test0057() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0057");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, (long) (-1));
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0058() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0058");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.writeLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "2) test0058(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
    }

    @Test
    public void test0059() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0059");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        java.lang.String str2 = logMark0.toString();
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.writeLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "3) test0059(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
// flaky "1) test0059(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
    }

    @Test
    public void test0060() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0060");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass8 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(wildcardClass8);
    }

    @Test
    public void test0061() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0061");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
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
    public void test0062() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0062");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long3 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
    }

    @Test
    public void test0063() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0063");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        java.lang.String str3 = logMark2.toString();
        java.lang.String str4 = logMark2.toString();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str3, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str4, "LogMark: logFileId - 1 , logFileOffset - 35");
    }

    @Test
    public void test0064() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0064");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileId();
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.writeLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "4) test0064(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long2 + "' != '" + 100L + "'", long2 == 100L);
    }

    @Test
    public void test0065() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0065");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.writeLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
    }

    @Test
    public void test0066() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0066");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
    }

    @Test
    public void test0067() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0067");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long4 = logMark0.getLogFileId();
        java.lang.Class<?> wildcardClass5 = logMark0.getClass();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "5) test0067(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
// flaky "2) test0067(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long4 + "' != '" + 100L + "'", long4 == 100L);
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0068() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0068");
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
        java.lang.Class<?> wildcardClass24 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertNotNull(wildcardClass24);
    }

    @Test
    public void test0069() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0069");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0070() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0070");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 1" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 1");
    }

    @Test
    public void test0071() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0071");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0072() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0072");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        java.lang.String str3 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark4.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str3, "LogMark: logFileId - 1 , logFileOffset - 35");
    }

    @Test
    public void test0073() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0073");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.writeLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "6) test0073(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long2 + "' != '" + 35L + "'", long2 == 35L);
    }

    @Test
    public void test0074() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0074");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        long long3 = logMark2.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
    }

    @Test
    public void test0075() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0075");
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
            logMark13.readLogMark(byteBuffer36);
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
    }

    @Test
    public void test0076() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0076");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark1.compare(logMark6);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark6.writeLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "7) test0076(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 35" + "'", str2, "LogMark: logFileId - 10 , logFileOffset - 35");
// flaky "3) test0076(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int7 + "' != '" + (-1) + "'", int7 == (-1));
    }

    @Test
    public void test0077() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0077");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        long long11 = logMark9.getLogFileId();
        int int12 = logMark3.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long14 = logMark3.getLogFileId();
        java.lang.Class<?> wildcardClass15 = logMark3.getClass();
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 0L + "'", long14 == 0L);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0078() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0078");
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
        java.nio.ByteBuffer byteBuffer23 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark13.readLogMark(byteBuffer23);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
    }

    @Test
    public void test0079() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0079");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark7.setLogMark((long) 10, (long) '4');
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark7.readLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
    }

    @Test
    public void test0080() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0080");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        long long9 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 0L + "'", long9 == 0L);
    }

    @Test
    public void test0081() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0081");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(35L, (long) 1);
        logMark2.setLogMark((long) (-1), (long) ' ');
    }

    @Test
    public void test0082() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0082");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 9223372036854775807L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
    }

    @Test
    public void test0083() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0083");
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
        logMark13.setLogMark(0L, (long) '#');
        java.lang.Class<?> wildcardClass28 = logMark13.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str23, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str24, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(wildcardClass28);
    }

    @Test
    public void test0084() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0084");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark5.setLogMark(1L, (long) (byte) 1);
        java.lang.Class<?> wildcardClass10 = logMark5.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(wildcardClass10);
    }

    @Test
    public void test0085() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0085");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        long long3 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
    }

    @Test
    public void test0086() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0086");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        long long3 = logMark2.getLogFileId();
        java.lang.String str4 = logMark2.toString();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 1" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 1");
    }

    @Test
    public void test0087() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0087");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
    }

    @Test
    public void test0088() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0088");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
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
    public void test0089() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0089");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark7.compare(logMark11);
        java.nio.ByteBuffer byteBuffer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark7.writeLogMark(byteBuffer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
    }

    @Test
    public void test0090() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0090");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark5.setLogMark(1L, (long) (byte) 1);
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
    }

    @Test
    public void test0091() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0091");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 100, (long) '#');
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0092() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0092");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        long long3 = logMark1.getLogFileId();
        java.lang.Class<?> wildcardClass4 = logMark1.getClass();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "8) test0092(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str2, "LogMark: logFileId - 32 , logFileOffset - 32");
// flaky "4) test0092(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test0093() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0093");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        long long3 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
    }

    @Test
    public void test0094() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0094");
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
        java.lang.Class<?> wildcardClass19 = logMark16.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + (-1) + "'", int18 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0095() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0095");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 100, 35L);
    }

    @Test
    public void test0096() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0096");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.Class<?> wildcardClass10 = logMark3.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertNotNull(wildcardClass10);
    }

    @Test
    public void test0097() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0097");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
    }

    @Test
    public void test0098() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0098");
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
        java.nio.ByteBuffer byteBuffer23 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer23);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
    }

    @Test
    public void test0099() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0099");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(10L, 9223372036854775807L);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0100() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0100");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.writeLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
    }

    @Test
    public void test0101() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0101");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        logMark2.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long7 = logMark6.getLogFileOffset();
        java.lang.Class<?> wildcardClass8 = logMark6.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 1L + "'", long7 == 1L);
        org.junit.Assert.assertNotNull(wildcardClass8);
    }

    @Test
    public void test0102() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0102");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
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
    public void test0103() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0103");
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
        java.lang.Class<?> wildcardClass11 = logMark8.getClass();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "9) test0103(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 32L + "'", long1 == 32L);
// flaky "5) test0103(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str2, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertNotNull(logMark3);
// flaky "1) test0103(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long6 + "' != '" + 32L + "'", long6 == 32L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
        org.junit.Assert.assertNotNull(logMark8);
// flaky "1) test0103(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long9 + "' != '" + 32L + "'", long9 == 32L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0104() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0104");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, (-1L));
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0105() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0105");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark((long) 100, (long) 'a');
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long7 = logMark6.getLogFileOffset();
        int int8 = logMark0.compare(logMark6);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.writeLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "10) test0105(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long2 + "' != '" + 32L + "'", long2 == 32L);
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
    }

    @Test
    public void test0106() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0106");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark2.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0107() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0107");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark5.setLogMark(1L, (long) (byte) 1);
        long long10 = logMark5.getLogFileId();
        java.lang.Class<?> wildcardClass11 = logMark5.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 1L + "'", long10 == 1L);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0108() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0108");
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
        java.lang.Class<?> wildcardClass38 = logMark2.getClass();
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
        org.junit.Assert.assertNotNull(wildcardClass38);
    }

    @Test
    public void test0109() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0109");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long5 = logMark4.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        int int8 = logMark3.compare(logMark7);
        java.lang.Class<?> wildcardClass9 = logMark7.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 97L + "'", long5 == 97L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0110() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0110");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(10L, (long) (short) -1);
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
    public void test0111() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0111");
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
        java.nio.ByteBuffer byteBuffer25 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark1.readLogMark(byteBuffer25);
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
        org.junit.Assert.assertNotNull(logMark19);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 1 + "'", int24 == 1);
    }

    @Test
    public void test0112() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0112");
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
        java.nio.ByteBuffer byteBuffer38 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark26.readLogMark(byteBuffer38);
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
    }

    @Test
    public void test0113() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0113");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark10.writeLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
    }

    @Test
    public void test0114() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0114");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, 100L);
    }

    @Test
    public void test0115() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0115");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark7.setLogMark((long) (byte) 100, (long) (short) 0);
        int int11 = logMark2.compare(logMark7);
        java.nio.ByteBuffer byteBuffer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark7.readLogMark(byteBuffer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + (-1) + "'", int11 == (-1));
    }

    @Test
    public void test0116() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0116");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        java.lang.String str5 = logMark2.toString();
        java.lang.Class<?> wildcardClass6 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str5, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(wildcardClass6);
    }

    @Test
    public void test0117() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0117");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark5.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark();
        int int11 = logMark5.compare(logMark10);
        java.nio.ByteBuffer byteBuffer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + 1 + "'", int11 == 1);
    }

    @Test
    public void test0118() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0118");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        long long11 = logMark9.getLogFileId();
        int int12 = logMark3.compare(logMark9);
        java.lang.Class<?> wildcardClass13 = logMark9.getClass();
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0119() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0119");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        java.lang.String str2 = logMark0.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        long long6 = logMark5.getLogFileId();
        int int7 = logMark0.compare(logMark5);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.writeLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
    }

    @Test
    public void test0120() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0120");
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
        java.nio.ByteBuffer byteBuffer28 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark16.writeLogMark(byteBuffer28);
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
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
    }

    @Test
    public void test0121() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0121");
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
        java.nio.ByteBuffer byteBuffer20 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark19.writeLogMark(byteBuffer20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertNotNull(logMark14);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
    }

    @Test
    public void test0122() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0122");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark2.setLogMark((long) (byte) 1, (long) '#');
        logMark2.setLogMark((long) 10, (long) ' ');
        java.lang.String str9 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 32" + "'", str9, "LogMark: logFileId - 10 , logFileOffset - 32");
    }

    @Test
    public void test0123() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0123");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, (long) 'a');
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
    public void test0124() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0124");
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
        java.lang.Class<?> wildcardClass39 = logMark23.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 0 + "'", int16 == 0);
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 10L + "'", long20 == 10L);
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 0 + "'", int27 == 0);
        org.junit.Assert.assertTrue("'" + long31 + "' != '" + 10L + "'", long31 == 10L);
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + 0 + "'", int32 == 0);
        org.junit.Assert.assertEquals("'" + str33 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str33, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str34 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str34, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + 1 + "'", int38 == 1);
        org.junit.Assert.assertNotNull(wildcardClass39);
    }

    @Test
    public void test0125() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0125");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        logMark1.setLogMark((long) (short) -1, 0L);
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark1.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test0126() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0126");
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
            logMark32.readLogMark(byteBuffer36);
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
    }

    @Test
    public void test0127() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0127");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark7.setLogMark((long) 10, (long) '4');
        java.lang.Class<?> wildcardClass11 = logMark7.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0128() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0128");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, (long) 'a');
    }

    @Test
    public void test0129() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0129");
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
        java.nio.ByteBuffer byteBuffer33 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark16.readLogMark(byteBuffer33);
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
    public void test0130() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0130");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.Class<?> wildcardClass5 = logMark4.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0131() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0131");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        logMark2.setLogMark((-1L), (-1L));
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0132() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0132");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), (long) 10);
        int int7 = logMark0.compare(logMark6);
        java.lang.Class<?> wildcardClass8 = logMark6.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertNotNull(wildcardClass8);
    }

    @Test
    public void test0133() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0133");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        java.lang.String str3 = logMark2.toString();
        java.lang.String str4 = logMark2.toString();
        long long5 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str3, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str4, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 1L + "'", long5 == 1L);
    }

    @Test
    public void test0134() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0134");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 100, 35L);
        java.lang.String str3 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 35" + "'", str3, "LogMark: logFileId - 100 , logFileOffset - 35");
    }

    @Test
    public void test0135() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0135");
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
        java.nio.ByteBuffer byteBuffer33 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark16.writeLogMark(byteBuffer33);
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
    public void test0136() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0136");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer2 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.writeLogMark(byteBuffer2);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
    }

    @Test
    public void test0137() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0137");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark(100L, (long) '4');
        long long7 = logMark3.getLogFileOffset();
        java.lang.Class<?> wildcardClass8 = logMark3.getClass();
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 52L + "'", long7 == 52L);
        org.junit.Assert.assertNotNull(wildcardClass8);
    }

    @Test
    public void test0138() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0138");
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
        java.lang.Class<?> wildcardClass14 = logMark2.getClass();
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 97L + "'", long9 == 97L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str11, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass14);
    }

    @Test
    public void test0139() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0139");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 1L + "'", long4 == 1L);
    }

    @Test
    public void test0140() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0140");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.writeLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test0141() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0141");
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
        java.nio.ByteBuffer byteBuffer28 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark16.readLogMark(byteBuffer28);
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
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
    }

    @Test
    public void test0142() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0142");
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
        java.nio.ByteBuffer byteBuffer15 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark1.writeLogMark(byteBuffer15);
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
    public void test0143() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0143");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, 100L);
        java.lang.String str3 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 100" + "'", str3, "LogMark: logFileId - 100 , logFileOffset - 100");
    }

    @Test
    public void test0144() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0144");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        long long5 = logMark2.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 0L + "'", long5 == 0L);
    }

    @Test
    public void test0145() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0145");
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
        java.lang.Class<?> wildcardClass20 = logMark10.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 0L + "'", long17 == 0L);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 97L + "'", long19 == 97L);
        org.junit.Assert.assertNotNull(wildcardClass20);
    }

    @Test
    public void test0146() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0146");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark(100L, (long) '4');
        long long7 = logMark3.getLogFileOffset();
        long long8 = logMark3.getLogFileId();
        java.lang.Class<?> wildcardClass9 = logMark3.getClass();
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 52L + "'", long7 == 52L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 100L + "'", long8 == 100L);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0147() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0147");
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
        java.nio.ByteBuffer byteBuffer18 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer18);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertNotNull(logMark14);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
    }

    @Test
    public void test0148() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0148");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        long long5 = logMark2.getLogFileOffset();
        java.lang.Class<?> wildcardClass6 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 0L + "'", long5 == 0L);
        org.junit.Assert.assertNotNull(wildcardClass6);
    }

    @Test
    public void test0149() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0149");
        org.apache.bookkeeper.bookie.LogMark logMark0 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 0L + "'", long2 == 0L);
    }

    @Test
    public void test0150() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0150");
        org.apache.bookkeeper.bookie.LogMark logMark0 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.nio.ByteBuffer byteBuffer2 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.writeLogMark(byteBuffer2);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0151() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0151");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileOffset();
        java.lang.Class<?> wildcardClass5 = logMark3.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 35L + "'", long4 == 35L);
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0152() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0152");
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
        java.lang.String str23 = logMark2.toString();
        java.nio.ByteBuffer byteBuffer24 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer24);
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
    }

    @Test
    public void test0153() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0153");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        logMark2.setLogMark((long) (-1), 9223372036854775807L);
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
    public void test0154() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0154");
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
        long long18 = logMark5.getLogFileId();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertNotNull(logMark14);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 97L + "'", long18 == 97L);
    }

    @Test
    public void test0155() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0155");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, (long) (short) 0);
        long long3 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
    }

    @Test
    public void test0156() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0156");
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
        java.lang.String str39 = logMark26.toString();
        logMark26.setLogMark(97L, 0L);
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
        org.junit.Assert.assertEquals("'" + str39 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str39, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test0157() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0157");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 10, (long) (short) -1);
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
    public void test0158() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0158");
        org.apache.bookkeeper.bookie.LogMark logMark0 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.Class<?> wildcardClass2 = logMark0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass2);
    }

    @Test
    public void test0159() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0159");
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
        java.lang.Class<?> wildcardClass36 = logMark16.getClass();
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 10L + "'", long29 == 10L);
        org.junit.Assert.assertNotNull(wildcardClass36);
    }

    @Test
    public void test0160() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0160");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark5.setLogMark((long) (short) 100, 35L);
        long long9 = logMark5.getLogFileId();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 100L + "'", long9 == 100L);
    }

    @Test
    public void test0161() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0161");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        logMark1.setLogMark((long) (short) -1, 0L);
        long long5 = logMark1.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        long long10 = logMark8.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        int int13 = logMark8.compare(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark19 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int20 = logMark16.compare(logMark19);
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark(logMark16);
        int int22 = logMark12.compare(logMark16);
        int int23 = logMark1.compare(logMark16);
        java.nio.ByteBuffer byteBuffer24 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark16.readLogMark(byteBuffer24);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + (-1L) + "'", long5 == (-1L));
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 0L + "'", long10 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + (-1) + "'", int22 == (-1));
        org.junit.Assert.assertTrue("'" + int23 + "' != '" + (-1) + "'", int23 == (-1));
    }

    @Test
    public void test0162() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0162");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 100);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0163() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0163");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        java.lang.String str2 = logMark0.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        long long6 = logMark5.getLogFileId();
        int int7 = logMark0.compare(logMark5);
        long long8 = logMark5.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.writeLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 97L + "'", long8 == 97L);
    }

    @Test
    public void test0164() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0164");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        logMark2.setLogMark(32L, 9223372036854775807L);
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str3, "LogMark: logFileId - 100 , logFileOffset - 97");
    }

    @Test
    public void test0165() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0165");
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
        java.nio.ByteBuffer byteBuffer29 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark16.readLogMark(byteBuffer29);
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
    }

    @Test
    public void test0166() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0166");
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
        java.lang.Class<?> wildcardClass48 = logMark13.getClass();
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
        org.junit.Assert.assertNotNull(wildcardClass48);
    }

    @Test
    public void test0167() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0167");
        org.apache.bookkeeper.bookie.LogMark logMark0 = new org.apache.bookkeeper.bookie.LogMark();
        java.lang.String str1 = logMark0.toString();
        java.lang.String str2 = logMark0.toString();
        logMark0.setLogMark((long) (-1), (long) 1);
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str1, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str2, "LogMark: logFileId - 0 , logFileOffset - 0");
    }

    @Test
    public void test0168() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0168");
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
        java.lang.Class<?> wildcardClass19 = logMark10.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 0L + "'", long17 == 0L);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 97L + "'", long18 == 97L);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0169() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0169");
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
        java.lang.Class<?> wildcardClass35 = logMark13.getClass();
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
        org.junit.Assert.assertNotNull(wildcardClass35);
    }

    @Test
    public void test0170() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0170");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        java.lang.String str12 = logMark11.toString();
        java.nio.ByteBuffer byteBuffer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark11.writeLogMark(byteBuffer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str10, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str12, "LogMark: logFileId - 100 , logFileOffset - 97");
    }

    @Test
    public void test0171() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0171");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        logMark3.setLogMark((long) '4', (long) '4');
        logMark3.setLogMark(35L, (long) 10);
        java.lang.Class<?> wildcardClass15 = logMark3.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0172() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0172");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        long long3 = logMark2.getLogFileOffset();
        long long4 = logMark2.getLogFileId();
        logMark2.setLogMark((long) 10, (long) (short) -1);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 1L + "'", long4 == 1L);
    }

    @Test
    public void test0173() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0173");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 10, (long) (short) -1);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0174() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0174");
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
        java.nio.ByteBuffer byteBuffer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark10.writeLogMark(byteBuffer19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 0L + "'", long17 == 0L);
    }

    @Test
    public void test0175() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0175");
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
        java.lang.Class<?> wildcardClass16 = logMark11.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str15, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(wildcardClass16);
    }

    @Test
    public void test0176() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0176");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(35L, (long) 1);
        java.lang.String str3 = logMark2.toString();
        java.lang.Class<?> wildcardClass4 = logMark2.getClass();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 1" + "'", str3, "LogMark: logFileId - 35 , logFileOffset - 1");
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test0177() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0177");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        java.nio.ByteBuffer byteBuffer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark11.writeLogMark(byteBuffer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str10, "LogMark: logFileId - 100 , logFileOffset - 97");
    }

    @Test
    public void test0178() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0178");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test0179() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0179");
        org.apache.bookkeeper.bookie.LogMark logMark0 = new org.apache.bookkeeper.bookie.LogMark();
        java.lang.String str1 = logMark0.toString();
        long long2 = logMark0.getLogFileId();
        long long3 = logMark0.getLogFileOffset();
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str1, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 0L + "'", long2 == 0L);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
    }

    @Test
    public void test0180() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0180");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        logMark2.setLogMark((long) 0, (long) '#');
        java.lang.String str13 = logMark2.toString();
        java.nio.ByteBuffer byteBuffer14 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str13, "LogMark: logFileId - 0 , logFileOffset - 35");
    }

    @Test
    public void test0181() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0181");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0182() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0182");
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
        java.lang.String str23 = logMark10.toString();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 97L + "'", long17 == 97L);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 1 + "'", int22 == 1);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0183() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0183");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(32L, 97L);
    }

    @Test
    public void test0184() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0184");
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
        java.nio.ByteBuffer byteBuffer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark1.writeLogMark(byteBuffer19);
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
    }

    @Test
    public void test0185() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0185");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        long long11 = logMark9.getLogFileId();
        int int12 = logMark3.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long14 = logMark3.getLogFileId();
        java.nio.ByteBuffer byteBuffer15 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer15);
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
    public void test0186() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0186");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long3 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
    }

    @Test
    public void test0187() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0187");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        logMark0.setLogMark((long) ' ', (long) ' ');
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.writeLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "11) test0187(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 100L + "'", long1 == 100L);
    }

    @Test
    public void test0188() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0188");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark(0L, (long) (byte) 0);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 32L + "'", long1 == 32L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str4, "LogMark: logFileId - 32 , logFileOffset - 32");
    }

    @Test
    public void test0189() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0189");
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
        java.nio.ByteBuffer byteBuffer21 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer21);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + (-1) + "'", int18 == (-1));
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + (-1L) + "'", long19 == (-1L));
    }

    @Test
    public void test0190() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0190");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
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
    public void test0191() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0191");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str4 = logMark3.toString();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "12) test0191(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
// flaky "6) test0191(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str4, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test0192() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0192");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long4 = logMark3.getLogFileOffset();
        long long5 = logMark3.getLogFileOffset();
        int int6 = logMark2.compare(logMark3);
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark3);
// flaky "13) test0192(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long4 + "' != '" + 10L + "'", long4 == 10L);
// flaky "7) test0192(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long5 + "' != '" + 10L + "'", long5 == 10L);
// flaky "2) test0192(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int6 + "' != '" + 1 + "'", int6 == 1);
    }

    @Test
    public void test0193() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0193");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark((long) 100, (long) 'a');
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long7 = logMark6.getLogFileOffset();
        int int8 = logMark0.compare(logMark6);
        java.lang.Class<?> wildcardClass9 = logMark0.getClass();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "14) test0193(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long2 + "' != '" + 10L + "'", long2 == 10L);
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0194() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0194");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        java.lang.Class<?> wildcardClass6 = logMark4.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(wildcardClass6);
    }

    @Test
    public void test0195() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0195");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark7.compare(logMark11);
        logMark11.setLogMark(97L, 97L);
        logMark11.setLogMark((long) (short) 100, (long) (byte) 100);
        java.nio.ByteBuffer byteBuffer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark11.readLogMark(byteBuffer19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
    }

    @Test
    public void test0196() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0196");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        long long4 = logMark2.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str3, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 97L + "'", long4 == 97L);
    }

    @Test
    public void test0197() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0197");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 100, (long) '#');
        logMark2.setLogMark((long) (byte) 10, (long) (byte) 100);
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
    public void test0198() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0198");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, (long) (short) 1);
    }

    @Test
    public void test0199() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0199");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark6);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
    }

    @Test
    public void test0200() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0200");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        logMark2.setLogMark((long) (-1), 9223372036854775807L);
        java.lang.String str6 = logMark2.toString();
        long long7 = logMark2.getLogFileId();
        logMark2.setLogMark((long) (short) -1, (long) (byte) 100);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807" + "'", str6, "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + (-1L) + "'", long7 == (-1L));
    }

    @Test
    public void test0201() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0201");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        long long11 = logMark9.getLogFileId();
        int int12 = logMark3.compare(logMark9);
        java.lang.String str13 = logMark9.toString();
        java.lang.Class<?> wildcardClass14 = logMark9.getClass();
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str13, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(wildcardClass14);
    }

    @Test
    public void test0202() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0202");
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
            logMark10.readLogMark(byteBuffer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 97L + "'", long9 == 97L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str11, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
    }

    @Test
    public void test0203() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0203");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        long long3 = logMark2.getLogFileOffset();
        long long4 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long8 = logMark7.getLogFileId();
        int int9 = logMark2.compare(logMark7);
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 1L + "'", long4 == 1L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 100L + "'", long8 == 100L);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
    }

    @Test
    public void test0204() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0204");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
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
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0205() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0205");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int10 = logMark3.compare(logMark8);
        long long11 = logMark8.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.writeLogMark(byteBuffer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
// flaky "15) test0205(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long11 + "' != '" + 1L + "'", long11 == 1L);
    }

    @Test
    public void test0206() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0206");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        long long4 = logMark2.getLogFileOffset();
        java.lang.Class<?> wildcardClass5 = logMark2.getClass();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "16) test0206(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 1L + "'", long1 == 1L);
// flaky "8) test0206(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 1" + "'", str3, "LogMark: logFileId - 10 , logFileOffset - 1");
// flaky "3) test0206(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long4 + "' != '" + 1L + "'", long4 == 1L);
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0207() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0207");
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
        java.lang.Class<?> wildcardClass28 = logMark16.getClass();
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertEquals("'" + str26 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str26, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
        org.junit.Assert.assertNotNull(wildcardClass28);
    }

    @Test
    public void test0208() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0208");
        org.apache.bookkeeper.bookie.LogMark logMark0 = new org.apache.bookkeeper.bookie.LogMark();
        java.lang.String str1 = logMark0.toString();
        java.lang.Class<?> wildcardClass2 = logMark0.getClass();
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str1, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(wildcardClass2);
    }

    @Test
    public void test0209() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0209");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long5 = logMark4.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        int int8 = logMark3.compare(logMark7);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark7.writeLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "17) test0209(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 1" + "'", str2, "LogMark: logFileId - 10 , logFileOffset - 1");
        org.junit.Assert.assertNotNull(logMark4);
// flaky "9) test0209(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long5 + "' != '" + 1L + "'", long5 == 1L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
    }

    @Test
    public void test0210() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0210");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.String str10 = logMark3.toString();
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 100" + "'", str10, "LogMark: logFileId - -1 , logFileOffset - 100");
    }

    @Test
    public void test0211() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0211");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark2.setLogMark((long) ' ', (long) (short) 100);
    }

    @Test
    public void test0212() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0212");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark8.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int13 = logMark2.compare(logMark12);
        long long14 = logMark12.getLogFileId();
        java.nio.ByteBuffer byteBuffer15 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark12.readLogMark(byteBuffer15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 1L + "'", long14 == 1L);
    }

    @Test
    public void test0213() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0213");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', 0L);
        long long3 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
    }

    @Test
    public void test0214() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0214");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0215() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0215");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(97L, 35L);
        long long7 = logMark6.getLogFileOffset();
        int int8 = logMark2.compare(logMark6);
        java.lang.Class<?> wildcardClass9 = logMark6.getClass();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 35L + "'", long7 == 35L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 1 + "'", int8 == 1);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0216() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0216");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, (-1L));
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
    public void test0217() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0217");
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
        java.nio.ByteBuffer byteBuffer21 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark20.readLogMark(byteBuffer21);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 52L + "'", long18 == 52L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
    }

    @Test
    public void test0218() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0218");
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
        java.nio.ByteBuffer byteBuffer48 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark13.readLogMark(byteBuffer48);
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
// flaky "18) test0218(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + 1 + "'", int35 == 1);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + (-1) + "'", int45 == (-1));
        org.junit.Assert.assertEquals("'" + str46 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str46, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long47 + "' != '" + 10L + "'", long47 == 10L);
    }

    @Test
    public void test0219() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0219");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long3 = logMark2.getLogFileId();
        logMark2.setLogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long8 = logMark7.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int11 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        long long13 = logMark12.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
        org.junit.Assert.assertNotNull(logMark7);
// flaky "19) test0219(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long8 + "' != '" + 1L + "'", long8 == 1L);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + (-1) + "'", int11 == (-1));
// flaky "10) test0219(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
    }

    @Test
    public void test0220() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0220");
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
        java.nio.ByteBuffer byteBuffer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark10.readLogMark(byteBuffer19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 0L + "'", long17 == 0L);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 97L + "'", long18 == 97L);
    }

    @Test
    public void test0221() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0221");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark(100L, (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        logMark7.setLogMark((long) (byte) 1, 0L);
    }

    @Test
    public void test0222() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0222");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
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
    public void test0223() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0223");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        java.lang.String str11 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark12.writeLogMark(byteBuffer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str11, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test0224() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0224");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, 35L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        int int5 = logMark2.compare(logMark4);
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark4.writeLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertNotNull(logMark4);
// flaky "20) test0224(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int5 + "' != '" + 1 + "'", int5 == 1);
    }

    @Test
    public void test0225() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0225");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        logMark3.setLogMark((long) '4', (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.nio.ByteBuffer byteBuffer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
    }

    @Test
    public void test0226() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0226");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
    }

    @Test
    public void test0227() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0227");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark7.compare(logMark11);
        logMark7.setLogMark((long) (short) 0, 32L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
    }

    @Test
    public void test0228() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0228");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        long long2 = logMark0.getLogFileOffset();
        long long3 = logMark0.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "21) test0228(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 1L + "'", long1 == 1L);
// flaky "11) test0228(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long2 + "' != '" + 1L + "'", long2 == 1L);
// flaky "4) test0228(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long3 + "' != '" + 1L + "'", long3 == 1L);
    }

    @Test
    public void test0229() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0229");
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
        java.nio.ByteBuffer byteBuffer47 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark13.writeLogMark(byteBuffer47);
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
// flaky "22) test0229(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + 1 + "'", int35 == 1);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + (-1) + "'", int45 == (-1));
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
    }

    @Test
    public void test0230() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0230");
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
        long long25 = logMark13.getLogFileId();
        java.lang.Class<?> wildcardClass26 = logMark13.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 10L + "'", long25 == 10L);
        org.junit.Assert.assertNotNull(wildcardClass26);
    }

    @Test
    public void test0231() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0231");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long8 = logMark2.getLogFileOffset();
        logMark2.setLogMark((long) (byte) 1, (long) (short) 10);
        java.nio.ByteBuffer byteBuffer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
    }

    @Test
    public void test0232() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0232");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 10" + "'", str4, "LogMark: logFileId - 1 , logFileOffset - 10");
    }

    @Test
    public void test0233() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0233");
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
        java.lang.Class<?> wildcardClass36 = logMark6.getClass();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "23) test0233(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 1" + "'", str2, "LogMark: logFileId - 10 , logFileOffset - 1");
// flaky "12) test0233(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int7 + "' != '" + (-1) + "'", int7 == (-1));
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str12, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertNotNull(logMark13);
// flaky "5) test0233(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str15 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 1" + "'", str15, "LogMark: logFileId - 10 , logFileOffset - 1");
        org.junit.Assert.assertEquals("'" + str20 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str20, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark24);
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + (-1) + "'", int26 == (-1));
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 0 + "'", int27 == 0);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
        org.junit.Assert.assertTrue("'" + int35 + "' != '" + 1 + "'", int35 == 1);
        org.junit.Assert.assertNotNull(wildcardClass36);
    }

    @Test
    public void test0234() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0234");
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
        java.nio.ByteBuffer byteBuffer40 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark27.readLogMark(byteBuffer40);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark8);
// flaky "24) test0234(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long9 + "' != '" + 1L + "'", long9 == 1L);
// flaky "13) test0234(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 1" + "'", str11, "LogMark: logFileId - 10 , logFileOffset - 1");
// flaky "6) test0234(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int12 + "' != '" + 1 + "'", int12 == 1);
// flaky "2) test0234(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + int36 + "' != '" + 0 + "'", int36 == 0);
        org.junit.Assert.assertEquals("'" + str37 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str37, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str38 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str38, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + 1 + "'", int39 == 1);
    }

    @Test
    public void test0235() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0235");
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
        long long28 = logMark13.getLogFileId();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str23, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str24, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + (-1L) + "'", long28 == (-1L));
    }

    @Test
    public void test0236() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0236");
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
            logMark15.readLogMark(byteBuffer16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "25) test0236(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str2, "LogMark: logFileId - 52 , logFileOffset - 100");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
    }

    @Test
    public void test0237() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0237");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) (short) 10);
        logMark2.setLogMark((long) (byte) 1, (long) 10);
        java.lang.Class<?> wildcardClass6 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass6);
    }

    @Test
    public void test0238() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0238");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(9223372036854775807L, 9223372036854775807L);
    }

    @Test
    public void test0239() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0239");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.Class<?> wildcardClass10 = logMark9.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertNotNull(wildcardClass10);
    }

    @Test
    public void test0240() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0240");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        logMark3.setLogMark((long) '4', (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long13 = logMark3.getLogFileId();
        java.nio.ByteBuffer byteBuffer14 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 52L + "'", long13 == 52L);
    }

    @Test
    public void test0241() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0241");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long8 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', 0L);
        int int12 = logMark2.compare(logMark11);
        java.nio.ByteBuffer byteBuffer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark11.writeLogMark(byteBuffer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
    }

    @Test
    public void test0242() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0242");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (byte) -1, (long) (-1));
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
    }

    @Test
    public void test0243() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0243");
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
        java.nio.ByteBuffer byteBuffer38 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer38);
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
    }

    @Test
    public void test0244() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0244");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        logMark2.setLogMark((long) 0, (long) '#');
        long long13 = logMark2.getLogFileOffset();
        java.lang.String str14 = logMark2.toString();
        java.lang.Class<?> wildcardClass15 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
// flaky "26) test0244(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 35L + "'", long13 == 35L);
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str14, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0245() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0245");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        java.lang.String str2 = logMark0.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        long long6 = logMark5.getLogFileId();
        int int7 = logMark0.compare(logMark5);
        long long8 = logMark5.getLogFileOffset();
        java.lang.String str9 = logMark5.toString();
        long long10 = logMark5.getLogFileId();
        logMark5.setLogMark((long) (short) 100, (long) (byte) 0);
        java.nio.ByteBuffer byteBuffer14 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.writeLogMark(byteBuffer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "27) test0245(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 52L + "'", long1 == 52L);
// flaky "14) test0245(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 52" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 52");
        org.junit.Assert.assertNotNull(logMark3);
// flaky "7) test0245(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long6 + "' != '" + (-1L) + "'", long6 == (-1L));
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
// flaky "3) test0245(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long8 + "' != '" + 52L + "'", long8 == 52L);
// flaky "1) test0245(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 52" + "'", str9, "LogMark: logFileId - -1 , logFileOffset - 52");
// flaky "1) test0245(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long10 + "' != '" + (-1L) + "'", long10 == (-1L));
    }

    @Test
    public void test0246() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0246");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark4.readLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0247() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0247");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0248() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0248");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, 100L);
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
    public void test0249() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0249");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark(100L, (long) '4');
        java.lang.Class<?> wildcardClass7 = logMark3.getClass();
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0250() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0250");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark((long) 100, (long) 'a');
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long7 = logMark6.getLogFileOffset();
        int int8 = logMark0.compare(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark9.writeLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "28) test0250(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long2 + "' != '" + 52L + "'", long2 == 52L);
        org.junit.Assert.assertNotNull(logMark6);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 97L + "'", long7 == 97L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
    }

    @Test
    public void test0251() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0251");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long8 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', 0L);
        int int12 = logMark2.compare(logMark11);
        java.lang.Class<?> wildcardClass13 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0252() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0252");
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
        logMark27.setLogMark((long) (byte) 0, (long) '#');
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
    public void test0253() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0253");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, 35L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        int int5 = logMark2.compare(logMark4);
        java.lang.Class<?> wildcardClass6 = logMark4.getClass();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertNotNull(logMark4);
// flaky "29) test0253(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int5 + "' != '" + 1 + "'", int5 == 1);
        org.junit.Assert.assertNotNull(wildcardClass6);
    }

    @Test
    public void test0254() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0254");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test0255() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0255");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        logMark2.setLogMark((long) (-1), 9223372036854775807L);
        java.lang.String str6 = logMark2.toString();
        long long7 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807" + "'", str6, "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + (-1L) + "'", long7 == (-1L));
    }

    @Test
    public void test0256() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0256");
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
            logMark2.writeLogMark(byteBuffer39);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
// flaky "30) test0256(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
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
    public void test0257() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0257");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark2.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark6.readLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0258() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0258");
        org.apache.bookkeeper.bookie.LogMark logMark0 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        logMark0.setLogMark((long) 10, (long) (short) 100);
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0259() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0259");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark(1L, (long) (short) 100);
        logMark0.setLogMark((long) 10, (long) (byte) 1);
        java.lang.Class<?> wildcardClass9 = logMark0.getClass();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "31) test0259(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long2 + "' != '" + 10L + "'", long2 == 10L);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0260() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0260");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark5.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark();
        int int11 = logMark5.compare(logMark10);
        org.apache.bookkeeper.bookie.LogMark logMark12 = null;
        // The following exception was thrown during execution in test generation
        try {
            int int13 = logMark5.compare(logMark12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + 1 + "'", int11 == 1);
    }

    @Test
    public void test0261() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0261");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        java.lang.String str8 = logMark5.toString();
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.writeLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str8, "LogMark: logFileId - -1 , logFileOffset - 0");
    }

    @Test
    public void test0262() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0262");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0263() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0263");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 0);
        long long3 = logMark2.getLogFileId();
        logMark2.setLogMark(0L, (long) (byte) 10);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
    }

    @Test
    public void test0264() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0264");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long8 = logMark2.getLogFileOffset();
        java.lang.Class<?> wildcardClass9 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0265() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0265");
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
        java.nio.ByteBuffer byteBuffer29 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark28.writeLogMark(byteBuffer29);
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
    }

    @Test
    public void test0266() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0266");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 0, (long) 100);
        long long3 = logMark2.getLogFileId();
        java.lang.Class<?> wildcardClass4 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test0267() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0267");
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
        long long19 = logMark18.getLogFileId();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertNotNull(logMark14);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 97L + "'", long19 == 97L);
    }

    @Test
    public void test0268() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0268");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.Class<?> wildcardClass2 = logMark0.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertNotNull(wildcardClass2);
    }

    @Test
    public void test0269() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0269");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        java.lang.String str4 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int14 = logMark10.compare(logMark13);
        logMark10.setLogMark((long) (short) 10, 10L);
        long long18 = logMark10.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark21 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int25 = logMark21.compare(logMark24);
        logMark21.setLogMark((long) (short) 10, 10L);
        long long29 = logMark21.getLogFileId();
        int int30 = logMark10.compare(logMark21);
        java.lang.String str31 = logMark21.toString();
        int int32 = logMark7.compare(logMark21);
        long long33 = logMark21.getLogFileId();
        long long34 = logMark21.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark37 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark40 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int41 = logMark37.compare(logMark40);
        logMark37.setLogMark((long) (short) 10, 10L);
        long long45 = logMark37.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark48 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark51 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int52 = logMark48.compare(logMark51);
        logMark48.setLogMark((long) (short) 10, 10L);
        long long56 = logMark48.getLogFileId();
        int int57 = logMark37.compare(logMark48);
        long long58 = logMark37.getLogFileId();
        long long59 = logMark37.getLogFileOffset();
        long long60 = logMark37.getLogFileId();
        int int61 = logMark21.compare(logMark37);
        org.apache.bookkeeper.bookie.LogMark logMark62 = new org.apache.bookkeeper.bookie.LogMark(logMark21);
        int int63 = logMark1.compare(logMark21);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 1" + "'", str4, "LogMark: logFileId - 10 , logFileOffset - 1");
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 10L + "'", long18 == 10L);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + 10L + "'", long29 == 10L);
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertEquals("'" + str31 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str31, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int32 + "' != '" + (-1) + "'", int32 == (-1));
        org.junit.Assert.assertTrue("'" + long33 + "' != '" + 10L + "'", long33 == 10L);
        org.junit.Assert.assertTrue("'" + long34 + "' != '" + 10L + "'", long34 == 10L);
        org.junit.Assert.assertTrue("'" + int41 + "' != '" + 0 + "'", int41 == 0);
        org.junit.Assert.assertTrue("'" + long45 + "' != '" + 10L + "'", long45 == 10L);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 0 + "'", int52 == 0);
        org.junit.Assert.assertTrue("'" + long56 + "' != '" + 10L + "'", long56 == 10L);
        org.junit.Assert.assertTrue("'" + int57 + "' != '" + 0 + "'", int57 == 0);
        org.junit.Assert.assertTrue("'" + long58 + "' != '" + 10L + "'", long58 == 10L);
        org.junit.Assert.assertTrue("'" + long59 + "' != '" + 10L + "'", long59 == 10L);
        org.junit.Assert.assertTrue("'" + long60 + "' != '" + 10L + "'", long60 == 10L);
        org.junit.Assert.assertTrue("'" + int61 + "' != '" + 0 + "'", int61 == 0);
        org.junit.Assert.assertTrue("'" + int63 + "' != '" + (-1) + "'", int63 == (-1));
    }

    @Test
    public void test0270() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0270");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark5.setLogMark((long) (short) 100, 35L);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
    }

    @Test
    public void test0271() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0271");
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
        java.nio.ByteBuffer byteBuffer57 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark32.writeLogMark(byteBuffer57);
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
    public void test0272() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0272");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int10 = logMark3.compare(logMark8);
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.readLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
    }

    @Test
    public void test0273() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0273");
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
        long long44 = logMark30.getLogFileOffset();
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
        org.junit.Assert.assertTrue("'" + long44 + "' != '" + 10L + "'", long44 == 10L);
    }

    @Test
    public void test0274() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0274");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        logMark3.setLogMark((long) '4', (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark12 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
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
    }

    @Test
    public void test0275() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0275");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        logMark2.setLogMark((long) ' ', (long) (byte) -1);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0276() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0276");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', (long) (byte) -1);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        long long6 = logMark5.getLogFileId();
        long long7 = logMark5.getLogFileId();
        int int8 = logMark2.compare(logMark5);
        java.lang.Class<?> wildcardClass9 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 0L + "'", long7 == 0L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 1 + "'", int8 == 1);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0277() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0277");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        logMark2.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long7 = logMark6.getLogFileOffset();
        java.lang.String str8 = logMark6.toString();
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark6.readLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 1L + "'", long7 == 1L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 1" + "'", str8, "LogMark: logFileId - 1 , logFileOffset - 1");
    }

    @Test
    public void test0278() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0278");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', 0L);
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
    public void test0279() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0279");
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
        java.nio.ByteBuffer byteBuffer58 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark16.readLogMark(byteBuffer58);
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
    public void test0280() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0280");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark7.compare(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark11);
        org.apache.bookkeeper.bookie.LogMark logMark14 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark14);
        java.lang.String str16 = logMark15.toString();
        int int17 = logMark11.compare(logMark15);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
        org.junit.Assert.assertNotNull(logMark14);
// flaky "32) test0280(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str16 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str16, "LogMark: logFileId - 100 , logFileOffset - 97");
// flaky "15) test0280(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int17 + "' != '" + (-1) + "'", int17 == (-1));
    }

    @Test
    public void test0281() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0281");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        java.lang.Class<?> wildcardClass4 = logMark3.getClass();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "33) test0281(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test0282() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0282");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        logMark2.setLogMark(1L, (long) (byte) 1);
        logMark2.setLogMark((long) (byte) 0, 0L);
        logMark2.setLogMark((long) 10, (long) (byte) 10);
        java.nio.ByteBuffer byteBuffer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test0283() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0283");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        java.lang.String str6 = logMark5.toString();
        int int7 = logMark3.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        long long10 = logMark8.getLogFileOffset();
        logMark8.setLogMark(1L, (long) (short) 100);
        logMark8.setLogMark((long) 10, (long) (byte) 1);
        int int17 = logMark5.compare(logMark8);
        java.lang.Class<?> wildcardClass18 = logMark8.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 35L + "'", long4 == 35L);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str6, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertNotNull(logMark8);
// flaky "34) test0283(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long10 + "' != '" + 97L + "'", long10 == 97L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + (-1) + "'", int17 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass18);
    }

    @Test
    public void test0284() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0284");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(35L, (long) 1);
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
    public void test0285() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0285");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(10L, (long) (short) -1);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0286() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0286");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (byte) -1, (long) (-1));
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
    }

    @Test
    public void test0287() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0287");
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
        java.lang.String str18 = logMark5.toString();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertNotNull(logMark14);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str18, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0288() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0288");
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
        java.nio.ByteBuffer byteBuffer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark17.writeLogMark(byteBuffer19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark9);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 1L + "'", long10 == 1L);
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 1" + "'", str12, "LogMark: logFileId - 10 , logFileOffset - 1");
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 32L + "'", long16 == 32L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
    }

    @Test
    public void test0289() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0289");
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
        logMark35.setLogMark((long) (short) 1, (long) (short) 10);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertNotNull(logMark25);
        org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 1" + "'", str27, "LogMark: logFileId - 10 , logFileOffset - 1");
        org.junit.Assert.assertNotNull(logMark29);
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 1L + "'", long30 == 1L);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 0 + "'", int33 == 0);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 1 + "'", int34 == 1);
    }

    @Test
    public void test0290() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0290");
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
        java.nio.ByteBuffer byteBuffer37 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark32.writeLogMark(byteBuffer37);
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
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 1" + "'", str36, "LogMark: logFileId - 10 , logFileOffset - 1");
    }

    @Test
    public void test0291() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0291");
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
        java.lang.Class<?> wildcardClass38 = logMark26.getClass();
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
        org.junit.Assert.assertNotNull(wildcardClass38);
    }

    @Test
    public void test0292() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0292");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        java.lang.String str8 = logMark5.toString();
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str8, "LogMark: logFileId - -1 , logFileOffset - 0");
    }

    @Test
    public void test0293() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0293");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long3 = logMark2.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "35) test0293(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
// flaky "16) test0293(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long3 + "' != '" + 97L + "'", long3 == 97L);
    }

    @Test
    public void test0294() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0294");
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
        java.lang.Class<?> wildcardClass30 = logMark27.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str23, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str24 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str24, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark25);
// flaky "36) test0294(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long28 + "' != '" + 100L + "'", long28 == 100L);
// flaky "17) test0294(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass30);
    }

    @Test
    public void test0295() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0295");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, (long) (short) 0);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0296() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0296");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark(100L, (long) '4');
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.Class<?> wildcardClass8 = logMark7.getClass();
        org.junit.Assert.assertNotNull(wildcardClass8);
    }

    @Test
    public void test0297() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0297");
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
        java.nio.ByteBuffer byteBuffer26 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark25.writeLogMark(byteBuffer26);
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
    }

    @Test
    public void test0298() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0298");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) (byte) 10);
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
    public void test0299() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0299");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long5 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 35L + "'", long5 == 35L);
    }

    @Test
    public void test0300() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0300");
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
        java.nio.ByteBuffer byteBuffer22 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer22);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + (-1) + "'", int10 == (-1));
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + (-1) + "'", int18 == (-1));
    }

    @Test
    public void test0301() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0301");
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
            logMark44.readLogMark(byteBuffer46);
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
// flaky "37) test0301(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + (-1) + "'", int45 == (-1));
    }

    @Test
    public void test0302() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0302");
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
        logMark15.setLogMark(9223372036854775807L, (long) '4');
        org.junit.Assert.assertNotNull(logMark0);
// flaky "38) test0302(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
    }

    @Test
    public void test0303() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0303");
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
        java.nio.ByteBuffer byteBuffer47 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark13.readLogMark(byteBuffer47);
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
// flaky "39) test0303(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + (-1) + "'", int45 == (-1));
        org.junit.Assert.assertEquals("'" + str46 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str46, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test0304() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0304");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        java.lang.String str4 = logMark1.toString();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark1.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "40) test0304(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str4, "LogMark: logFileId - 100 , logFileOffset - 97");
    }

    @Test
    public void test0305() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0305");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark7.setLogMark((long) (byte) 100, (long) (short) 0);
        int int11 = logMark2.compare(logMark7);
        java.lang.Class<?> wildcardClass12 = logMark7.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + (-1) + "'", int11 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass12);
    }

    @Test
    public void test0306() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0306");
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
        java.nio.ByteBuffer byteBuffer49 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark13.writeLogMark(byteBuffer49);
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
// flaky "41) test0306(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + (-1) + "'", int45 == (-1));
        org.junit.Assert.assertEquals("'" + str46 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str46, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long47 + "' != '" + 10L + "'", long47 == 10L);
        org.junit.Assert.assertTrue("'" + long48 + "' != '" + 10L + "'", long48 == 10L);
    }

    @Test
    public void test0307() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0307");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, (long) 'a');
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
    public void test0308() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0308");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        java.lang.String str3 = logMark2.toString();
        long long4 = logMark2.getLogFileId();
        java.lang.String str5 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark6 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        java.lang.String str10 = logMark8.toString();
        int int11 = logMark2.compare(logMark8);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + (-1L) + "'", long4 == (-1L));
        org.junit.Assert.assertEquals("'" + str5 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str5, "LogMark: logFileId - -1 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark6);
// flaky "42) test0308(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str10, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + (-1) + "'", int11 == (-1));
    }

    @Test
    public void test0309() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0309");
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
        java.lang.Class<?> wildcardClass26 = logMark25.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertNotNull(wildcardClass26);
    }

    @Test
    public void test0310() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0310");
        org.apache.bookkeeper.bookie.LogMark logMark0 = new org.apache.bookkeeper.bookie.LogMark();
        java.lang.String str1 = logMark0.toString();
        java.nio.ByteBuffer byteBuffer2 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.readLogMark(byteBuffer2);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str1, "LogMark: logFileId - 0 , logFileOffset - 0");
    }

    @Test
    public void test0311() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0311");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(52L, (long) 'a');
        java.lang.String str3 = logMark2.toString();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 97" + "'", str3, "LogMark: logFileId - 52 , logFileOffset - 97");
    }

    @Test
    public void test0312() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0312");
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
        java.lang.Class<?> wildcardClass24 = logMark13.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertNotNull(wildcardClass24);
    }

    @Test
    public void test0313() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0313");
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
        long long36 = logMark35.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertNotNull(logMark25);
// flaky "43) test0313(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str27, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark29);
// flaky "18) test0313(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long30 + "' != '" + 97L + "'", long30 == 97L);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 0 + "'", int33 == 0);
// flaky "8) test0313(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
// flaky "4) test0313(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long36 + "' != '" + 97L + "'", long36 == 97L);
    }

    @Test
    public void test0314() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0314");
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
        java.lang.Class<?> wildcardClass26 = logMark15.getClass();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "44) test0314(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertNotNull(logMark16);
// flaky "19) test0314(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str18, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark20);
// flaky "9) test0314(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long21 + "' != '" + 97L + "'", long21 == 97L);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertNotNull(wildcardClass26);
    }

    @Test
    public void test0315() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0315");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass4 = logMark3.getClass();
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test0316() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0316");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        logMark2.setLogMark((long) (-1), 9223372036854775807L);
        java.lang.Class<?> wildcardClass6 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass6);
    }

    @Test
    public void test0317() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0317");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        logMark2.setLogMark(32L, 9223372036854775807L);
        long long7 = logMark2.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "45) test0317(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
// flaky "20) test0317(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str3, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 9223372036854775807L + "'", long7 == 9223372036854775807L);
    }

    @Test
    public void test0318() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0318");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        java.lang.String str2 = logMark0.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        long long6 = logMark5.getLogFileId();
        int int7 = logMark0.compare(logMark5);
        long long8 = logMark5.getLogFileOffset();
        java.lang.String str9 = logMark5.toString();
        long long10 = logMark5.getLogFileId();
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.writeLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "46) test0318(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
// flaky "21) test0318(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark3);
// flaky "10) test0318(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
// flaky "5) test0318(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long8 + "' != '" + 97L + "'", long8 == 97L);
// flaky "2) test0318(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str9, "LogMark: logFileId - 100 , logFileOffset - 97");
// flaky "2) test0318(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long10 + "' != '" + 100L + "'", long10 == 100L);
    }

    @Test
    public void test0319() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0319");
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
        java.lang.Class<?> wildcardClass19 = logMark18.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertNotNull(logMark14);
// flaky "47) test0319(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int16 + "' != '" + (-1) + "'", int16 == (-1));
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0320() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0320");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, (long) (short) 0);
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
    public void test0321() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0321");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, (long) 10);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0322() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0322");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark();
        logMark3.setLogMark((long) (short) 1, (long) 0);
        int int7 = logMark0.compare(logMark3);
        org.junit.Assert.assertNotNull(logMark0);
// flaky "48) test0322(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long2 + "' != '" + 97L + "'", long2 == 97L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
    }

    @Test
    public void test0323() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0323");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark4.readLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "49) test0323(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 97");
    }

    @Test
    public void test0324() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0324");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass8 = logMark7.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(wildcardClass8);
    }

    @Test
    public void test0325() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0325");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long4 = logMark0.getLogFileId();
        logMark0.setLogMark(52L, (long) 100);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.writeLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "50) test0325(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 97L + "'", long1 == 97L);
// flaky "22) test0325(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long4 + "' != '" + 100L + "'", long4 == 100L);
    }

    @Test
    public void test0326() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0326");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark(100L, (long) '4');
        long long7 = logMark3.getLogFileOffset();
        java.lang.String str8 = logMark3.toString();
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 52L + "'", long7 == 52L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 52" + "'", str8, "LogMark: logFileId - 100 , logFileOffset - 52");
    }

    @Test
    public void test0327() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0327");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long3 = logMark2.getLogFileId();
        long long4 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 100L + "'", long4 == 100L);
    }

    @Test
    public void test0328() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0328");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long4 = logMark3.getLogFileOffset();
        long long5 = logMark3.getLogFileOffset();
        int int6 = logMark2.compare(logMark3);
        long long7 = logMark3.getLogFileId();
        long long8 = logMark3.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 100L + "'", long4 == 100L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 100L + "'", long5 == 100L);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + (-1) + "'", int6 == (-1));
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 52L + "'", long7 == 52L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 100L + "'", long8 == 100L);
    }

    @Test
    public void test0329() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0329");
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
        org.apache.bookkeeper.bookie.LogMark logMark20 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long21 = logMark20.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark22 = new org.apache.bookkeeper.bookie.LogMark(logMark20);
        java.lang.String str23 = logMark22.toString();
        logMark22.setLogMark(32L, 9223372036854775807L);
        long long27 = logMark22.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark(logMark22);
        int int29 = logMark14.compare(logMark28);
        java.nio.ByteBuffer byteBuffer30 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark14.readLogMark(byteBuffer30);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 52L + "'", long18 == 52L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
        org.junit.Assert.assertNotNull(logMark20);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 100L + "'", long21 == 100L);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str23, "LogMark: logFileId - 52 , logFileOffset - 100");
        org.junit.Assert.assertTrue("'" + long27 + "' != '" + 32L + "'", long27 == 32L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + 1 + "'", int29 == 1);
    }

    @Test
    public void test0330() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0330");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
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
    public void test0331() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0331");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 9223372036854775807L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass5 = logMark2.getClass();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 9223372036854775807L + "'", long3 == 9223372036854775807L);
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0332() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0332");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        long long3 = logMark2.getLogFileOffset();
        long long4 = logMark2.getLogFileId();
        logMark2.setLogMark((long) (short) -1, 52L);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 1L + "'", long4 == 1L);
    }

    @Test
    public void test0333() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0333");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, 9223372036854775807L);
    }

    @Test
    public void test0334() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0334");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long3 = logMark2.getLogFileId();
        logMark2.setLogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long8 = logMark7.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int11 = logMark2.compare(logMark7);
        logMark7.setLogMark((long) (short) -1, 52L);
        java.nio.ByteBuffer byteBuffer15 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark7.writeLogMark(byteBuffer15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 100L + "'", long3 == 100L);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 100L + "'", long8 == 100L);
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + (-1) + "'", int11 == (-1));
    }

    @Test
    public void test0335() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0335");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        logMark2.setLogMark((long) (-1), 9223372036854775807L);
        java.lang.String str6 = logMark2.toString();
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
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark34 = new org.apache.bookkeeper.bookie.LogMark(logMark33);
        java.lang.String str35 = logMark34.toString();
        logMark34.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark39 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark40 = new org.apache.bookkeeper.bookie.LogMark(logMark39);
        int int41 = logMark34.compare(logMark39);
        int int42 = logMark20.compare(logMark39);
        org.apache.bookkeeper.bookie.LogMark logMark45 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark48 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int49 = logMark45.compare(logMark48);
        org.apache.bookkeeper.bookie.LogMark logMark50 = new org.apache.bookkeeper.bookie.LogMark(logMark45);
        org.apache.bookkeeper.bookie.LogMark logMark51 = new org.apache.bookkeeper.bookie.LogMark(logMark50);
        int int52 = logMark20.compare(logMark51);
        long long53 = logMark20.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark56 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark(logMark56);
        java.lang.String str58 = logMark56.toString();
        int int59 = logMark20.compare(logMark56);
        int int60 = logMark2.compare(logMark20);
        java.nio.ByteBuffer byteBuffer61 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark20.writeLogMark(byteBuffer61);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807" + "'", str6, "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807");
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 10L + "'", long17 == 10L);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + 0 + "'", int29 == 0);
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 10L + "'", long30 == 10L);
        org.junit.Assert.assertEquals("'" + str35 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str35, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark39);
        org.junit.Assert.assertTrue("'" + int41 + "' != '" + (-1) + "'", int41 == (-1));
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 1 + "'", int42 == 1);
        org.junit.Assert.assertTrue("'" + int49 + "' != '" + 0 + "'", int49 == 0);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + (-1) + "'", int52 == (-1));
        org.junit.Assert.assertTrue("'" + long53 + "' != '" + 10L + "'", long53 == 10L);
        org.junit.Assert.assertEquals("'" + str58 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str58, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int59 + "' != '" + 1 + "'", int59 == 1);
        org.junit.Assert.assertTrue("'" + int60 + "' != '" + (-1) + "'", int60 == (-1));
    }

    @Test
    public void test0336() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0336");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        long long11 = logMark9.getLogFileId();
        int int12 = logMark3.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.nio.ByteBuffer byteBuffer14 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
    }

    @Test
    public void test0337() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0337");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark(100L, (long) '4');
        long long7 = logMark3.getLogFileOffset();
        java.lang.String str8 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        java.lang.String str12 = logMark11.toString();
        java.lang.String str13 = logMark11.toString();
        int int14 = logMark3.compare(logMark11);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 52L + "'", long7 == 52L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 52" + "'", str8, "LogMark: logFileId - 100 , logFileOffset - 52");
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str12, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str13, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 1 + "'", int14 == 1);
    }

    @Test
    public void test0338() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0338");
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
        java.lang.Class<?> wildcardClass25 = logMark16.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark17);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 52L + "'", long18 == 52L);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 52L + "'", long19 == 52L);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 1 + "'", int20 == 1);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 1 + "'", int24 == 1);
        org.junit.Assert.assertNotNull(wildcardClass25);
    }

    @Test
    public void test0339() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0339");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(35L, (long) (short) 1);
    }

    @Test
    public void test0340() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0340");
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
        java.nio.ByteBuffer byteBuffer25 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark1.readLogMark(byteBuffer25);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 52" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 52");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 1L + "'", long24 == 1L);
    }

    @Test
    public void test0341() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0341");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        java.lang.String str3 = logMark2.toString();
        long long4 = logMark2.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 1" + "'", str3, "LogMark: logFileId - 0 , logFileOffset - 1");
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 1L + "'", long4 == 1L);
    }

    @Test
    public void test0342() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0342");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        java.lang.String str2 = logMark0.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        long long6 = logMark5.getLogFileId();
        int int7 = logMark0.compare(logMark5);
        long long8 = logMark5.getLogFileOffset();
        java.lang.String str9 = logMark5.toString();
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 52L + "'", long1 == 52L);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 52" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 52");
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + (-1L) + "'", long6 == (-1L));
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 52L + "'", long8 == 52L);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 52" + "'", str9, "LogMark: logFileId - -1 , logFileOffset - 52");
    }

    @Test
    public void test0343() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0343");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 0, (long) '4');
    }

    @Test
    public void test0344() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0344");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 35L);
    }

    @Test
    public void test0345() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0345");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark5.setLogMark((long) (short) 100, 35L);
        long long9 = logMark5.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 35L + "'", long9 == 35L);
    }

    @Test
    public void test0346() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0346");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 10, 9223372036854775807L);
    }

    @Test
    public void test0347() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0347");
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
            logMark15.readLogMark(byteBuffer17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 52" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 52");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
    }

    @Test
    public void test0348() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0348");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        java.lang.Class<?> wildcardClass9 = logMark3.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0349() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0349");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark5);
        long long8 = logMark5.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 1L + "'", long8 == 1L);
    }

    @Test
    public void test0350() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0350");
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
        logMark57.setLogMark((long) 1, (long) 100);
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
    public void test0351() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0351");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark((long) '#', (long) (short) 0);
    }

    @Test
    public void test0352() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0352");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(1L, (-1L));
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
    public void test0353() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0353");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.writeLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
    }

    @Test
    public void test0354() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0354");
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
        org.apache.bookkeeper.bookie.LogMark logMark44 = null;
        // The following exception was thrown during execution in test generation
        try {
            int int45 = logMark30.compare(logMark44);
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
    public void test0355() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0355");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', 0L);
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
    public void test0356() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0356");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark7.setLogMark((long) (byte) 100, (long) (short) 0);
        int int11 = logMark2.compare(logMark7);
        java.nio.ByteBuffer byteBuffer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark7.writeLogMark(byteBuffer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + (-1) + "'", int11 == (-1));
    }

    @Test
    public void test0357() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0357");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0358() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0358");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        logMark2.setLogMark((long) (short) 10, 10L);
        long long10 = logMark2.getLogFileId();
        java.lang.String str11 = logMark2.toString();
        logMark2.setLogMark((long) 10, 9223372036854775807L);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str11, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test0359() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0359");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark0.toString();
        long long3 = logMark0.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 52" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 52");
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 52L + "'", long3 == 52L);
    }

    @Test
    public void test0360() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0360");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), 10L);
        logMark2.setLogMark((long) 100, (long) (short) 0);
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
    public void test0361() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0361");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark2.setLogMark((long) (byte) 100, (long) (short) 0);
        java.lang.Class<?> wildcardClass6 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass6);
    }

    @Test
    public void test0362() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0362");
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
        java.nio.ByteBuffer byteBuffer23 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer23);
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
    public void test0363() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0363");
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
        java.nio.ByteBuffer byteBuffer20 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark7.readLogMark(byteBuffer20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 52L + "'", long18 == 52L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
    }

    @Test
    public void test0364() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0364");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark6.writeLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0365() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0365");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark5);
        logMark5.setLogMark(0L, 97L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
    }

    @Test
    public void test0366() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0366");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) -1, (long) (short) -1);
        java.lang.String str3 = logMark2.toString();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - -1" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - -1");
    }

    @Test
    public void test0367() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0367");
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
        long long26 = logMark15.getLogFileId();
        java.nio.ByteBuffer byteBuffer27 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark15.readLogMark(byteBuffer27);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "51) test0367(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
// flaky "23) test0367(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertNotNull(logMark16);
// flaky "11) test0367(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str18, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark20);
// flaky "6) test0367(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertTrue("'" + long26 + "' != '" + (-1L) + "'", long26 == (-1L));
    }

    @Test
    public void test0368() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0368");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        logMark0.setLogMark(1L, (long) (short) 100);
        logMark0.setLogMark((long) 10, (long) (byte) 1);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.readLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "52) test0368(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long2 + "' != '" + 10L + "'", long2 == 10L);
    }

    @Test
    public void test0369() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0369");
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
        long long49 = logMark13.getLogFileOffset();
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
        org.junit.Assert.assertEquals("'" + str46 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str46, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long47 + "' != '" + 10L + "'", long47 == 10L);
        org.junit.Assert.assertTrue("'" + long48 + "' != '" + 10L + "'", long48 == 10L);
        org.junit.Assert.assertTrue("'" + long49 + "' != '" + 10L + "'", long49 == 10L);
    }

    @Test
    public void test0370() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0370");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        logMark2.setLogMark((long) (-1), 9223372036854775807L);
        java.lang.String str6 = logMark2.toString();
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
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark34 = new org.apache.bookkeeper.bookie.LogMark(logMark33);
        java.lang.String str35 = logMark34.toString();
        logMark34.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark39 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark40 = new org.apache.bookkeeper.bookie.LogMark(logMark39);
        int int41 = logMark34.compare(logMark39);
        int int42 = logMark20.compare(logMark39);
        org.apache.bookkeeper.bookie.LogMark logMark45 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark48 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int49 = logMark45.compare(logMark48);
        org.apache.bookkeeper.bookie.LogMark logMark50 = new org.apache.bookkeeper.bookie.LogMark(logMark45);
        org.apache.bookkeeper.bookie.LogMark logMark51 = new org.apache.bookkeeper.bookie.LogMark(logMark50);
        int int52 = logMark20.compare(logMark51);
        long long53 = logMark20.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark56 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark(logMark56);
        java.lang.String str58 = logMark56.toString();
        int int59 = logMark20.compare(logMark56);
        int int60 = logMark2.compare(logMark20);
        java.lang.String str61 = logMark20.toString();
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807" + "'", str6, "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807");
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 10L + "'", long17 == 10L);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + 0 + "'", int29 == 0);
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 10L + "'", long30 == 10L);
        org.junit.Assert.assertEquals("'" + str35 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str35, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark39);
        org.junit.Assert.assertTrue("'" + int41 + "' != '" + (-1) + "'", int41 == (-1));
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 1 + "'", int42 == 1);
        org.junit.Assert.assertTrue("'" + int49 + "' != '" + 0 + "'", int49 == 0);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + (-1) + "'", int52 == (-1));
        org.junit.Assert.assertTrue("'" + long53 + "' != '" + 10L + "'", long53 == 10L);
        org.junit.Assert.assertEquals("'" + str58 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str58, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int59 + "' != '" + 1 + "'", int59 == 1);
        org.junit.Assert.assertTrue("'" + int60 + "' != '" + (-1) + "'", int60 == (-1));
        org.junit.Assert.assertEquals("'" + str61 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str61, "LogMark: logFileId - 10 , logFileOffset - 10");
    }

    @Test
    public void test0371() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0371");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long8 = logMark7.getLogFileId();
        long long9 = logMark7.getLogFileId();
        java.nio.ByteBuffer byteBuffer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark7.writeLogMark(byteBuffer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 97L + "'", long8 == 97L);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 97L + "'", long9 == 97L);
    }

    @Test
    public void test0372() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0372");
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
        org.apache.bookkeeper.bookie.LogMark logMark28 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, 35L);
        int int29 = logMark13.compare(logMark28);
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
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 10L + "'", long25 == 10L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + 1 + "'", int29 == 1);
    }

    @Test
    public void test0373() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0373");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', 0L);
        long long3 = logMark2.getLogFileId();
        long long4 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 32L + "'", long4 == 32L);
    }

    @Test
    public void test0374() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0374");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, 1L);
    }

    @Test
    public void test0375() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0375");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long3 = logMark2.getLogFileOffset();
        logMark2.setLogMark(97L, (long) '4');
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
    }

    @Test
    public void test0376() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0376");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        java.lang.String str3 = logMark0.toString();
        java.lang.Class<?> wildcardClass4 = logMark0.getClass();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "53) test0376(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long2 + "' != '" + 10L + "'", long2 == 10L);
// flaky "24) test0376(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test0377() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0377");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 9223372036854775807L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass5 = logMark4.getClass();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 9223372036854775807L + "'", long3 == 9223372036854775807L);
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0378() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0378");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 0, 100L);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0379() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0379");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        logMark2.setLogMark(32L, 9223372036854775807L);
        long long7 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long9 = logMark2.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "54) test0379(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
// flaky "25) test0379(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 32L + "'", long7 == 32L);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 9223372036854775807L + "'", long9 == 9223372036854775807L);
    }

    @Test
    public void test0380() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0380");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long11 = logMark10.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark10.readLogMark(byteBuffer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 100L + "'", long11 == 100L);
    }

    @Test
    public void test0381() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0381");
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
            logMark3.readLogMark(byteBuffer19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + (-1L) + "'", long13 == (-1L));
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
    }

    @Test
    public void test0382() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0382");
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
        java.lang.Class<?> wildcardClass36 = logMark35.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertNotNull(logMark25);
// flaky "55) test0382(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str27, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark29);
// flaky "26) test0382(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long30 + "' != '" + 10L + "'", long30 == 10L);
        org.junit.Assert.assertTrue("'" + int33 + "' != '" + 0 + "'", int33 == 0);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 1 + "'", int34 == 1);
        org.junit.Assert.assertNotNull(wildcardClass36);
    }

    @Test
    public void test0383() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0383");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) (byte) 100);
        long long3 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
    }

    @Test
    public void test0384() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0384");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), (long) (byte) -1);
    }

    @Test
    public void test0385() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0385");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.Class<?> wildcardClass6 = logMark3.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(wildcardClass6);
    }

    @Test
    public void test0386() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0386");
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
        java.nio.ByteBuffer byteBuffer24 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer24);
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
    }

    @Test
    public void test0387() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0387");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        logMark2.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long7 = logMark6.getLogFileOffset();
        java.lang.String str8 = logMark6.toString();
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark6.writeLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 1L + "'", long7 == 1L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 1" + "'", str8, "LogMark: logFileId - 1 , logFileOffset - 1");
    }

    @Test
    public void test0388() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0388");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark(logMark8);
        int int10 = logMark3.compare(logMark8);
        long long11 = logMark8.getLogFileOffset();
        long long12 = logMark8.getLogFileOffset();
        logMark8.setLogMark((long) (short) 10, (long) (short) 1);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark8);
// flaky "56) test0388(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
// flaky "27) test0388(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long11 + "' != '" + 10L + "'", long11 == 10L);
// flaky "12) test0388(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long12 + "' != '" + 10L + "'", long12 == 10L);
    }

    @Test
    public void test0389() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0389");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 0, (long) 100);
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
    }

    @Test
    public void test0390() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0390");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
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
    public void test0391() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0391");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) (-1));
    }

    @Test
    public void test0392() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0392");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 0);
        long long3 = logMark2.getLogFileId();
        long long4 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 32L + "'", long3 == 32L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 32L + "'", long4 == 32L);
    }

    @Test
    public void test0393() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0393");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        java.lang.String str2 = logMark0.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        long long6 = logMark5.getLogFileId();
        int int7 = logMark0.compare(logMark5);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 1L + "'", long1 == 1L);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 1" + "'", str2, "LogMark: logFileId - 10 , logFileOffset - 1");
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 10L + "'", long6 == 10L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
    }

    @Test
    public void test0394() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0394");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        logMark2.setLogMark(1L, (long) (byte) 1);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark6.readLogMark(byteBuffer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test0395() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0395");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        java.lang.String str9 = logMark2.toString();
        logMark2.setLogMark((long) 10, 35L);
        long long13 = logMark2.getLogFileOffset();
        java.nio.ByteBuffer byteBuffer14 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str9, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 35L + "'", long13 == 35L);
    }

    @Test
    public void test0396() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0396");
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
        java.nio.ByteBuffer byteBuffer54 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark32.readLogMark(byteBuffer54);
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
// flaky "57) test0396(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
// flaky "28) test0396(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 32 , logFileOffset - 32" + "'", str36, "LogMark: logFileId - 32 , logFileOffset - 32");
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long51 + "' != '" + 10L + "'", long51 == 10L);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + (-1) + "'", int52 == (-1));
    }

    @Test
    public void test0397() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0397");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark2.setLogMark((long) (byte) 100, (long) (short) 0);
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
    public void test0398() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0398");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        java.lang.String str6 = logMark5.toString();
        int int7 = logMark3.compare(logMark5);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark5.readLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 35L + "'", long4 == 35L);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str6, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
    }

    @Test
    public void test0399() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0399");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(1L, 100L);
    }

    @Test
    public void test0400() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0400");
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
        java.nio.ByteBuffer byteBuffer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark17.readLogMark(byteBuffer19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark9);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str12, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 32L + "'", long16 == 32L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
    }

    @Test
    public void test0401() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0401");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        logMark2.setLogMark((long) (-1), 9223372036854775807L);
        java.lang.String str6 = logMark2.toString();
        logMark2.setLogMark(35L, (long) (byte) 10);
        logMark2.setLogMark((long) (byte) 10, (long) (byte) 0);
        logMark2.setLogMark((long) (byte) 1, (long) (byte) 1);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807" + "'", str6, "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807");
    }

    @Test
    public void test0402() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0402");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        long long5 = logMark2.getLogFileId();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 0L + "'", long5 == 0L);
    }

    @Test
    public void test0403() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0403");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        long long11 = logMark9.getLogFileId();
        int int12 = logMark3.compare(logMark9);
        org.apache.bookkeeper.bookie.LogMark logMark13 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.String str14 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, (long) (-1));
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str14, "LogMark: logFileId - 0 , logFileOffset - 35");
    }

    @Test
    public void test0404() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0404");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark3.setLogMark(100L, (long) '4');
        long long7 = logMark3.getLogFileOffset();
        java.lang.String str8 = logMark3.toString();
        logMark3.setLogMark((long) (short) 100, (long) 'a');
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 52L + "'", long7 == 52L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 52" + "'", str8, "LogMark: logFileId - 100 , logFileOffset - 52");
    }

    @Test
    public void test0405() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0405");
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
    }

    @Test
    public void test0406() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0406");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        logMark8.setLogMark(10L, 0L);
        logMark8.setLogMark((long) 100, (long) 'a');
        logMark8.setLogMark(52L, (long) 0);
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str10, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test0407() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0407");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        long long4 = logMark2.getLogFileOffset();
        logMark2.setLogMark(52L, 10L);
        java.nio.ByteBuffer byteBuffer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 10L + "'", long4 == 10L);
    }

    @Test
    public void test0408() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0408");
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
        java.lang.Class<?> wildcardClass30 = logMark23.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertEquals("'" + str20 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str20, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 100L + "'", long28 == 100L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass30);
    }

    @Test
    public void test0409() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0409");
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
        logMark8.setLogMark((long) '#', (long) 'a');
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark3);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + (-1L) + "'", long6 == (-1L));
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 0 + "'", int7 == 0);
        org.junit.Assert.assertNotNull(logMark8);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 10L + "'", long9 == 10L);
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
    }

    @Test
    public void test0410() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0410");
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
        java.nio.ByteBuffer byteBuffer23 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark1.readLogMark(byteBuffer23);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 35 , logFileOffset - 97");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 35L + "'", long15 == 35L);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 35L + "'", long19 == 35L);
    }

    @Test
    public void test0411() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0411");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark1.toString();
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long5 = logMark4.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark4);
        int int8 = logMark3.compare(logMark7);
        java.lang.Class<?> wildcardClass9 = logMark3.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 97" + "'", str2, "LogMark: logFileId - 35 , logFileOffset - 97");
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 97L + "'", long5 == 97L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 0 + "'", int8 == 0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0412() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0412");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.writeLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
    }

    @Test
    public void test0413() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0413");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), (long) 1);
    }

    @Test
    public void test0414() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0414");
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
        long long53 = logMark32.getLogFileOffset();
        java.lang.String str54 = logMark32.toString();
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
        org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 35 , logFileOffset - 97" + "'", str36, "LogMark: logFileId - 35 , logFileOffset - 97");
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 0 + "'", int47 == 0);
        org.junit.Assert.assertTrue("'" + long51 + "' != '" + 10L + "'", long51 == 10L);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + (-1) + "'", int52 == (-1));
        org.junit.Assert.assertTrue("'" + long53 + "' != '" + 10L + "'", long53 == 10L);
        org.junit.Assert.assertEquals("'" + str54 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str54, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test0415() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0415");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) ' ');
    }

    @Test
    public void test0416() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0416");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.nio.ByteBuffer byteBuffer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
    }

    @Test
    public void test0417() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0417");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str4, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test0418() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0418");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 0, (long) 1);
        logMark2.setLogMark((long) (-1), 9223372036854775807L);
        java.lang.String str6 = logMark2.toString();
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
        org.apache.bookkeeper.bookie.LogMark logMark33 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark34 = new org.apache.bookkeeper.bookie.LogMark(logMark33);
        java.lang.String str35 = logMark34.toString();
        logMark34.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark39 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark40 = new org.apache.bookkeeper.bookie.LogMark(logMark39);
        int int41 = logMark34.compare(logMark39);
        int int42 = logMark20.compare(logMark39);
        org.apache.bookkeeper.bookie.LogMark logMark45 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark48 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int49 = logMark45.compare(logMark48);
        org.apache.bookkeeper.bookie.LogMark logMark50 = new org.apache.bookkeeper.bookie.LogMark(logMark45);
        org.apache.bookkeeper.bookie.LogMark logMark51 = new org.apache.bookkeeper.bookie.LogMark(logMark50);
        int int52 = logMark20.compare(logMark51);
        long long53 = logMark20.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark56 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark(logMark56);
        java.lang.String str58 = logMark56.toString();
        int int59 = logMark20.compare(logMark56);
        int int60 = logMark2.compare(logMark20);
        logMark20.setLogMark((-1L), (long) 100);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807" + "'", str6, "LogMark: logFileId - -1 , logFileOffset - 9223372036854775807");
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 10L + "'", long17 == 10L);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + int29 + "' != '" + 0 + "'", int29 == 0);
        org.junit.Assert.assertTrue("'" + long30 + "' != '" + 10L + "'", long30 == 10L);
        org.junit.Assert.assertEquals("'" + str35 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str35, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark39);
        org.junit.Assert.assertTrue("'" + int41 + "' != '" + 0 + "'", int41 == 0);
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 1 + "'", int42 == 1);
        org.junit.Assert.assertTrue("'" + int49 + "' != '" + 0 + "'", int49 == 0);
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + (-1) + "'", int52 == (-1));
        org.junit.Assert.assertTrue("'" + long53 + "' != '" + 10L + "'", long53 == 10L);
        org.junit.Assert.assertEquals("'" + str58 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str58, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int59 + "' != '" + 1 + "'", int59 == 1);
        org.junit.Assert.assertTrue("'" + int60 + "' != '" + (-1) + "'", int60 == (-1));
    }

    @Test
    public void test0419() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0419");
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
        java.nio.ByteBuffer byteBuffer43 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark12.readLogMark(byteBuffer43);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
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
    }

    @Test
    public void test0420() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0420");
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
        logMark1.setLogMark(0L, 9223372036854775807L);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
    }

    @Test
    public void test0421() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0421");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark(0L, (long) (byte) 0);
        long long8 = logMark3.getLogFileId();
        java.lang.Class<?> wildcardClass9 = logMark3.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str4, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0422() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0422");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) 10);
        java.lang.String str3 = logMark2.toString();
        logMark2.setLogMark((long) (byte) 100, 100L);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 10" + "'", str3, "LogMark: logFileId - 0 , logFileOffset - 10");
    }

    @Test
    public void test0423() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0423");
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
        java.lang.Class<?> wildcardClass20 = logMark14.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 52L + "'", long18 == 52L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
        org.junit.Assert.assertNotNull(wildcardClass20);
    }

    @Test
    public void test0424() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0424");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str3 = logMark2.toString();
        logMark2.setLogMark(32L, 9223372036854775807L);
        long long7 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.Class<?> wildcardClass9 = logMark2.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str3, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 32L + "'", long7 == 32L);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0425() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0425");
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
        java.lang.String str19 = logMark3.toString();
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + (-1L) + "'", long13 == (-1L));
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str19, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0426() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0426");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        long long11 = logMark9.getLogFileId();
        int int12 = logMark3.compare(logMark9);
        long long13 = logMark9.getLogFileId();
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 97L + "'", long13 == 97L);
    }

    @Test
    public void test0427() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0427");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 0, (long) (byte) 0);
        java.lang.String str3 = logMark2.toString();
        java.lang.Class<?> wildcardClass4 = logMark2.getClass();
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str3, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test0428() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0428");
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
        java.lang.Class<?> wildcardClass42 = logMark24.getClass();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 1L + "'", long14 == 1L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 35L + "'", long19 == 35L);
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 35L + "'", long20 == 35L);
        org.junit.Assert.assertEquals("'" + str25 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str25, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark29);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + 0 + "'", int38 == 0);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + (-1) + "'", int39 == (-1));
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + (-1L) + "'", long40 == (-1L));
        org.junit.Assert.assertTrue("'" + int41 + "' != '" + 1 + "'", int41 == 1);
        org.junit.Assert.assertNotNull(wildcardClass42);
    }

    @Test
    public void test0429() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0429");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark2.setLogMark((long) (byte) 100, (long) (short) 0);
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
    public void test0430() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0430");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        java.lang.String str9 = logMark5.toString();
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
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - -1 , logFileOffset - 0");
    }

    @Test
    public void test0431() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0431");
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
        java.lang.String str21 = logMark8.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark9);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str12, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 32L + "'", long16 == 32L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 0L + "'", long19 == 0L);
        org.junit.Assert.assertEquals("'" + str20 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str20, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str21 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str21, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0432() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0432");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long1 = logMark0.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark(0L, (long) (byte) 0);
        logMark3.setLogMark(100L, (long) 1);
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.readLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertTrue("'" + long1 + "' != '" + 10L + "'", long1 == 10L);
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str4, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test0433() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0433");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        long long6 = logMark5.getLogFileId();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 97L + "'", long6 == 97L);
    }

    @Test
    public void test0434() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0434");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), 10L);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0435() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0435");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark7.compare(logMark11);
        java.nio.ByteBuffer byteBuffer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark7.readLogMark(byteBuffer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
    }

    @Test
    public void test0436() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0436");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        long long3 = logMark2.getLogFileOffset();
        long long4 = logMark2.getLogFileOffset();
        logMark2.setLogMark(0L, (long) (short) -1);
        long long8 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 35L + "'", long4 == 35L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 0L + "'", long8 == 0L);
    }

    @Test
    public void test0437() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0437");
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
        java.lang.Class<?> wildcardClass21 = logMark8.getClass();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark9);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str12, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 32L + "'", long16 == 32L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 0L + "'", long19 == 0L);
        org.junit.Assert.assertEquals("'" + str20 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str20, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test0438() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0438");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.lang.String str2 = logMark0.toString();
        java.lang.Class<?> wildcardClass3 = logMark0.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0439() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0439");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        logMark3.setLogMark((long) '4', (long) '4');
        logMark3.setLogMark(35L, (long) 10);
        java.nio.ByteBuffer byteBuffer15 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark3.writeLogMark(byteBuffer15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
    }

    @Test
    public void test0440() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0440");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark3);
        java.lang.String str9 = logMark8.toString();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str9, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test0441() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0441");
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
        java.lang.String str32 = logMark15.toString();
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
        org.junit.Assert.assertEquals("'" + str32 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str32, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test0442() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0442");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        java.lang.String str3 = logMark2.toString();
        java.nio.ByteBuffer byteBuffer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str3 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str3, "LogMark: logFileId - 97 , logFileOffset - 0");
    }

    @Test
    public void test0443() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0443");
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
        long long25 = logMark13.getLogFileId();
        java.nio.ByteBuffer byteBuffer26 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark13.readLogMark(byteBuffer26);
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
    }

    @Test
    public void test0444() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0444");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((-1L), 10L);
        long long3 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark6);
        java.nio.ByteBuffer byteBuffer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark6.readLogMark(byteBuffer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + (-1L) + "'", long3 == (-1L));
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + 1 + "'", int8 == 1);
    }

    @Test
    public void test0445() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0445");
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
        org.apache.bookkeeper.bookie.LogMark logMark43 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark(logMark43);
        java.lang.String str45 = logMark44.toString();
        logMark44.setLogMark((long) (short) -1, 10L);
        org.apache.bookkeeper.bookie.LogMark logMark49 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark50 = new org.apache.bookkeeper.bookie.LogMark(logMark49);
        int int51 = logMark44.compare(logMark49);
        org.apache.bookkeeper.bookie.LogMark logMark54 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark57 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int58 = logMark54.compare(logMark57);
        int int59 = logMark44.compare(logMark57);
        long long60 = logMark44.getLogFileOffset();
        long long61 = logMark44.getLogFileId();
        int int62 = logMark2.compare(logMark44);
        org.junit.Assert.assertNotNull(logMark8);
// flaky "58) test0445(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long9 + "' != '" + 100L + "'", long9 == 100L);
// flaky "29) test0445(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str11 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str11, "LogMark: logFileId - 52 , logFileOffset - 100");
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 1 + "'", int12 == 1);
// flaky "13) test0445(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 0 + "'", int20 == 0);
        org.junit.Assert.assertTrue("'" + long24 + "' != '" + 10L + "'", long24 == 10L);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + long35 + "' != '" + 10L + "'", long35 == 10L);
        org.junit.Assert.assertTrue("'" + int36 + "' != '" + 0 + "'", int36 == 0);
        org.junit.Assert.assertEquals("'" + str37 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str37, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str38 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str38, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + 1 + "'", int39 == 1);
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + 52L + "'", long40 == 52L);
        org.junit.Assert.assertEquals("'" + str45 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str45, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark49);
// flaky "7) test0445(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int51 + "' != '" + (-1) + "'", int51 == (-1));
        org.junit.Assert.assertTrue("'" + int58 + "' != '" + 0 + "'", int58 == 0);
        org.junit.Assert.assertTrue("'" + int59 + "' != '" + (-1) + "'", int59 == (-1));
        org.junit.Assert.assertTrue("'" + long60 + "' != '" + 10L + "'", long60 == 10L);
        org.junit.Assert.assertTrue("'" + long61 + "' != '" + (-1L) + "'", long61 == (-1L));
        org.junit.Assert.assertTrue("'" + int62 + "' != '" + 1 + "'", int62 == 1);
    }

    @Test
    public void test0446() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0446");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, 10L);
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
    public void test0447() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0447");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        logMark2.setLogMark((long) 0, (long) '#');
        java.nio.ByteBuffer byteBuffer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.writeLogMark(byteBuffer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
    }

    @Test
    public void test0448() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0448");
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
        logMark18.setLogMark((long) (byte) 1, 97L);
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertNotNull(logMark2);
// flaky "59) test0448(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str4, "LogMark: logFileId - 52 , logFileOffset - 100");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str9, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark13);
// flaky "30) test0448(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int15 + "' != '" + (-1) + "'", int15 == (-1));
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 0 + "'", int16 == 0);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long27 + "' != '" + (-1L) + "'", long27 == (-1L));
        org.junit.Assert.assertEquals("'" + str29 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 100" + "'", str29, "LogMark: logFileId - -1 , logFileOffset - 100");
// flaky "14) test0448(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int30 + "' != '" + 1 + "'", int30 == 1);
    }

    @Test
    public void test0449() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0449");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        long long2 = logMark0.getLogFileOffset();
        long long3 = logMark0.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark4 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.writeLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
// flaky "60) test0449(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long2 + "' != '" + 100L + "'", long2 == 100L);
// flaky "31) test0449(RegressionTest0)":         org.junit.Assert.assertTrue("'" + long3 + "' != '" + 52L + "'", long3 == 52L);
    }

    @Test
    public void test0450() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0450");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 1, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        long long6 = logMark5.getLogFileOffset();
        int int7 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        long long9 = logMark5.getLogFileOffset();
        long long10 = logMark5.getLogFileId();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 0L + "'", long9 == 0L);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + (-1L) + "'", long10 == (-1L));
    }

    @Test
    public void test0451() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0451");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) ' ', (long) 0);
        java.lang.Class<?> wildcardClass3 = logMark2.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0452() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0452");
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
        org.apache.bookkeeper.bookie.LogMark logMark16 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark17 = new org.apache.bookkeeper.bookie.LogMark(logMark16);
        java.lang.String str18 = logMark17.toString();
        logMark17.setLogMark((long) (-1), (long) (byte) 100);
        long long22 = logMark17.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark23 = new org.apache.bookkeeper.bookie.LogMark(logMark17);
        org.apache.bookkeeper.bookie.LogMark logMark24 = new org.apache.bookkeeper.bookie.LogMark(logMark17);
        long long25 = logMark24.getLogFileOffset();
        int int26 = logMark8.compare(logMark24);
        java.lang.String str27 = logMark24.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
// flaky "61) test0452(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str10, "LogMark: logFileId - 52 , logFileOffset - 100");
// flaky "32) test0452(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str13, "LogMark: logFileId - 52 , logFileOffset - 100");
        org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str18, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long22 + "' != '" + (-1L) + "'", long22 == (-1L));
        org.junit.Assert.assertTrue("'" + long25 + "' != '" + 100L + "'", long25 == 100L);
// flaky "15) test0452(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int26 + "' != '" + 1 + "'", int26 == 1);
        org.junit.Assert.assertEquals("'" + str27 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 100" + "'", str27, "LogMark: logFileId - -1 , logFileOffset - 100");
    }

    @Test
    public void test0453() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0453");
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
        long long41 = logMark32.getLogFileId();
        java.lang.String str42 = logMark32.toString();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
// flaky "62) test0453(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
// flaky "33) test0453(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
// flaky "16) test0453(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str36 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str36, "LogMark: logFileId - 52 , logFileOffset - 100");
        org.junit.Assert.assertTrue("'" + long41 + "' != '" + (-1L) + "'", long41 == (-1L));
        org.junit.Assert.assertEquals("'" + str42 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str42, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test0454() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0454");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark9 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int10 = logMark6.compare(logMark9);
        long long11 = logMark9.getLogFileId();
        int int12 = logMark3.compare(logMark9);
        java.lang.String str13 = logMark9.toString();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark9);
        java.lang.Class<?> wildcardClass15 = logMark9.getClass();
        org.junit.Assert.assertTrue("'" + int10 + "' != '" + 0 + "'", int10 == 0);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 97L + "'", long11 == 97L);
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + (-1) + "'", int12 == (-1));
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str13, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0455() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0455");
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
        org.apache.bookkeeper.bookie.LogMark logMark44 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
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
    }

    @Test
    public void test0456() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0456");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '4', 52L);
    }

    @Test
    public void test0457() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0457");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark7.setLogMark((long) (byte) 100, (long) (short) 0);
        int int11 = logMark2.compare(logMark7);
        long long12 = logMark7.getLogFileId();
        java.nio.ByteBuffer byteBuffer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark7.writeLogMark(byteBuffer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str4, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + (-1) + "'", int11 == (-1));
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 100L + "'", long12 == 100L);
    }

    @Test
    public void test0458() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0458");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        java.lang.String str9 = logMark2.toString();
        long long10 = logMark2.getLogFileId();
        long long11 = logMark2.getLogFileId();
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str9, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 0L + "'", long10 == 0L);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 0L + "'", long11 == 0L);
    }

    @Test
    public void test0459() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0459");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark();
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark5);
        int int7 = logMark2.compare(logMark6);
        logMark6.setLogMark((long) '4', 0L);
        org.apache.bookkeeper.bookie.LogMark logMark11 = null;
        // The following exception was thrown during execution in test generation
        try {
            int int12 = logMark6.compare(logMark11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
    }

    @Test
    public void test0460() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0460");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), 0L);
        logMark2.setLogMark((long) (short) 0, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark8.compare(logMark11);
        int int13 = logMark2.compare(logMark8);
        long long14 = logMark8.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 0L + "'", long14 == 0L);
    }

    @Test
    public void test0461() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0461");
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
            logMark24.writeLogMark(byteBuffer26);
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
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + (-1L) + "'", long15 == (-1L));
        org.junit.Assert.assertEquals("'" + str22 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str22, "LogMark: logFileId - 1 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 1 + "'", int25 == 1);
    }

    @Test
    public void test0462() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0462");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 10, (long) (short) 10);
    }

    @Test
    public void test0463() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0463");
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
        long long26 = logMark15.getLogFileId();
        long long27 = logMark15.getLogFileOffset();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str2, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertNotNull(logMark16);
        org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str18, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark20);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertTrue("'" + int25 + "' != '" + 0 + "'", int25 == 0);
        org.junit.Assert.assertTrue("'" + long26 + "' != '" + (-1L) + "'", long26 == (-1L));
        org.junit.Assert.assertTrue("'" + long27 + "' != '" + 10L + "'", long27 == 10L);
    }

    @Test
    public void test0464() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0464");
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
            logMark3.readLogMark(byteBuffer24);
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
    }

    @Test
    public void test0465() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0465");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        java.nio.ByteBuffer byteBuffer2 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.writeLogMark(byteBuffer2);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark0);
    }

    @Test
    public void test0466() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0466");
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
            logMark14.writeLogMark(byteBuffer15);
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
    public void test0467() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0467");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
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
    public void test0468() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0468");
        org.apache.bookkeeper.bookie.LogMark logMark0 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark1 = new org.apache.bookkeeper.bookie.LogMark(logMark0);
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(logMark1);
        logMark2.setLogMark(1L, (long) (byte) 1);
        java.lang.Class<?> wildcardClass6 = logMark2.getClass();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertNotNull(wildcardClass6);
    }

    @Test
    public void test0469() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0469");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        long long3 = logMark2.getLogFileId();
        long long4 = logMark2.getLogFileId();
        java.nio.ByteBuffer byteBuffer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark2.readLogMark(byteBuffer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 0L + "'", long3 == 0L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
    }

    @Test
    public void test0470() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0470");
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
        long long47 = logMark13.getLogFileId();
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
        org.junit.Assert.assertEquals("'" + str46 + "' != '" + "LogMark: logFileId - 10 , logFileOffset - 10" + "'", str46, "LogMark: logFileId - 10 , logFileOffset - 10");
        org.junit.Assert.assertTrue("'" + long47 + "' != '" + 10L + "'", long47 == 10L);
    }

    @Test
    public void test0471() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0471");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 100, (long) (short) 1);
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
    public void test0472() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0472");
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
        java.lang.Class<?> wildcardClass16 = logMark13.getClass();
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 97L + "'", long14 == 97L);
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str15, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(wildcardClass16);
    }

    @Test
    public void test0473() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0473");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) 10);
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
    public void test0474() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0474");
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
        java.nio.ByteBuffer byteBuffer53 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark13.writeLogMark(byteBuffer53);
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
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
        org.junit.Assert.assertEquals("'" + str51 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 35" + "'", str51, "LogMark: logFileId - 0 , logFileOffset - 35");
        org.junit.Assert.assertTrue("'" + int52 + "' != '" + 1 + "'", int52 == 1);
    }

    @Test
    public void test0475() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0475");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.readLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str10, "LogMark: logFileId - -1 , logFileOffset - 10");
    }

    @Test
    public void test0476() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0476");
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
        java.nio.ByteBuffer byteBuffer20 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark15.writeLogMark(byteBuffer20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int7 + "' != '" + 1 + "'", int7 == 1);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
    }

    @Test
    public void test0477() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0477");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long4 = logMark3.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, (long) (short) 0);
        int int8 = logMark3.compare(logMark7);
        logMark3.setLogMark((long) 'a', (long) (short) 1);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 35L + "'", long4 == 35L);
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
    }

    @Test
    public void test0478() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0478");
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
            logMark12.writeLogMark(byteBuffer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + (-1L) + "'", long11 == (-1L));
    }

    @Test
    public void test0479() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0479");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) '#', 0L);
        logMark2.setLogMark((long) (byte) 100, (long) (short) 0);
        long long6 = logMark2.getLogFileOffset();
        long long7 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 0L + "'", long7 == 0L);
    }

    @Test
    public void test0480() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0480");
        org.apache.bookkeeper.bookie.LogMark logMark0 = new org.apache.bookkeeper.bookie.LogMark();
        java.lang.String str1 = logMark0.toString();
        long long2 = logMark0.getLogFileId();
        java.nio.ByteBuffer byteBuffer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark0.readLogMark(byteBuffer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "LogMark: logFileId - 0 , logFileOffset - 0" + "'", str1, "LogMark: logFileId - 0 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 0L + "'", long2 == 0L);
    }

    @Test
    public void test0481() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0481");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark3.toString();
        logMark3.setLogMark((long) (-1), (long) (byte) 100);
        long long8 = logMark3.getLogFileId();
        logMark3.setLogMark(0L, 0L);
        org.apache.bookkeeper.bookie.LogMark logMark12 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        long long13 = logMark12.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark14 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark15 = new org.apache.bookkeeper.bookie.LogMark(logMark12);
        org.apache.bookkeeper.bookie.LogMark logMark18 = new org.apache.bookkeeper.bookie.LogMark((long) (-1), (long) 10);
        int int19 = logMark12.compare(logMark18);
        int int20 = logMark3.compare(logMark18);
        long long21 = logMark18.getLogFileOffset();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + (-1L) + "'", long8 == (-1L));
        org.junit.Assert.assertNotNull(logMark12);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + 0 + "'", int19 == 0);
        org.junit.Assert.assertTrue("'" + int20 + "' != '" + 1 + "'", int20 == 1);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
    }

    @Test
    public void test0482() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0482");
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
        java.nio.ByteBuffer byteBuffer17 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark12.writeLogMark(byteBuffer17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 10L + "'", long5 == 10L);
        org.junit.Assert.assertEquals("'" + str6 + "' != '" + "LogMark: logFileId - -1 , logFileOffset - 10" + "'", str6, "LogMark: logFileId - -1 , logFileOffset - 10");
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + (-1L) + "'", long10 == (-1L));
        org.junit.Assert.assertTrue("'" + int11 + "' != '" + 0 + "'", int11 == 0);
        org.junit.Assert.assertNotNull(logMark12);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 10L + "'", long13 == 10L);
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 10L + "'", long15 == 10L);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
    }

    @Test
    public void test0483() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0483");
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
        long long42 = logMark24.getLogFileId();
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 0L + "'", long4 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 1 + "'", int13 == 1);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 1L + "'", long14 == 1L);
        org.junit.Assert.assertTrue("'" + int18 + "' != '" + 1 + "'", int18 == 1);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 35L + "'", long19 == 35L);
        org.junit.Assert.assertTrue("'" + long20 + "' != '" + 35L + "'", long20 == 35L);
        org.junit.Assert.assertEquals("'" + str25 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str25, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark29);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
        org.junit.Assert.assertTrue("'" + int38 + "' != '" + 0 + "'", int38 == 0);
        org.junit.Assert.assertTrue("'" + int39 + "' != '" + (-1) + "'", int39 == (-1));
        org.junit.Assert.assertTrue("'" + long40 + "' != '" + (-1L) + "'", long40 == (-1L));
        org.junit.Assert.assertTrue("'" + int41 + "' != '" + 1 + "'", int41 == 1);
        org.junit.Assert.assertTrue("'" + long42 + "' != '" + (-1L) + "'", long42 == (-1L));
    }

    @Test
    public void test0484() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0484");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str4 = logMark2.toString();
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        long long6 = logMark5.getLogFileId();
        java.lang.Class<?> wildcardClass7 = logMark5.getClass();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 97L + "'", long6 == 97L);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0485() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0485");
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
        long long46 = logMark13.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 10L + "'", long10 == 10L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertTrue("'" + long21 + "' != '" + 10L + "'", long21 == 10L);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 0 + "'", int22 == 0);
        org.junit.Assert.assertTrue("'" + long23 + "' != '" + 10L + "'", long23 == 10L);
        org.junit.Assert.assertEquals("'" + str28 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str28, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark32);
// flaky "63) test0485(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int34 + "' != '" + (-1) + "'", int34 == (-1));
// flaky "34) test0485(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int35 + "' != '" + (-1) + "'", int35 == (-1));
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + (-1) + "'", int45 == (-1));
        org.junit.Assert.assertTrue("'" + long46 + "' != '" + 10L + "'", long46 == 10L);
    }

    @Test
    public void test0486() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0486");
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
        java.nio.ByteBuffer byteBuffer20 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark7.writeLogMark(byteBuffer20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 52L + "'", long18 == 52L);
        org.junit.Assert.assertTrue("'" + int19 + "' != '" + (-1) + "'", int19 == (-1));
    }

    @Test
    public void test0487() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0487");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 1);
    }

    @Test
    public void test0488() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0488");
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
        long long29 = logMark22.getLogFileId();
        org.junit.Assert.assertEquals("'" + str4 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str4, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 0L + "'", long6 == 0L);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + 0 + "'", int13 == 0);
        org.junit.Assert.assertNotNull(logMark14);
        org.junit.Assert.assertTrue("'" + int16 + "' != '" + 1 + "'", int16 == 1);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + 1 + "'", int27 == 1);
        org.junit.Assert.assertTrue("'" + long28 + "' != '" + 10L + "'", long28 == 10L);
        org.junit.Assert.assertTrue("'" + long29 + "' != '" + (-1L) + "'", long29 == (-1L));
    }

    @Test
    public void test0489() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0489");
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
        java.nio.ByteBuffer byteBuffer32 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark20.readLogMark(byteBuffer32);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + (-1L) + "'", long9 == (-1L));
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 0L + "'", long14 == 0L);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 1 + "'", int17 == 1);
        org.junit.Assert.assertTrue("'" + int24 + "' != '" + 0 + "'", int24 == 0);
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + (-1) + "'", int26 == (-1));
        org.junit.Assert.assertTrue("'" + int27 + "' != '" + (-1) + "'", int27 == (-1));
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 1 + "'", int31 == 1);
    }

    @Test
    public void test0490() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0490");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (byte) 1, (long) (short) 1);
    }

    @Test
    public void test0491() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0491");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        long long9 = logMark2.getLogFileOffset();
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 35L + "'", long9 == 35L);
    }

    @Test
    public void test0492() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0492");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) (short) 1, 35L);
        long long3 = logMark2.getLogFileOffset();
        long long4 = logMark2.getLogFileId();
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(100L, (long) 10);
        long long8 = logMark7.getLogFileId();
        int int9 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark10 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        logMark2.setLogMark(0L, (long) (byte) 10);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 1L + "'", long4 == 1L);
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 100L + "'", long8 == 100L);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + (-1) + "'", int9 == (-1));
    }

    @Test
    public void test0493() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0493");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) (byte) -1);
        logMark2.setLogMark((long) (byte) 1, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        java.lang.String str7 = logMark6.toString();
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 1 , logFileOffset - 35" + "'", str7, "LogMark: logFileId - 1 , logFileOffset - 35");
    }

    @Test
    public void test0494() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0494");
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
        java.nio.ByteBuffer byteBuffer16 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark15.readLogMark(byteBuffer16);
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
    public void test0495() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0495");
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
        org.apache.bookkeeper.bookie.LogMark logMark62 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark63 = new org.apache.bookkeeper.bookie.LogMark(logMark62);
        long long64 = logMark63.getLogFileOffset();
        long long65 = logMark63.getLogFileId();
        logMark63.setLogMark(97L, (long) '4');
        int int69 = logMark57.compare(logMark63);
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
        org.junit.Assert.assertTrue("'" + long64 + "' != '" + 35L + "'", long64 == 35L);
        org.junit.Assert.assertTrue("'" + long65 + "' != '" + 0L + "'", long65 == 0L);
        org.junit.Assert.assertTrue("'" + int69 + "' != '" + (-1) + "'", int69 == (-1));
    }

    @Test
    public void test0496() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0496");
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
        java.lang.String str33 = logMark32.toString();
        org.junit.Assert.assertNotNull(logMark0);
// flaky "64) test0496(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str2, "LogMark: logFileId - 52 , logFileOffset - 100");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
// flaky "35) test0496(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertNotNull(logMark16);
// flaky "17) test0496(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str18 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str18, "LogMark: logFileId - 52 , logFileOffset - 100");
        org.junit.Assert.assertEquals("'" + str23 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str23, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark27);
// flaky "8) test0496(RegressionTest0)":         org.junit.Assert.assertTrue("'" + int29 + "' != '" + (-1) + "'", int29 == (-1));
        org.junit.Assert.assertTrue("'" + int30 + "' != '" + 0 + "'", int30 == 0);
        org.junit.Assert.assertTrue("'" + int31 + "' != '" + 0 + "'", int31 == 0);
// flaky "3) test0496(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str33 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str33, "LogMark: logFileId - 52 , logFileOffset - 100");
    }

    @Test
    public void test0497() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0497");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark5 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int6 = logMark2.compare(logMark5);
        org.apache.bookkeeper.bookie.LogMark logMark7 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        org.apache.bookkeeper.bookie.LogMark logMark8 = new org.apache.bookkeeper.bookie.LogMark(logMark7);
        int int9 = logMark2.compare(logMark8);
        java.lang.String str10 = logMark8.toString();
        java.nio.ByteBuffer byteBuffer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark8.writeLogMark(byteBuffer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int6 + "' != '" + 0 + "'", int6 == 0);
        org.junit.Assert.assertNotNull(logMark7);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 1 + "'", int9 == 1);
// flaky "65) test0497(RegressionTest0)":         org.junit.Assert.assertEquals("'" + str10 + "' != '" + "LogMark: logFileId - 52 , logFileOffset - 100" + "'", str10, "LogMark: logFileId - 52 , logFileOffset - 100");
    }

    @Test
    public void test0498() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0498");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(0L, (long) '#');
        org.apache.bookkeeper.bookie.LogMark logMark3 = new org.apache.bookkeeper.bookie.LogMark(logMark2);
        org.apache.bookkeeper.bookie.LogMark logMark6 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        org.apache.bookkeeper.bookie.LogMark logMark7 = new org.apache.bookkeeper.bookie.LogMark(logMark6);
        int int8 = logMark2.compare(logMark7);
        org.apache.bookkeeper.bookie.LogMark logMark11 = new org.apache.bookkeeper.bookie.LogMark((long) 'a', (long) 0);
        int int12 = logMark7.compare(logMark11);
        logMark11.setLogMark(97L, 97L);
        java.nio.ByteBuffer byteBuffer16 = null;
        // The following exception was thrown during execution in test generation
        try {
            logMark11.writeLogMark(byteBuffer16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + int8 + "' != '" + (-1) + "'", int8 == (-1));
        org.junit.Assert.assertTrue("'" + int12 + "' != '" + 0 + "'", int12 == 0);
    }

    @Test
    public void test0499() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0499");
        org.apache.bookkeeper.bookie.LogMark logMark2 = new org.apache.bookkeeper.bookie.LogMark(97L, 35L);
        long long3 = logMark2.getLogFileOffset();
        org.apache.bookkeeper.bookie.LogMark logMark4 = org.apache.bookkeeper.bookie.LogMark.MAX_VALUE;
        int int5 = logMark2.compare(logMark4);
        logMark4.setLogMark((long) 100, 32L);
        org.junit.Assert.assertTrue("'" + long3 + "' != '" + 35L + "'", long3 == 35L);
        org.junit.Assert.assertNotNull(logMark4);
        org.junit.Assert.assertTrue("'" + int5 + "' != '" + 1 + "'", int5 == 1);
    }

    @Test
    public void test0500() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0500");
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
        java.lang.String str17 = logMark15.toString();
        org.junit.Assert.assertNotNull(logMark0);
        org.junit.Assert.assertEquals("'" + str2 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 32" + "'", str2, "LogMark: logFileId - 100 , logFileOffset - 32");
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "LogMark: logFileId - 97 , logFileOffset - 0" + "'", str7, "LogMark: logFileId - 97 , logFileOffset - 0");
        org.junit.Assert.assertNotNull(logMark11);
        org.junit.Assert.assertTrue("'" + int13 + "' != '" + (-1) + "'", int13 == (-1));
        org.junit.Assert.assertTrue("'" + int14 + "' != '" + 0 + "'", int14 == 0);
        org.junit.Assert.assertEquals("'" + str17 + "' != '" + "LogMark: logFileId - 100 , logFileOffset - 32" + "'", str17, "LogMark: logFileId - 100 , logFileOffset - 32");
    }
}
