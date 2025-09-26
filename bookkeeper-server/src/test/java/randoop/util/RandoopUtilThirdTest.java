package randoop.util;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RandoopUtilThirdTest {

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
        org.apache.bookkeeper.util.ByteBufList.Encoder encoder0 = new org.apache.bookkeeper.util.ByteBufList.Encoder();
        io.netty.channel.ChannelHandlerContext channelHandlerContext1 = null;
        encoder0.handlerRemoved(channelHandlerContext1);
        io.netty.channel.ChannelHandlerContext channelHandlerContext3 = null;
        encoder0.handlerRemoved(channelHandlerContext3);
        io.netty.channel.ChannelHandlerContext channelHandlerContext5 = null;
        encoder0.handlerAdded(channelHandlerContext5);
        boolean boolean7 = encoder0.isSharable();
        io.netty.channel.ChannelHandlerContext channelHandlerContext8 = null;
        io.netty.channel.ChannelPromise channelPromise9 = null;
        // The following exception was thrown during execution in test generation
        try {
            encoder0.deregister(channelHandlerContext8, channelPromise9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertTrue("'" + boolean7 + "' != '" + true + "'", boolean7 == true);
    }

    @Test
    public void test1002() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1002");
        org.apache.bookkeeper.util.ByteBufList.Encoder encoder0 = new org.apache.bookkeeper.util.ByteBufList.Encoder();
        io.netty.channel.ChannelHandlerContext channelHandlerContext1 = null;
        encoder0.handlerRemoved(channelHandlerContext1);
        io.netty.channel.ChannelHandlerContext channelHandlerContext3 = null;
        encoder0.handlerAdded(channelHandlerContext3);
        io.netty.channel.ChannelHandlerContext channelHandlerContext5 = null;
        encoder0.handlerRemoved(channelHandlerContext5);
        io.netty.channel.ChannelHandlerContext channelHandlerContext7 = null;
        encoder0.handlerAdded(channelHandlerContext7);
    }

    @Test
    public void test1003() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1003");
        org.apache.bookkeeper.util.ByteBufList byteBufList0 = org.apache.bookkeeper.util.ByteBufList.get();
        int int1 = byteBufList0.readableBytes();
        io.netty.util.ReferenceCounted referenceCounted3 = byteBufList0.retain((int) (byte) 1);
        io.netty.util.ReferenceCounted referenceCounted4 = byteBufList0.touch();
        org.apache.bookkeeper.util.ByteBufList byteBufList5 = byteBufList0.retain();
        boolean boolean6 = byteBufList0.release();
        boolean boolean7 = byteBufList0.hasArray();
        org.apache.bookkeeper.util.ByteBufList byteBufList8 = org.apache.bookkeeper.util.ByteBufList.get();
        int int9 = byteBufList8.readableBytes();
        io.netty.util.ReferenceCounted referenceCounted11 = byteBufList8.retain((int) (byte) 1);
        io.netty.util.ReferenceCounted referenceCounted12 = byteBufList8.touch();
        org.apache.bookkeeper.util.ByteBufList byteBufList13 = byteBufList8.retain();
        byte[] byteArray14 = byteBufList8.toArray();
        int int15 = byteBufList8.size();
        org.apache.bookkeeper.util.ByteBufList byteBufList16 = org.apache.bookkeeper.util.ByteBufList.get();
        int int17 = byteBufList16.readableBytes();
        io.netty.util.ReferenceCounted referenceCounted19 = byteBufList16.retain((int) (byte) 1);
        io.netty.util.ReferenceCounted referenceCounted20 = byteBufList16.touch();
        org.apache.bookkeeper.util.ByteBufList byteBufList21 = byteBufList16.retain();
        int int22 = byteBufList16.refCnt();
        boolean boolean23 = byteBufList16.hasArray();
        io.netty.buffer.ByteBuf byteBuf24 = org.apache.bookkeeper.util.ByteBufList.coalesce(byteBufList16);
        byteBufList8.add(byteBuf24);
        byteBufList0.prepend(byteBuf24);
        org.apache.bookkeeper.util.ByteBufList byteBufList27 = org.apache.bookkeeper.util.ByteBufList.get();
        int int28 = byteBufList27.readableBytes();
        io.netty.util.ReferenceCounted referenceCounted30 = byteBufList27.retain((int) (byte) 1);
        org.apache.bookkeeper.util.ByteBufList byteBufList31 = org.apache.bookkeeper.util.ByteBufList.get();
        io.netty.buffer.ByteBuf byteBuf32 = org.apache.bookkeeper.util.ByteBufList.coalesce(byteBufList31);
        byteBufList27.add(byteBuf32);
        int int34 = byteBufList27.arrayOffset();
        boolean boolean35 = byteBufList27.hasArray();
        byte[] byteArray41 = new byte[] { (byte) -1, (byte) 10, (byte) 0, (byte) 10, (byte) -1 };
        int int42 = byteBufList27.getBytes(byteArray41);
        boolean boolean43 = byteBufList27.hasArray();
        boolean boolean44 = byteBufList27.release();
        int int45 = byteBufList27.readableBytes();
        io.netty.buffer.ByteBuf byteBuf46 = org.apache.bookkeeper.util.ByteBufList.coalesce(byteBufList27);
        org.apache.bookkeeper.util.ByteBufList byteBufList47 = org.apache.bookkeeper.util.ByteBufList.get(byteBuf24, byteBuf46);
        org.junit.Assert.assertNotNull(byteBufList0);
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 0 + "'", int1 == 0);
        org.junit.Assert.assertNotNull(referenceCounted3);
        org.junit.Assert.assertNotNull(referenceCounted4);
        org.junit.Assert.assertNotNull(byteBufList5);
        org.junit.Assert.assertTrue("'" + boolean6 + "' != '" + false + "'", boolean6 == false);
        org.junit.Assert.assertTrue("'" + boolean7 + "' != '" + false + "'", boolean7 == false);
        org.junit.Assert.assertNotNull(byteBufList8);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertNotNull(referenceCounted11);
        org.junit.Assert.assertNotNull(referenceCounted12);
        org.junit.Assert.assertNotNull(byteBufList13);
        org.junit.Assert.assertNotNull(byteArray14);
        org.junit.Assert.assertArrayEquals(byteArray14, new byte[] {});
        org.junit.Assert.assertTrue("'" + int15 + "' != '" + 0 + "'", int15 == 0);
        org.junit.Assert.assertNotNull(byteBufList16);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertNotNull(referenceCounted19);
        org.junit.Assert.assertNotNull(referenceCounted20);
        org.junit.Assert.assertNotNull(byteBufList21);
        org.junit.Assert.assertTrue("'" + int22 + "' != '" + 3 + "'", int22 == 3);
        org.junit.Assert.assertTrue("'" + boolean23 + "' != '" + false + "'", boolean23 == false);
        org.junit.Assert.assertNotNull(byteBuf24);
        org.junit.Assert.assertNotNull(byteBufList27);
        org.junit.Assert.assertTrue("'" + int28 + "' != '" + 0 + "'", int28 == 0);
        org.junit.Assert.assertNotNull(referenceCounted30);
        org.junit.Assert.assertNotNull(byteBufList31);
        org.junit.Assert.assertNotNull(byteBuf32);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
        org.junit.Assert.assertTrue("'" + boolean35 + "' != '" + true + "'", boolean35 == true);
        org.junit.Assert.assertNotNull(byteArray41);
        org.junit.Assert.assertArrayEquals(byteArray41, new byte[] { (byte) -1, (byte) 10, (byte) 0, (byte) 10, (byte) -1 });
        org.junit.Assert.assertTrue("'" + int42 + "' != '" + 0 + "'", int42 == 0);
        org.junit.Assert.assertTrue("'" + boolean43 + "' != '" + true + "'", boolean43 == true);
        org.junit.Assert.assertTrue("'" + boolean44 + "' != '" + false + "'", boolean44 == false);
        org.junit.Assert.assertTrue("'" + int45 + "' != '" + 0 + "'", int45 == 0);
        org.junit.Assert.assertNotNull(byteBuf46);
        org.junit.Assert.assertNotNull(byteBufList47);
    }

    @Test
    public void test1004() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "Regression3Test.test1004");
        org.apache.bookkeeper.util.ByteBufList byteBufList0 = org.apache.bookkeeper.util.ByteBufList.get();
        int int1 = byteBufList0.readableBytes();
        io.netty.util.ReferenceCounted referenceCounted3 = byteBufList0.retain((int) (byte) 1);
        org.apache.bookkeeper.util.ByteBufList byteBufList4 = org.apache.bookkeeper.util.ByteBufList.get();
        io.netty.buffer.ByteBuf byteBuf5 = org.apache.bookkeeper.util.ByteBufList.coalesce(byteBufList4);
        byteBufList0.add(byteBuf5);
        org.apache.bookkeeper.util.ByteBufList byteBufList7 = org.apache.bookkeeper.util.ByteBufList.get(byteBuf5);
        org.apache.bookkeeper.util.ByteBufList byteBufList8 = org.apache.bookkeeper.util.ByteBufList.get();
        int int9 = byteBufList8.readableBytes();
        io.netty.util.ReferenceCounted referenceCounted11 = byteBufList8.retain((int) (byte) 1);
        org.apache.bookkeeper.util.ByteBufList byteBufList12 = org.apache.bookkeeper.util.ByteBufList.get();
        io.netty.buffer.ByteBuf byteBuf13 = org.apache.bookkeeper.util.ByteBufList.coalesce(byteBufList12);
        byteBufList8.add(byteBuf13);
        org.apache.bookkeeper.util.ByteBufList byteBufList15 = org.apache.bookkeeper.util.ByteBufList.get(byteBuf13);
        org.apache.bookkeeper.util.ByteBufList byteBufList16 = org.apache.bookkeeper.util.ByteBufList.get();
        int int17 = byteBufList16.readableBytes();
        io.netty.util.ReferenceCounted referenceCounted19 = byteBufList16.retain((int) (byte) 1);
        org.apache.bookkeeper.util.ByteBufList byteBufList20 = org.apache.bookkeeper.util.ByteBufList.get();
        io.netty.buffer.ByteBuf byteBuf21 = org.apache.bookkeeper.util.ByteBufList.coalesce(byteBufList20);
        byteBufList16.add(byteBuf21);
        org.apache.bookkeeper.util.ByteBufList byteBufList23 = org.apache.bookkeeper.util.ByteBufList.get(byteBuf13, byteBuf21);
        org.apache.bookkeeper.util.ByteBufList byteBufList24 = org.apache.bookkeeper.util.ByteBufList.get(byteBuf5, byteBuf21);
        org.apache.bookkeeper.util.ByteBufList byteBufList25 = org.apache.bookkeeper.util.ByteBufList.get();
        int int26 = byteBufList25.readableBytes();
        io.netty.util.ReferenceCounted referenceCounted28 = byteBufList25.retain((int) (byte) 1);
        org.apache.bookkeeper.util.ByteBufList byteBufList29 = org.apache.bookkeeper.util.ByteBufList.get();
        io.netty.buffer.ByteBuf byteBuf30 = org.apache.bookkeeper.util.ByteBufList.coalesce(byteBufList29);
        byteBufList25.add(byteBuf30);
        org.apache.bookkeeper.util.ByteBufList byteBufList32 = org.apache.bookkeeper.util.ByteBufList.get(byteBuf30);
        org.apache.bookkeeper.util.ByteBufList byteBufList33 = org.apache.bookkeeper.util.ByteBufList.get();
        int int34 = byteBufList33.readableBytes();
        io.netty.util.ReferenceCounted referenceCounted36 = byteBufList33.retain((int) (byte) 1);
        org.apache.bookkeeper.util.ByteBufList byteBufList37 = org.apache.bookkeeper.util.ByteBufList.get();
        io.netty.buffer.ByteBuf byteBuf38 = org.apache.bookkeeper.util.ByteBufList.coalesce(byteBufList37);
        byteBufList33.add(byteBuf38);
        org.apache.bookkeeper.util.ByteBufList byteBufList40 = org.apache.bookkeeper.util.ByteBufList.get(byteBuf30, byteBuf38);
        org.apache.bookkeeper.util.ByteBufList byteBufList41 = org.apache.bookkeeper.util.ByteBufList.get(byteBuf5, byteBuf38);
        org.apache.bookkeeper.util.ByteBufList byteBufList42 = org.apache.bookkeeper.util.ByteBufList.get(byteBuf5);
        int int43 = byteBufList42.refCnt();
        int int44 = byteBufList42.readableBytes();
        org.apache.bookkeeper.util.ByteBufList byteBufList45 = org.apache.bookkeeper.util.ByteBufList.get();
        int int46 = byteBufList45.readableBytes();
        int int47 = byteBufList45.refCnt();
        boolean boolean48 = byteBufList45.hasArray();
        io.netty.util.ReferenceCounted referenceCounted50 = byteBufList45.touch((java.lang.Object) '#');
        int int51 = byteBufList45.refCnt();
        java.lang.Class<?> wildcardClass52 = byteBufList45.getClass();
        io.netty.util.ReferenceCounted referenceCounted53 = byteBufList42.touch((java.lang.Object) wildcardClass52);
        int int54 = byteBufList42.arrayOffset();
        int int55 = byteBufList42.refCnt();
        org.junit.Assert.assertNotNull(byteBufList0);
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 0 + "'", int1 == 0);
        org.junit.Assert.assertNotNull(referenceCounted3);
        org.junit.Assert.assertNotNull(byteBufList4);
        org.junit.Assert.assertNotNull(byteBuf5);
        org.junit.Assert.assertNotNull(byteBufList7);
        org.junit.Assert.assertNotNull(byteBufList8);
        org.junit.Assert.assertTrue("'" + int9 + "' != '" + 0 + "'", int9 == 0);
        org.junit.Assert.assertNotNull(referenceCounted11);
        org.junit.Assert.assertNotNull(byteBufList12);
        org.junit.Assert.assertNotNull(byteBuf13);
        org.junit.Assert.assertNotNull(byteBufList15);
        org.junit.Assert.assertNotNull(byteBufList16);
        org.junit.Assert.assertTrue("'" + int17 + "' != '" + 0 + "'", int17 == 0);
        org.junit.Assert.assertNotNull(referenceCounted19);
        org.junit.Assert.assertNotNull(byteBufList20);
        org.junit.Assert.assertNotNull(byteBuf21);
        org.junit.Assert.assertNotNull(byteBufList23);
        org.junit.Assert.assertNotNull(byteBufList24);
        org.junit.Assert.assertNotNull(byteBufList25);
        org.junit.Assert.assertTrue("'" + int26 + "' != '" + 0 + "'", int26 == 0);
        org.junit.Assert.assertNotNull(referenceCounted28);
        org.junit.Assert.assertNotNull(byteBufList29);
        org.junit.Assert.assertNotNull(byteBuf30);
        org.junit.Assert.assertNotNull(byteBufList32);
        org.junit.Assert.assertNotNull(byteBufList33);
        org.junit.Assert.assertTrue("'" + int34 + "' != '" + 0 + "'", int34 == 0);
        org.junit.Assert.assertNotNull(referenceCounted36);
        org.junit.Assert.assertNotNull(byteBufList37);
        org.junit.Assert.assertNotNull(byteBuf38);
        org.junit.Assert.assertNotNull(byteBufList40);
        org.junit.Assert.assertNotNull(byteBufList41);
        org.junit.Assert.assertNotNull(byteBufList42);
        org.junit.Assert.assertTrue("'" + int43 + "' != '" + 1 + "'", int43 == 1);
        org.junit.Assert.assertTrue("'" + int44 + "' != '" + 0 + "'", int44 == 0);
        org.junit.Assert.assertNotNull(byteBufList45);
        org.junit.Assert.assertTrue("'" + int46 + "' != '" + 0 + "'", int46 == 0);
        org.junit.Assert.assertTrue("'" + int47 + "' != '" + 1 + "'", int47 == 1);
        org.junit.Assert.assertTrue("'" + boolean48 + "' != '" + false + "'", boolean48 == false);
        org.junit.Assert.assertNotNull(referenceCounted50);
        org.junit.Assert.assertTrue("'" + int51 + "' != '" + 1 + "'", int51 == 1);
        org.junit.Assert.assertNotNull(wildcardClass52);
        org.junit.Assert.assertNotNull(referenceCounted53);
        org.junit.Assert.assertTrue("'" + int54 + "' != '" + 0 + "'", int54 == 0);
        org.junit.Assert.assertTrue("'" + int55 + "' != '" + 1 + "'", int55 == 1);
    }
}

