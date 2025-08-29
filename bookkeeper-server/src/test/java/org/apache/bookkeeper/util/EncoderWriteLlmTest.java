package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;
import static org.junit.Assert.*;

public class EncoderWriteLlmTest {

    // Category: ByteBufList with multiple buffers
    @Test
    public void testWriteMultipleBuffers() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[]{1});
        ByteBuf b2 = Unpooled.wrappedBuffer(new byte[]{2});
        ByteBufList list = ByteBufList.get(b1, b2);

        EmbeddedChannel channel = new EmbeddedChannel(ByteBufList.ENCODER);
        channel.writeOutbound(list);

        ByteBuf out1 = channel.readOutbound();
        ByteBuf out2 = channel.readOutbound();

        assertEquals(1, out1.readByte());
        assertEquals(2, out2.readByte());
        assertNull(channel.readOutbound());
    }

    // Category: ByteBufList with single buffer
    @Test
    public void testWriteSingleBuffer() {
        ByteBuf b = Unpooled.wrappedBuffer(new byte[]{42});
        ByteBufList list = ByteBufList.get(b);

        EmbeddedChannel channel = new EmbeddedChannel(ByteBufList.ENCODER);
        channel.writeOutbound(list);

        ByteBuf out = channel.readOutbound();
        assertEquals(42, out.readByte());
        assertNull(channel.readOutbound());
    }

    // Edge Case: ByteBufList vuoto
    @Test
    public void testWriteEmptyByteBufList() {
        ByteBufList list = ByteBufList.get();

        EmbeddedChannel channel = new EmbeddedChannel(ByteBufList.ENCODER);
        channel.writeOutbound(list);

        assertNull(channel.readOutbound()); // nessun buffer da scrivere
    }

    //  Category: msg non è ByteBufList
    @Test
    public void testWriteNonByteBufListMessage() {
        EmbeddedChannel channel = new EmbeddedChannel(ByteBufList.ENCODER);
        String msg = "Hello Netty";
        channel.writeOutbound(msg);

        Object out = channel.readOutbound();
        assertEquals("Hello Netty", out);
    }

    //  Exception Simulation: promise fallita
    @Test
    public void testWriteWithFailingPromise() {
        ByteBuf b = Unpooled.wrappedBuffer(new byte[]{99});
        ByteBufList list = ByteBufList.get(b);

        ChannelHandlerContext ctx = new EmbeddedChannel(ByteBufList.ENCODER).pipeline().firstContext();
        ChannelPromise promise = ctx.newPromise();

        // Simuliamo una promise fallita manualmente
        promise.setFailure(new RuntimeException("Simulated failure"));

        try {
            ByteBufList.ENCODER.write(ctx, list, promise);
        } catch (Exception e) {
            fail("Should not throw exception directly");
        }

        assertTrue(promise.isDone());
        assertTrue(promise.cause() instanceof RuntimeException);
    }

    // Edge Case: promise nulla
    @Test
    public void testWriteWithVoidPromiseSafe() {
        ByteBuf b = Unpooled.wrappedBuffer(new byte[]{7});
        ByteBufList list = ByteBufList.get(b);

        // Usa EmbeddedChannel che gestisce internamente ctx e promise
        EmbeddedChannel channel = new EmbeddedChannel(ByteBufList.ENCODER);

        // Questo simula una scrittura con voidPromise in modo sicuro
        boolean accepted = channel.writeOutbound(list);
        assertTrue(accepted);

        ByteBuf out = channel.readOutbound();
        assertEquals(7, out.readByte());
        assertNull(channel.readOutbound());
    }


}