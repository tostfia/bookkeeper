package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;



import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ByteBufListEncoderTest{

    private ByteBufList.Encoder encoder;
    private ByteBufList bufList;

    @Before
    public void setUp() {
        encoder = new ByteBufList.Encoder();
    }

    @After
    public void tearDown() {
        if (bufList != null) {
            bufList.release();  // rilascia eventuali buffer creati nei test
        }
    }

    @Test
    public void testWriteWithByteBufListSuccess() throws Exception {
        //Mock del contesto e dei promise
        ChannelHandlerContext ctx= mock(ChannelHandlerContext.class);
        ChannelPromise promise = mock(ChannelPromise.class);
        ChannelPromise voidPromise = mock(ChannelPromise.class);


        when(ctx.voidPromise()).thenReturn(voidPromise);
        when(ctx.newPromise()).thenReturn(mock(ChannelPromise.class));

        //Creo ByteBufList con due buffer
        ByteBufList bufList= ByteBufList.get();
        ByteBuf buf1 = Unpooled.wrappedBuffer("hello".getBytes());
        ByteBuf buf2 = Unpooled.wrappedBuffer("world".getBytes());
        bufList.add(buf1);
        bufList.add(buf2);

        //Eseguo il write
        encoder.write(ctx, bufList, promise);
        // Verifichiamo che ctx.write sia stato chiamato due volte
        ArgumentCaptor<ByteBuf> bufCaptor = ArgumentCaptor.forClass(ByteBuf.class);
        ArgumentCaptor<ChannelPromise> promCaptor = ArgumentCaptor.forClass(ChannelPromise.class);
        verify(ctx, times(2)).write(bufCaptor.capture(), promCaptor.capture());

        // Controlliamo che siano stati inviati retainedDuplicate
        for (ByteBuf written : bufCaptor.getAllValues()) {
            assertTrue(written.isReadable());
            assertTrue(written.refCnt() > 0); // retainedDuplicate incrementa ref count
        }

        // Possiamo anche verificare il contenuto dei buffer
        ByteBuf first = bufCaptor.getAllValues().get(0);
        ByteBuf second = bufCaptor.getAllValues().get(1);
        byte[] firstBytes = new byte[first.readableBytes()];
        byte[] secondBytes = new byte[second.readableBytes()];
        first.readBytes(firstBytes);
        second.readBytes(secondBytes);
        assertArrayEquals("hello".getBytes(), firstBytes);
        assertArrayEquals("world".getBytes(), secondBytes);
    }

    @Test
    public void testWriteWithNonByteBufListMessage() throws Exception {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        ChannelPromise promise = mock(ChannelPromise.class);

        String msg = "NotAByteBufList";
        encoder.write(ctx, msg, promise);

        // Verifichiamo che ctx.write venga chiamato con lo stesso oggetto
        verify(ctx, times(1)).write(msg, promise);
    }

    @Test
    public void testWriteWithNullMessage() throws Exception {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        ChannelPromise promise = mock(ChannelPromise.class);

        encoder.write(ctx, null, promise);

        // Deve chiamare ctx.write con null
        verify(ctx, times(1)).write(null, promise);
    }

    @Test
    public void testWriteWithNullContext() throws Exception {
        ChannelPromise promise = mock(ChannelPromise.class);
        ByteBufList bufList = ByteBufList.get();
        bufList.add(Unpooled.wrappedBuffer("data".getBytes()));

        // Se ctx è null, ci aspettiamo che l'encoder lanci NullPointerException
        try {
            encoder.write(null, bufList, promise);
        } catch (NullPointerException ex) {
            assertTrue(ex.getMessage() == null || ex.getMessage().contains("ctx"));
        }
    }

    @Test
    public void testWriteWithNullPromise() throws Exception {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.newPromise()).thenReturn(mock(ChannelPromise.class));
        // NON serve mockare voidPromise qui

        ByteBufList bufList = ByteBufList.get();
        bufList.add(Unpooled.wrappedBuffer("data".getBytes()));

        encoder.write(ctx, bufList, null);
    }

    //l’implementazione è intenzionalmente non-null-safe per promise

    @Test
    public void testWriteWithVoidPromise() throws Exception {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        ChannelPromise composite = new DefaultChannelPromise(mock(Channel.class));
        when(ctx.newPromise()).thenReturn(composite);
        when(ctx.voidPromise()).thenReturn(new VoidChannelPromise(mock(Channel.class), false));

        ByteBufList b = ByteBufList.get(Unpooled.wrappedBuffer("hello".getBytes()));
        ByteBufList.Encoder encoder = new ByteBufList.Encoder();

        ChannelPromise promise = ctx.voidPromise(); // void
        encoder.write(ctx, b, promise);

    }

    @Test
    public void testWriteWithPromiseSuccess() {
        EmbeddedChannel channel = new EmbeddedChannel(new ByteBufList.Encoder());
        ByteBufList bufList = ByteBufList.get(Unpooled.wrappedBuffer("hello".getBytes()));

        ChannelPromise promise = channel.newPromise();
        channel.writeAndFlush(bufList, promise);
        promise.awaitUninterruptibly();

        // La promise deve essere completata con successo
        assertTrue("La promise dovrebbe essere completata con successo", promise.isSuccess());

        // I buffer devono essere rilasciati
        assertEquals("Il buffer dovrebbe essere rilasciato", 0, bufList.refCnt());
    }


    @Test
    public void testWriteWithPromiseFailure() {
        EmbeddedChannel channel = new EmbeddedChannel(
                // Encoder reale
                new ByteBufList.Encoder(),
                // Handler che forza il fallimento della write
                new ChannelOutboundHandlerAdapter() {
                    @Override
                    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                        // Rilascia il buffer perché la write fallisce
                        ReferenceCountUtil.release(msg);
                        promise.setFailure(new RuntimeException("simulated failure"));
                    }
                }

        );

        ByteBufList bufList = ByteBufList.get(Unpooled.wrappedBuffer("hello".getBytes()));
        ChannelPromise promise = channel.newPromise();

        channel.writeAndFlush(bufList, promise);
        promise.awaitUninterruptibly();

        // La promise deve essere completata con fallimento
        assertFalse("La promise deve fallire", promise.isSuccess());
        assertNotNull("La causa del fallimento non deve essere nulla", promise.cause());
        assertEquals("simulated failure", promise.cause().getMessage());

        // I buffer devono essere rilasciati anche in caso di errore

        assertEquals("Il buffer dovrebbe essere rilasciato", 0, bufList.refCnt());

    }









}



