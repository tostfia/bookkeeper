package org.apache.bookkeeper.util.integration;



import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class ByteBufListEncoderE2ETest {

    @Test
    public void testOutboundMessagesAreWrittenCorrectly() {
        // Creo un channel con l'encoder
        EmbeddedChannel channel = new EmbeddedChannel(new ByteBufList.Encoder());

        // Creo ByteBufList con 2 buffer
        ByteBuf b1 = Unpooled.wrappedBuffer("hello".getBytes(StandardCharsets.UTF_8));
        ByteBuf b2 = Unpooled.wrappedBuffer("world".getBytes(StandardCharsets.UTF_8));
        ByteBufList list = ByteBufList.get(b1, b2);

        // Scrivo ByteBufList sul canale
        assertTrue(channel.writeOutbound(list));
        assertTrue(channel.finish());

        // Leggo i messaggi scritti sul canale (uno per buffer)
        ByteBuf written1 = channel.readOutbound();
        ByteBuf written2 = channel.readOutbound();

        // Verifico il contenuto dei messaggi
        assertEquals("hello", written1.toString(StandardCharsets.UTF_8));
        assertEquals("world", written2.toString(StandardCharsets.UTF_8));

        // RefCnt deve essere corretto (rilasciati quando il channel chiude)
        written1.release();
        written2.release();

        assertNull("Non devono esserci altri messaggi", channel.readOutbound());
    }

    @Test
    public void testFailureReleasesBuffers() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new ByteBufList.Encoder(),
                new ChannelOutboundHandlerAdapter() {
                    @Override
                    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                        // Simulo il fallimento della write
                        ReferenceCountUtil.release(msg);
                        promise.setFailure(new RuntimeException("simulated failure"));
                    }
                }
        );

        ByteBufList list = ByteBufList.get(Unpooled.wrappedBuffer("hello".getBytes(StandardCharsets.UTF_8)));

        // Usiamo writeOneOutbound invece di writeOutbound, così otteniamo un ChannelFuture
        ChannelFuture future = channel.writeOneOutbound(list);
        future.awaitUninterruptibly();

        // La write deve fallire
        assertFalse("La future deve fallire", future.isSuccess());
        assertNotNull("La causa del fallimento non deve essere nulla", future.cause());
        assertEquals("simulated failure", future.cause().getMessage());

        // Il buffer deve essere rilasciato anche in caso di errore
        assertEquals("Il buffer dovrebbe essere rilasciato", 0, list.refCnt());
    }

}
