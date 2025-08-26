package org.apache.bookkeeper.util;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class ByteBufListEncoderTest {

    enum ExpectedResult { SUCCESS, FAILURE }

    @Parameterized.Parameter(0)
    public String description;

    @Parameterized.Parameter(1)
    public ChannelHandlerContext ctx;

    @Parameterized.Parameter(2)
    public Object msg;

    @Parameterized.Parameter(3)
    public ChannelPromise promise;

    @Parameterized.Parameter(4)
    public ExpectedResult expected;

    private ByteBufList.Encoder encoder;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        encoder = new ByteBufList.Encoder();
    }

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Object[][] data() {
        return new Object[][]{
                // --- msg null ---
                { "msg=null, ctx=mock, promise=mock", mock(ChannelHandlerContext.class), null, mock(ChannelPromise.class), ExpectedResult.SUCCESS },

                // --- msg valido ByteBufList ---
                { "msg=ByteBufList.get(), ctx=mock, promise=mock", mock(ChannelHandlerContext.class), ByteBufList.get(), mock(ChannelPromise.class), ExpectedResult.FAILURE },

                // --- msg mock ByteBufList ---
                { "msg=mock(ByteBufList)", mock(ChannelHandlerContext.class), mock(ByteBufList.class), mock(ChannelPromise.class), ExpectedResult.FAILURE },

                // --- msg oggetto di altro tipo ---
                { "msg=String, ctx=mock, promise=mock", mock(ChannelHandlerContext.class), "NotAByteBufList", mock(ChannelPromise.class), ExpectedResult.SUCCESS },

                // --- ctx null ---
                { "ctx=null, msg=ByteBufList.get()", null, ByteBufList.get(), mock(ChannelPromise.class), ExpectedResult.FAILURE },

                // --- promise null ---
                { "promise=null, msg=ByteBufList.get()", mock(ChannelHandlerContext.class), ByteBufList.get(), null, ExpectedResult.FAILURE }
        };
    }

    @Test
    public void testWrite() {
        ExpectedResult actual;
        try {
            encoder.write(ctx, msg, promise);
            actual = ExpectedResult.SUCCESS;
        } catch (Throwable t) {
            actual = ExpectedResult.FAILURE;
        }
        assertEquals("Case: " + description, expected, actual);
    }
}

