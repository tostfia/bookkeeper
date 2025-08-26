package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class ByteBufListTest {

    enum TestMethod { GET2, GET1, CLONE, ADD, PREPEND, GET_BUFFER, GET_BYTES, COALESCE, TOUCH }
    enum ExpectedResult { SUCCESS, FAILURE }

    @Parameterized.Parameter(0)
    public TestMethod method;

    @Parameterized.Parameter(1)
    public String description;

    @Parameterized.Parameter(2)
    public Object param;

    @Parameterized.Parameter(3)
    public ExpectedResult expected;

    private ByteBufList bufList;

    @Before
    public void setUp() {
        bufList = ByteBufList.get();
    }

    @After
    public void tearDown() {
        bufList = null;
    }

    @Parameterized.Parameters(name = "{index}: method={0}, case={1}, expected={3}")
    public static Object[][] data() {
        return new Object[][] {
                // --- GET2 ---
                { TestMethod.GET2, "null,null", new Object[]{null, null}, ExpectedResult.SUCCESS },
                { TestMethod.GET2, "buf,null", new Object[]{mock(ByteBuf.class), null}, ExpectedResult.SUCCESS },
                { TestMethod.GET2, "buf,buf", new Object[]{mock(ByteBuf.class), mock(ByteBuf.class)}, ExpectedResult.SUCCESS },

                // --- GET1 ---
                { TestMethod.GET1, "null", null, ExpectedResult.SUCCESS },
                { TestMethod.GET1, "mockBuf", mock(ByteBuf.class), ExpectedResult.SUCCESS },

                // --- CLONE ---
                { TestMethod.CLONE, "null", null, ExpectedResult.FAILURE },
                { TestMethod.CLONE, "mockList", mock(ByteBufList.class), ExpectedResult.FAILURE },

                // --- ADD ---
                { TestMethod.ADD, "nullBuf", null, ExpectedResult.SUCCESS},
                { TestMethod.ADD, "mockBuf", mock(ByteBuf.class), ExpectedResult.SUCCESS},

                // --- PREPEND ---
                { TestMethod.PREPEND, "nullBuf", null, ExpectedResult.SUCCESS },
                { TestMethod.PREPEND, "mockBuf", mock(ByteBuf.class), ExpectedResult.SUCCESS },

                // --- GET_BUFFER ---
                { TestMethod.GET_BUFFER, "negativeIndex", -1, ExpectedResult.FAILURE },
                { TestMethod.GET_BUFFER, "zeroIndex", 0, ExpectedResult.FAILURE},
                { TestMethod.GET_BUFFER, "positiveIndex", 5, ExpectedResult.FAILURE },

                // --- GET_BYTES ---
                { TestMethod.GET_BYTES, "nullArray", null, ExpectedResult.SUCCESS },
                { TestMethod.GET_BYTES, "emptyArray", new byte[0], ExpectedResult.SUCCESS },
                { TestMethod.GET_BYTES, "array3", new byte[3], ExpectedResult.SUCCESS },

                // --- COALESCE ---
                { TestMethod.COALESCE, "null", null, ExpectedResult.FAILURE },
                { TestMethod.COALESCE, "mockList", mock(ByteBufList.class), ExpectedResult.FAILURE },

                // --- TOUCH ---
                { TestMethod.TOUCH, "nullHint", null, ExpectedResult.SUCCESS },
                { TestMethod.TOUCH, "stringHint", "hint", ExpectedResult.SUCCESS },
                { TestMethod.TOUCH, "intHint", 123, ExpectedResult.SUCCESS }
        };
    }

    @Test
    public void runTest() {
        ExpectedResult actual;
        try {
            switch (method) {
                case GET2:
                    Object[] two = (Object[]) param;
                    ByteBufList.get((ByteBuf) two[0], (ByteBuf) two[1]);
                    break;
                case GET1:
                    ByteBufList.get((ByteBuf) param);
                    break;
                case CLONE:
                    ByteBufList.clone((ByteBufList) param);
                    break;
                case ADD:
                    bufList.add((ByteBuf) param);
                    break;
                case PREPEND:
                    bufList.prepend((ByteBuf) param);
                    break;
                case GET_BUFFER:
                    bufList.getBuffer((int) param);
                    break;
                case GET_BYTES:
                    bufList.getBytes((byte[]) param);
                    break;
                case COALESCE:
                    ByteBufList.coalesce((ByteBufList) param);
                    break;
                case TOUCH:
                    bufList.touch(param);
                    break;
            }
            actual = ExpectedResult.SUCCESS;
        } catch (Throwable t) {
            actual = ExpectedResult.FAILURE;
        }

        assertEquals("Case: " + description, expected, actual);
    }
}
