package ringbufferondisk;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class RingBufferOnDiskTest
{
    public static final int DEFAULT_MAX_BYTES = 64;
    private RandomAccessFile fileBuffer;
    private RingBufferOnDisk ringBufferOnDisk;

    @Before
    public void setUp() throws Exception
    {
        fileBuffer = new RandomAccessFile("buffer.dat", "rw");
        ringBufferOnDisk = new RingBufferOnDisk(DEFAULT_MAX_BYTES, fileBuffer);
    }

    @After
    public void tearDown() throws Exception
    {
        fileBuffer.close();
    }

    @Test
    public void testSimpleAddAndGet() throws Exception
    {
        final byte expectedValue = 1;
        ringBufferOnDisk.add(expectedValue);
        assertEquals(expectedValue, ringBufferOnDisk.get(0));
    }

    @Test
    public void testGetMaxBytes() throws Exception
    {
        assertEquals(DEFAULT_MAX_BYTES, ringBufferOnDisk.capacity());
    }

    @Test
    public void testCount() throws Exception
    {
        for(int i = 0; i < DEFAULT_MAX_BYTES; i++)
        {
            assertEquals("Count should equal the number of bytes added so far",
                         i, ringBufferOnDisk.count());
            ringBufferOnDisk.add((byte) 1);
        }
        for(int i = 0; i < DEFAULT_MAX_BYTES; i++)
        {
            assertEquals("Count should equal the max number of bytes",
                         DEFAULT_MAX_BYTES, ringBufferOnDisk.count());
            ringBufferOnDisk.add((byte) 2);
        }
    }

    @Test
    public void testCircularAddAndGet() throws Exception
    {
        final byte expectedValue1 = 1;
        for(int i = 0; i < DEFAULT_MAX_BYTES; i++)
        {
            ringBufferOnDisk.add(expectedValue1);
            final String extraInfo = "Adding value: " + expectedValue1 + " failed at index: " + i;
            assertEquals(extraInfo, expectedValue1, ringBufferOnDisk.get(i));
        }

        // now add till we should only have a single old value left
        final byte expectedValue2 = 2;
        almostFillWithNewValue(ringBufferOnDisk, expectedValue1, expectedValue2);

        // and finally, repeat with a new value
        final byte expectedValue3 = 3;
        almostFillWithNewValue(ringBufferOnDisk, expectedValue2, expectedValue3);
    }

    @Test
    public void testCircularAddAndGetIncrementing() throws Exception
    {
        // add [0, DEFAULT_MAX_BYTES)
        for(int i = 0; i < DEFAULT_MAX_BYTES; i++)
        {
            ringBufferOnDisk.add(i);
        }
        for(int i = 0; i < DEFAULT_MAX_BYTES; i++)
        {
            assertEquals(i, ringBufferOnDisk.get(i));
        }

        // add [DEFAULT_MAX_BYTES, 2*DEFAULT_MAX_BYTES)
        for(int i = DEFAULT_MAX_BYTES; i < DEFAULT_MAX_BYTES * 2; i++)
        {
            ringBufferOnDisk.add(i);
        }
        for(int i = 0; i < DEFAULT_MAX_BYTES; i++)
        {
            assertEquals(DEFAULT_MAX_BYTES + i, ringBufferOnDisk.get(i));
        }
    }

    @Test
    public void testSendToOutputStream() throws Exception
    {
        // start out by writing an incrementing byte pattern
        for(int i = 0; i < DEFAULT_MAX_BYTES; i++)
        {
            ringBufferOnDisk.add(i);
        }
        testAllRanges(0);

        // now add items, testing all ranges each time
        for(int i = 0; i < DEFAULT_MAX_BYTES; i++)
        {
            ringBufferOnDisk.add(DEFAULT_MAX_BYTES + i);
            testAllRanges(i + 1);
        }
    }

    private void testAllRanges(final int rangePatternStartVal) throws IOException
    {
        for(int rangeStart = 0; rangeStart < DEFAULT_MAX_BYTES; rangeStart++)
        {
            for(int rangeSize = 0; rangeSize < DEFAULT_MAX_BYTES - rangeStart; rangeSize++)
            {
                testGetRange(rangeStart, rangeSize, rangePatternStartVal + rangeStart);
            }
        }
    }

    private void testGetRange(final long startOffset, final long count,
                              final int rangePatternStartVal) throws IOException
    {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ringBufferOnDisk.sendToOutputStream(outputStream, startOffset, count);
        outputStream.close();
        final byte[] readBytes = outputStream.toByteArray();
        if(count == 0)
        {
            final String assertInfo =
                "Should read no bytes for zero count. StartOffset = " + startOffset;
            assertTrue(assertInfo, readBytes.length == 0);
        }
        else
        {
            final String assertInfo =
                "No bytes read for startOffset, count = " + startOffset + ", " + count;
            assertTrue(assertInfo, readBytes.length > 0);
        }
        for(int i = 0; i < readBytes.length; i++)
        {
            final String extraInfo = "Wrong pattern value at index: " + i + " for count: " +
                count + " using the starting pattern of " + rangePatternStartVal;
            assertEquals(extraInfo, rangePatternStartVal + i, readBytes[i]);
        }
    }

    private void almostFillWithNewValue(final RingBufferOnDisk ringBufferOnDisk,
                                        final byte oldExpectedValue,
                                        final byte newExpectedValue) throws IOException
    {
        for(int i = 0; i < ringBufferOnDisk.capacity() - 1; i++)
        {
            ringBufferOnDisk.add(newExpectedValue);
            final String extraInfo =
                "Adding new value: " + newExpectedValue + " at index: " + i +
                    " should not change the first value because we're pushing less than" +
                    "a full buffer.";
            assertEquals(extraInfo, oldExpectedValue, ringBufferOnDisk.get(0));
        }

        // and now the old value should cycle out
        ringBufferOnDisk.add(newExpectedValue);
        assertNotEquals(oldExpectedValue, ringBufferOnDisk.get(0));
        assertEquals(newExpectedValue, ringBufferOnDisk.get(0));
    }
}