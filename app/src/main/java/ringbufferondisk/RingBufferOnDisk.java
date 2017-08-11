package ringbufferondisk;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

/**
 * This class implements a ring/circular streamingBuffer for a fixed number of bytes
 * that doesn't use any RAM. Instead, it operates direct to file.
 */
public class RingBufferOnDisk
{
    private final long maxBytes;
    private final RandomAccessFile fileBuffer;
    final byte[] streamingBuffer;
    long currentByteOffset = 0;
    long bytesAdded = 0; // after the initial phase of filling the streamingBuffer, this will be == capacity()

    RingBufferOnDisk(final long maxBytes,
                     final RandomAccessFile fileBuffer,
                     final int streamBufferSize) throws IOException
    {
        if(maxBytes < 0)
        {
            throw new IllegalArgumentException();
        }
        this.maxBytes = maxBytes;
        this.fileBuffer = fileBuffer;
        this.fileBuffer.setLength(maxBytes);
        this.fileBuffer.seek(0L);
        streamingBuffer = new byte[streamBufferSize];
    }

    public void add(final int valToAdd) throws IOException
    {
        fileBuffer.write(valToAdd);
        bytesAdded = Math.min(bytesAdded + 1, maxBytes);
        currentByteOffset++;
        if(currentByteOffset == maxBytes)
        {
            loopBackToStart();
        }
    }

    public void add(final byte[] valsToAdd) throws IOException
    {
        long endFileOffset = currentByteOffset + valsToAdd.length;
        if(endFileOffset > capacity())
        {
            final int numBytesInFirstWrite = (int) (capacity() - currentByteOffset);
            fileBuffer.write(valsToAdd, 0, numBytesInFirstWrite);
            loopBackToStart();
            final int numBytesInSecondWrite = valsToAdd.length - numBytesInFirstWrite;
            fileBuffer.write(valsToAdd, numBytesInFirstWrite, numBytesInSecondWrite);
            currentByteOffset = numBytesInSecondWrite;
        }
        else
        {
            fileBuffer.write(valsToAdd, 0, valsToAdd.length);
            currentByteOffset = endFileOffset;
        }
    }

    /**
     * @param offset the offset from the start of the ring streamingBuffer. Should be <= {@link #capacity()}. If not, it will be clamped.
     * @return the byte at the given offset
     */
    public int get(final long offset) throws IOException
    {
        if((offset < 0) ||
            (offset >= count()))
        {
            throw new IllegalArgumentException("Offset: " + offset + ". Count: " + count());
        }

        final long fileOffset = calcFileOffset(offset);
        return getFromFile(fileOffset);
    }

    /**
     * This method sends data straight from the circular streamingBuffer on disk
     * straight to an OutputStream without needing to use large amounts
     * of RAM as an intermediate step. The OutputStream can be optionally
     * buffered, of course.
     *
     * @param outputStream the destination for the data read
     * @param offset offset for first data item to read
     * @param numBytesToSend number of bytes to read. The actual number read is
     * Math.min(numBytesToSend, {@link #count()})
     * @return The number of bytes actually sent
     */
    public long sendToOutputStream(final OutputStream outputStream,
                                   final long offset,
                                   final long numBytesToSend) throws IOException
    {
        // to improve efficiency, we want to write as much data in one go as possible
        final long startFileOffset = calcFileOffset(offset);
        final long endFileOffset = calcFileOffset(offset + numBytesToSend);
        final long actualBytesToSend = Math.min(numBytesToSend, count());
        if(endFileOffset < startFileOffset)
        {
            // index has looped around in a full streamingBuffer
            streamRange(outputStream, startFileOffset, capacity());
            streamRange(outputStream, 0, endFileOffset);
        }
        else
        {
            streamRange(outputStream, startFileOffset, endFileOffset);
        }
        return actualBytesToSend;
    }

    /**
     * Streams data in the range [startFileOffset, endF) through a small streamingBuffer from
     * the RandomAccessFile to the OutputStream.
     *
     * @param outputStream The destination stream
     * @param startFileOffset the offset first byte of data to send
     * @param endFileOffset the offset just after the last byte to send
     */
    private void streamRange(final OutputStream outputStream,
                             final long startFileOffset,
                             final long endFileOffset) throws IOException
    {
        if(endFileOffset < startFileOffset)
        {
            throw new IllegalArgumentException(
                "Can only stream forwards from the file to output. " +
                    "To stream from a circular streamingBuffer, call " +
                    "streamRange() twice for the ranges " +
                    "[start, capacity) and [0, endOffset)");
        }
        if(endFileOffset > capacity())
        {
            throw new IndexOutOfBoundsException("End offset past the end of the file");
        }

        long bytesLeftToCopy = (endFileOffset - startFileOffset);
        long currentReadOffset = startFileOffset;
        while(bytesLeftToCopy > 0)
        {
            final int bytesRead =
                getFromFile(currentReadOffset, streamingBuffer,
                            (int) Math.min(bytesLeftToCopy, streamingBuffer.length));
            if(bytesRead == -1)
            {
                throw new IndexOutOfBoundsException("Streaming read past the end of the file");
            }
            else
            {
                outputStream.write(streamingBuffer, 0, bytesRead);
                bytesLeftToCopy -= bytesRead;
                currentReadOffset += bytesRead;
            }
        }
    }

    public long capacity()
    {
        return maxBytes;
    }

    public long count()
    {
        return bytesAdded;
    }

    void loopBackToStart() throws IOException
    {
        currentByteOffset = 0;
        fileBuffer.seek(0L);
    }

    private long calcFileOffset(final long logicalOffset)
    {
        if(count() < capacity())
        {
            return logicalOffset;
        }
        else
        {
            // the logical "zero" index is the item just after the tail
            final long offsetInFile = (currentByteOffset + logicalOffset) % capacity();
            return offsetInFile;
        }
    }

    private int getFromFile(final long offsetInFile) throws IOException
    {
        final long savedFilePos = currentByteOffset;
        fileBuffer.seek(offsetInFile);
        final int readVal = fileBuffer.read();
        fileBuffer.seek(savedFilePos);
        return readVal;
    }

    private int getFromFile(final long offsetInFile,
                            final byte[] dest,
                            final int numBytesToRead) throws IOException
    {
        final long savedFilePos = currentByteOffset;
        fileBuffer.seek(offsetInFile);
        final int numBytesRead = fileBuffer.read(dest, 0, Math.min(numBytesToRead, dest.length));
        fileBuffer.seek(savedFilePos);
        return numBytesRead;
    }
}
