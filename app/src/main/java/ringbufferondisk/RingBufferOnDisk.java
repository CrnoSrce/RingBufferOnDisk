package ringbufferondisk;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

/**
 * This class implements a ring/circular buffer for a fixed number of bytes
 * that doesn't use any RAM. Instead, it operates direct to file.
 */
public class RingBufferOnDisk
{
    private final long maxBytes;
    private final RandomAccessFile fileBuffer;
    long currentByteOffset = 0;
    long bytesAdded = 0; // after the initial phase of filling the buffer, this will be == capacity()

    RingBufferOnDisk(final long maxBytes,
                     final RandomAccessFile fileBuffer) throws IOException
    {
        if(maxBytes < 0)
        {
            throw new IllegalArgumentException();
        }
        this.maxBytes = maxBytes;
        this.fileBuffer = fileBuffer;
        this.fileBuffer.setLength(maxBytes);
        this.fileBuffer.seek(0L);
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

    /**
     * @param offset the offset from the start of the ring buffer. Should be <= {@link #capacity()}. If not, it will be clamped.
     * @return the byte at the given offset
     */
    public int get(final long offset) throws IOException
    {
        if((offset < 0) ||
            (offset >= count()))
        {
            throw new IllegalArgumentException();
        }

        if(count() < capacity())
        {
            return getFromFile(offset);
        }
        else
        {
            final long offsetInFile = (currentByteOffset + offset) % capacity();
            return getFromFile(offsetInFile);
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

    /**
     * This method sends data straight from the circular buffer on disk
     * straight to an OutputStream without needing to use large amounts
     * of RAM as an intermediate step.
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
        final long actualBytesToSend = Math.min(numBytesToSend, count());
        final long endOffset = offset + actualBytesToSend;
        for(long i = offset; i < endOffset; i++)
        {
            final int currentVal = get(i);
            outputStream.write(currentVal);
        }
        return actualBytesToSend;
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
}
