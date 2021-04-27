package org.apache.crail.storage.tcp;

import org.apache.crail.CrailBuffer;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.memory.OffHeapBuffer;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.storage.StorageEndpoint;
import org.apache.crail.storage.StorageFuture;
import org.apache.crail.storage.StorageUtils;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;
import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.ConcurrentHashMap;


public class TcpStorageLocalEndpoint implements StorageEndpoint {

    private static final Logger LOG = CrailUtils.getLogger();
    private ConcurrentHashMap<Long, CrailBuffer> bufferMap;
    private Unsafe unsafe;
    private InetSocketAddress address;
    
    public TcpStorageLocalEndpoint(InetSocketAddress datanodeAddr) throws IOException {
        LOG.info("new local endpoint for address " + datanodeAddr);
        String dataPath = StorageUtils.getDatanodeDirectory(TcpStorageConstants.STORAGE_TCP_DATA_PATH, datanodeAddr);
        File dataDir = new File(dataPath);
        if (!dataDir.exists()){
            throw new IOException("Local TCP data path missing");
        }
        this.address = datanodeAddr;
        this.bufferMap = new ConcurrentHashMap<Long, CrailBuffer>();
        try {
            this.unsafe = getUnsafe();
        } catch(Exception e) {
            throw new IOException("unable to create Unsafe");
        }
        for (File dataFile : dataDir.listFiles()) {
            long lba = Long.parseLong(dataFile.getName());
            OffHeapBuffer offHeapBuffer = OffHeapBuffer.wrap(mmap(dataFile));
            bufferMap.put(lba, offHeapBuffer);
        }
    }

    private MappedByteBuffer mmap(File file) throws IOException{
        RandomAccessFile randomFile = new RandomAccessFile(file.getAbsolutePath(), "rw");
        FileChannel channel = randomFile.getChannel();
        MappedByteBuffer mappedBuffer = channel.map(MapMode.READ_WRITE, 0, CrailConstants.REGION_SIZE);
        randomFile.close();
        return mappedBuffer;
    }

    private static long getAlignedLba(long remoteLba){
        return remoteLba / CrailConstants.REGION_SIZE;
    }

    private static long getLbaOffset(long remoteLba){
        return remoteLba % CrailConstants.REGION_SIZE;
    }

    private Unsafe getUnsafe() throws Exception {
        Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
        theUnsafe.setAccessible(true);
        Unsafe unsafe = (Unsafe) theUnsafe.get(null);
        return unsafe;
    }

    
    public StorageFuture write(CrailBuffer buffer, BlockInfo remoteMr, long remoteOffset) throws IOException,
            InterruptedException {
        if (buffer.remaining() > CrailConstants.BLOCK_SIZE){
            throw new IOException("write size too large " + buffer.remaining());
        }
        if (buffer.remaining() <= 0){
            throw new IOException("write size too small, len " + buffer.remaining());
        }
        if (remoteOffset < 0){
            throw new IOException("remote offset too small " + remoteOffset);
        }

        long alignedLba = getAlignedLba(remoteMr.getLba());
        long lbaOffset = getLbaOffset(remoteMr.getLba());

        CrailBuffer mappedBuffer = bufferMap.get(alignedLba);
        if (mappedBuffer == null){
            throw new IOException("No mapped buffer for this key " + remoteMr.getLkey() + ", address " + address);
        }

        if (lbaOffset + remoteOffset + buffer.remaining() > CrailConstants.REGION_SIZE){
            long tmpAddr = lbaOffset + remoteOffset + buffer.remaining();
            throw new IOException("remote fileOffset + remoteOffset + len too large " + tmpAddr);
        }
        long srcAddr = buffer.address() + buffer.position();
        long dstAddr = mappedBuffer.address() + lbaOffset + remoteOffset;
        TcpLocalFuture future = new TcpLocalFuture(unsafe, srcAddr, dstAddr, buffer.remaining());
        return future;
    }
    
    public StorageFuture read(CrailBuffer buffer, BlockInfo remoteMr, long remoteOffset) throws IOException,
            InterruptedException {
        if (buffer.remaining() > CrailConstants.BLOCK_SIZE){
            throw new IOException("read size too large");
        }
        if (buffer.remaining() <= 0){
            throw new IOException("read size too small, len " + buffer.remaining());
        }
        if (remoteOffset < 0){
            throw new IOException("remote offset too small " + remoteOffset);
        }
        if (buffer.position() < 0){
            throw new IOException("local offset too small " + buffer.position());
        }

        long alignedLba = getAlignedLba(remoteMr.getLba());
        long lbaOffset = getLbaOffset(remoteMr.getLba());

        CrailBuffer mappedBuffer = bufferMap.get(alignedLba);
        if (mappedBuffer == null){
            throw new IOException("No mapped buffer for this key");
        }
        if (lbaOffset + remoteOffset + buffer.remaining() > CrailConstants.REGION_SIZE){
            long tmpAddr = lbaOffset + remoteOffset + buffer.remaining();
            throw new IOException("remote fileOffset + remoteOffset + len too large " + tmpAddr);
        }
        long srcAddr = mappedBuffer.address() + lbaOffset + remoteOffset;
        long dstAddr = buffer.address() + buffer.position();
        TcpLocalFuture future = new TcpLocalFuture(unsafe, srcAddr, dstAddr, buffer.remaining());
        return future;
    }

    @Override
    public void close() throws IOException, InterruptedException {
    }

    @Override
    public boolean isLocal() {
        return true;
    }
}
