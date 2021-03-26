package org.apache.crail.metadata;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class RelocationBlockInfo  extends BlockInfo {

    public static int CSIZE = BlockInfo.CSIZE + 18;

    private short isLast;
    private long capacity;
    private long fd;

    public RelocationBlockInfo() {}

    public RelocationBlockInfo(BlockInfo blockInfo, short isLast, long capacity, long fd) {
        this.setBlockInfo(blockInfo);
        this.isLast = isLast;
        this.capacity = capacity;
        this.fd = fd;
    }

    public int write(ByteBuffer buffer) {
        super.write(buffer);
        buffer.putShort(isLast);
        buffer.putLong(capacity);
        buffer.putLong(fd);
        return CSIZE;
    }

    public void update(ByteBuffer buffer) throws UnknownHostException {
        super.update(buffer);
        this.isLast = buffer.getShort();
        this.capacity = buffer.getLong();
        this.fd = buffer.getLong();
    }

    public short getIsLast() {
        return this.isLast;
    }

    public long getCapacity() {
        return this.capacity;
    }

    public long getFd() {
        return this.fd;
    }
}
