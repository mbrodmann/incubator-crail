package org.apache.crail.metadata;

import java.nio.ByteBuffer;

public class DataNodeStatus {
    public static final int CSIZE = 2;

    private short status;

    // Namenode tells Datanode to stop
    public static final short STATUS_DATANODE_STOP = 1;

    // Namenode tells Datanode to prepare for relocation
    public static final short STATUS_DATANODE_RELOCATION = 2;

    // Namenode tells Relocator that Datanode is not ready yet
    public static final short STATUS_DATANODE_PREPARING_RELOCATION = 3;

    // Namenode tells Relocator that Datanode is ready for relocation
    public static final short STATUS_DATANODE_READY_RELOCATION = 4;

    public DataNodeStatus() {
        this.status = 0;
    }

    public int write(ByteBuffer buffer) {
        buffer.putShort(status);
        return CSIZE;
    }

    public void update(ByteBuffer buffer) {
        this.status = buffer.getShort();
    }

    public short getStatus() {
        return this.status;
    }

    public void setStatus(short status) {
        this.status = status;
    }
}
