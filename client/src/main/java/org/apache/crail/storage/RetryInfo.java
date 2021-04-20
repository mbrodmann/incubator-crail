package org.apache.crail.storage;

import org.apache.crail.CrailBuffer;
import org.apache.crail.core.CoreSubOperation;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.rpc.RpcConnection;
import org.apache.crail.rpc.RpcFuture;
import org.apache.crail.rpc.RpcGetBlock;

public class RetryInfo {

    CoreSubOperation subOperation;

    // buffer used for dataOp
    CrailBuffer dataBuf;

    // info for retrieving block
    long fd;
    long token;
    long position;
    long syncedCapacity;
    
    // stores updated block information after update
    BlockInfo updatedBlockInfo = null;

    // other fields
    RpcConnection namenodeClientRpc;

    public RetryInfo(RpcConnection clientRpc) {
        this.namenodeClientRpc = clientRpc;
    }

    public void setCoreSubOperation(long fd, long fileOffset, int bufferPosition, int len) throws Exception {
        this.subOperation = new CoreSubOperation(fd, fileOffset, bufferPosition, len);
    }

    public void setBuffer(CrailBuffer buff) {
        this.dataBuf = buff;
    }

    public void setBlockInfo(long fd, long token, long position, long syncedCapacity) {
        this.fd = fd;
        this.token = token;
        this.position = position;
        this.syncedCapacity = syncedCapacity;
    }
    
    public long getFd() {
        return this.fd;
    }

    public CoreSubOperation getSubOperation() {
        return this.subOperation;
    }

    public CrailBuffer getBuffer() {
        return this.dataBuf;
    }
    
    public BlockInfo getBlockInfo() {
        return this.updatedBlockInfo;
    }

    public BlockInfo retryLookup() {
        try {
            RpcFuture<RpcGetBlock> rpcFuture = namenodeClientRpc.getBlock(fd, token, position, syncedCapacity);
            BlockInfo block = rpcFuture.get().getBlockInfo();
            this.updatedBlockInfo = block;
            return block;
        } catch (Exception e) {
            System.out.println("Failed retry of dataOp ...");
            e.printStackTrace();
            return null;
        }
    }
}
