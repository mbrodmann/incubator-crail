package org.apache.crail.storage;

import org.apache.crail.CrailBuffer;
import org.apache.crail.core.CoreStream;
import org.apache.crail.core.CoreSubOperation;
import org.apache.crail.metadata.BlockInfo;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class TriggerCall implements Callable<StorageFuture> {

    StorageEndpoint endpoint;
    CoreSubOperation opDesc;
    CrailBuffer dataBuf;
    BlockInfo currentBlock;
    
    CoreStream stream;
    
    public TriggerCall(StorageEndpoint endpoint, CoreSubOperation opDesc, CrailBuffer dataBuf, BlockInfo currentBlock, CoreStream stream) {
        this.endpoint = endpoint;
        this.opDesc = opDesc;
        this.dataBuf = dataBuf;
        this.currentBlock = currentBlock;
        
        this.stream = stream;
    }

    public StorageFuture call() throws Exception {
        return stream.trigger(endpoint, opDesc, dataBuf, currentBlock);
    }

}
