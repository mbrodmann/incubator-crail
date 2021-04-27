package org.apache.crail.storage.tcp;

import org.apache.crail.storage.RetryInfo;
import org.apache.crail.storage.StorageFuture;
import org.apache.crail.storage.StorageResult;
import sun.misc.Unsafe;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TcpLocalFuture implements StorageFuture, StorageResult {
    private Unsafe unsafe;
    private long srcAddr;
    private long dstAddr;
    private int remaining;

    private int len;
    private boolean isDone;

    public TcpLocalFuture(Unsafe unsafe, long srcAddr, long dstAddr, int remaining) {
        this.unsafe = unsafe;
        this.srcAddr = srcAddr;
        this.dstAddr = dstAddr;
        this.remaining = remaining;

        this.len = 0;
        this.isDone = false;
    }

    @Override
    public RetryInfo getRetryInfo() {
        // TODO
        return null;
    }

    @Override
    public void addRetryInfo(RetryInfo retryInfo) {
        // TODO
    }

    @Override
    public int getLen() {
        return len;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        if (!isDone){
            getDone();
        }
        return isDone;
    }

    @Override
    public StorageResult get() throws InterruptedException, ExecutionException {
        if (!isDone){
            getDone();
        }
        return this;
    }

    @Override
    public StorageResult get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (!isDone){
            getDone();
        }
        return this;
    }

    @Override
    public boolean isSynchronous() {
        return true;
    }

    void getDone(){
        unsafe.copyMemory(srcAddr, dstAddr, remaining);
        len = remaining;
        isDone = true;
    }
}
