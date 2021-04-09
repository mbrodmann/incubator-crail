package org.apache.crail.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TimeoutExecutor {
    
    // TODO: check whether cachedThreadPool is better choice
    public static final ExecutorService executorService = Executors.newFixedThreadPool(16);

    TimeoutExecutor() {}
}
