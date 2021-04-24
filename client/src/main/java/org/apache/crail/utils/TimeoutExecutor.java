package org.apache.crail.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TimeoutExecutor {
    
    // was replaced by using selector.select(1000) in nio
    public static final ExecutorService executorService = Executors.newFixedThreadPool(1);

    TimeoutExecutor() {}
}
