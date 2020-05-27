package com.stolsvik.mats.websocket.impl;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Trick to make ThreadPoolExecutor work as anyone in the world would expect: Have a constant pool of "corePoolSize",
 * and then as more tasks are concurrently running than threads available, you increase the number of threads until
 * "maxPoolSize", at which point the rest go on queue.
 *
 * Snitched from <a href="https://stackoverflow.com/a/24493856">https://stackoverflow.com/a/24493856</a>.
 *
 * @author Endre Stølsvik 2020-05-21 23:46 - http://stolsvik.com/, endre@stolsvik.com
 */
public class SaneThreadPoolExecutor implements Executor, MatsSocketStatics {

    private final ThreadPoolExecutor _threadPool;
    private final AtomicInteger _threadNumber = new AtomicInteger();

    SaneThreadPoolExecutor(int corePoolSize, int maxPoolSize, String threadTypeName, String serverId) {
        // Part 1: So, we extend a LinkedTransferQueue to behave a bit special on "offer(..)":
        LinkedTransferQueue<Runnable> runQueue = new LinkedTransferQueue<Runnable>() {
            @Override
            public boolean offer(Runnable e) {
                // If there are any pool thread waiting for job, give it the job, otherwise return false.
                // The TPE interprets false as "no more room on queue", so it rejects it. (cont'd on part 2)
                return tryTransfer(e);
            }
        };
        _threadPool = new ThreadPoolExecutor(corePoolSize, maxPoolSize,
                5L, TimeUnit.MINUTES, runQueue,
                r -> new Thread(r, THREAD_PREFIX + threadTypeName + "#"
                        + _threadNumber.getAndIncrement() + " {" + serverId + '}'));

        // Part 2: We make a special RejectionExecutionHandler ...
        _threadPool.setRejectedExecutionHandler((r, executor) -> {
            // ... which upon rejection due to "full queue" puts the task on queue nevertheless
            // (LinkedTransferQueue is not bounded).
            ((LinkedTransferQueue<Runnable>) _threadPool.getQueue()).put(r);
        });

    }

    @Override
    public void execute(Runnable command) {
        _threadPool.execute(command);
    }

    public void shutdownNice(int gracefulShutdownMillis) {
        _threadPool.shutdown();
        try {
            _threadPool.awaitTermination(gracefulShutdownMillis, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            // Just re-set interrupted flag, and go on exiting.
            Thread.currentThread().interrupt();
        }
        _threadPool.shutdownNow();
    }
}
