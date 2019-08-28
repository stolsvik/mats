package com.stolsvik.mats.util;

import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.MatsEndpoint.DetachedProcessContext;
import com.stolsvik.mats.MatsEndpoint.MatsObject;
import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsEndpoint.ProcessTerminatorLambda;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsInitiator.InitiateLambda;

/**
 * Note: WIP.
 *
 * @author Endre Stølsvik 2019-08-25 20:35 - http://stolsvik.com/, endre@stolsvik.com
 */
public class MatsFuturizer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MatsFuturizer.class);
    private String LOG_PREFIX = "#MATS-UTIL# ";

    /**
     * Creates a MatsFuturizer, <b>and you should only need one per MatsFactory</b> (which again mostly means one per
     * application or micro-service or JVM).
     * 
     * @param matsFactory
     *            The underlying {@link MatsFactory} on which outgoing messages will be sent, and on which the receiving
     *            {@link MatsFactory#subscriptionTerminator(String, Class, Class, ProcessTerminatorLambda)
     *            SubscriptionTerminator} will be created.
     * @param endpointIdPrefix
     *            the first part of the endpointId, which typically should be some "class-like" construct denoting the
     *            service name, like "OrderService" or "InventoryService".
     * @return the {@link MatsFuturizer}, which is tied to a newly created
     *         {@link MatsFactory#subscriptionTerminator(String, Class, Class, ProcessTerminatorLambda)
     *         SubscriptionTerminator}.
     */
    public static MatsFuturizer createMatsFuturizer(MatsFactory matsFactory, String endpointIdPrefix) {
        return new MatsFuturizer(matsFactory, endpointIdPrefix);
    }

    protected final MatsFactory _matsFactory;
    protected final MatsInitiator _matsInitiator;
    protected final String _terminatorEndpointId;
    protected final MatsEndpoint<Void, String> _replyHandlerEndpoint;

    protected MatsFuturizer(MatsFactory matsFactory, String endpointIdPrefix) {
        _matsFactory = matsFactory;
        _matsInitiator = matsFactory.getDefaultInitiator();
        _terminatorEndpointId = endpointIdPrefix + ".private.Futurizer."
                + _matsFactory.getFactoryConfig().getNodename();
        _replyHandlerEndpoint = _matsFactory.subscriptionTerminator(_terminatorEndpointId, String.class,
                MatsObject.class,
                this::handleRepliesForPromises);
    }

    public static class Reply<T> {
        public final DetachedProcessContext context;
        public final T reply;
        public final long initiatedTimestamp;

        public Reply(DetachedProcessContext context, T reply, long initiatedTimestamp) {
            this.context = context;
            this.reply = reply;
            this.initiatedTimestamp = initiatedTimestamp;
        }
    }

    // TODO: Timeout thread.

    public <T> CompletableFuture<Reply<T>> futurizeInteractiveUnreliable(String traceId, String from, String to, int timeoutMillis,
            Class<T> replyClass, Object request) {
        return futurizeGeneric(traceId, from, to, timeoutMillis, replyClass, request,
                msg -> msg.interactive().nonPersistent());
    }

    public <T> CompletableFuture<Reply<T>> futurizeGeneric(String traceId, String from, String to, int timeoutMillis,
            Class<T> replyClass, Object request, InitiateLambda extraMessageInit) {
        Promise<T> promise = _createPromise(replyClass, timeoutMillis);
        _assertFuturizerRunning();
        _enqueuePromise(promise);
        _sendRequestToFulfillPromise(from, to, traceId, request, extraMessageInit, promise);
        return promise._future;
    }

    // ===== Internal classes and methods, can be overridden if you want to make a customized MatsFuturizer

    protected static class Promise<T> implements Comparable<Promise> {
        private final String _correlationId;
        private final long _initiatedTimestamp;
        private final long _timeoutTimestamp;
        private final Class<T> _replyClass;
        private final CompletableFuture<Reply<T>> _future;

        public Promise(String correlationId, long initiatedTimestamp, long timeoutTimestamp, Class<T> replyClass,
                CompletableFuture<Reply<T>> future) {
            _correlationId = correlationId;
            _initiatedTimestamp = initiatedTimestamp;
            _timeoutTimestamp = timeoutTimestamp;
            _replyClass = replyClass;
            _future = future;
        }

        @Override
        public int compareTo(Promise o) {
            // ?: Are timestamps equal?
            if (this._timeoutTimestamp == o._timeoutTimestamp) {
                // -> Yes, timestamps equal, so compare by correlationId.
                return this._correlationId.compareTo(o._correlationId);
            }
            // "signum", but zero is handled above.
            return this._timeoutTimestamp - o._timeoutTimestamp > 0 ? +1 : -1;
        }
    }

    protected <T> Promise<T> _createPromise(Class<T> replyClass, int timeoutMillis) {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("Timeout cannot be zero or negative [" + timeoutMillis + "].");
        }
        String correlationId = RandomString.randomCorrelationId();
        long timestamp = System.currentTimeMillis();
        CompletableFuture<Reply<T>> future = new CompletableFuture<>();
        return new Promise<>(correlationId, timestamp, timestamp + timeoutMillis, replyClass, future);
    }

    protected <T> void _enqueuePromise(Promise<T> promise) {
        synchronized (_correlationIdToPromiseMap) {
            // This is the lookup that the reply-handler uses to get to the promise from the correlationId.
            _correlationIdToPromiseMap.put(promise._correlationId, promise);
            // This is the priority queue that the timeouter-thread uses to get the next Promise to timeout.
            _timeoutSortedPromises.add(promise);
            // Notify the timeouter-thread that a new promise was entered, to re-evaluate "next to timeout".
            _correlationIdToPromiseMap.notifyAll();
        }
    }

    protected volatile boolean _replyHandlerEndpointStarted;

    protected void _assertFuturizerRunning() {
        // ?: Have we already checked that the reply endpoint is running?
        if (!_replyHandlerEndpointStarted) {
            // -> No, so wait for it to start now
            boolean started = _replyHandlerEndpoint.waitForStarted(30_000);
            // ?: Did it start?
            if (!started) {
                // -> No, so that's bad.
                throw new IllegalStateException("The Reply Handler SubscriptionTerminator Endpoint would not start.");
            }
            // Shortcut this question forever after.
            _replyHandlerEndpointStarted = true;
        }
    }

    protected <T> void _sendRequestToFulfillPromise(String from, String endpointId, String traceId, Object request,
            InitiateLambda extraMessageInit, Promise<T> promise) {
        _matsInitiator.initiateUnchecked(msg -> {
            // Stash in the standard stuff
            msg.traceId(traceId)
                    .from(from)
                    .to(endpointId)
                    .replyToSubscription(_terminatorEndpointId, promise._correlationId);
            // Stash up with any extra initialization stuff
            extraMessageInit.initiate(msg);
            // Do the request.
            msg.request(request);
        });
    }

    protected ThreadPoolExecutor _getThreadPool() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        return new ThreadPoolExecutor(availableProcessors, availableProcessors * 5,
                5L, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(), _getThreadFactory());
    }

    protected ThreadFactory _getThreadFactory() {
        AtomicInteger threadNumber = new AtomicInteger();
        return r -> new Thread(r, "MatsFuturizer completer #" + threadNumber.getAndIncrement());
    }

    // Synchronized on itself
    protected final HashMap<String, Promise<?>> _correlationIdToPromiseMap = new HashMap<>();
    // Synchronized on the HashMap above (i.e. both are synchronized on the HashMap).
    protected final PriorityQueue<Promise> _timeoutSortedPromises = new PriorityQueue<>();

    protected final ThreadPoolExecutor _threadPool = _getThreadPool();

    protected void handleRepliesForPromises(ProcessContext<Void> context, String correlationId,
            MatsObject replyObject) {
        // Immediately pick this out of the map & queue
        Promise promise;
        synchronized (_correlationIdToPromiseMap) {
            promise = _correlationIdToPromiseMap.remove(correlationId);
            if (promise != null) {
                _timeoutSortedPromises.remove(promise);
            }
        }
        // ?: Did we still have the Promise?
        if (promise == null) {
            // -> Promise gone, log on WARN and exit.
            log.warn(LOG_PREFIX + "Got reply for CorrelationId:[" + correlationId + "] from:[" + context
                    .getFromStageId() + "] with traceId:[" + context.getTraceId()
                    + "], but the Promise had timed out. It was sent [" + (System.currentTimeMillis()
                            - promise._initiatedTimestamp) + " ms] ago, while the timeout was ["
                    + (promise._timeoutTimestamp - promise._initiatedTimestamp) + " ms].");
            return;
        }

        // ----- We have Promise, and shall now fulfill it. Send off to pool thread.

        _threadPool.execute(() -> {
            try {
                uncheckedComplete(context, replyObject, promise);
            }
            catch (Throwable t) {
                log.error(LOG_PREFIX + "Got problems completing future for CorrelationId:[" + correlationId
                        + "] from:[" + context.getFromStageId() + "] with traceId:[" + context.getTraceId() + "]");
            }
        });
    }

    @SuppressWarnings("unchecked")
    protected void uncheckedComplete(ProcessContext<Void> context, MatsObject replyObject, Promise promise) {
        Object replyInReplyClass = replyObject.toClass(promise._replyClass);
        Reply<?> tReply = new Reply<>(context, replyInReplyClass, promise._initiatedTimestamp);
        promise._future.complete(tReply);
    }

    /**
     * Closes the MatsFuturizer. Notice: Spring will also notice this method if the MatsFuturizer is registered as a
     * <code>@Bean</code>, and will register it as a destroy method.
     */
    public void close() {
        log.info("MatsFuturizer.close() invoked: Shutting down future completion threadpool and timeout thread.");
        _threadPool.shutdown();
    }
}
