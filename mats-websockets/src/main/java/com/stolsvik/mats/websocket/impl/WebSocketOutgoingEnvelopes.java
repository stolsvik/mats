package com.stolsvik.mats.websocket.impl;

import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.ACK;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.ACK2;
import static com.stolsvik.mats.websocket.MatsSocketServer.MessageType.NACK;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEnvelopeWithMetaDto;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEnvelopeWithMetaDto.Direction;

/**
 * Handles async sending of not-"information bearing messages", that is, messages that are idempotent. These are ACK and
 * NACKs, ACK2s, PONGs, PUB, and incomingHandler-settled DENY, RESOLVE, REJECT, FORWARD or RETRY. For all of these, if
 * we cannot send it out due to WebSocket closed, we will upon reconnect of the session get the "causes" of them again
 * (for PUB, we will get a SUB specifying which message was the last it saw).
 *
 * @author Endre Stølsvik 2020-05-22 00:00 - http://stolsvik.com/, endre@stolsvik.com
 */
public class WebSocketOutgoingEnvelopes implements MatsSocketStatics {

    private static final Logger log = LoggerFactory.getLogger(WebSocketOutgoingEnvelopes.class);

    private final DefaultMatsSocketServer _matsSocketServer;

    private final SaneThreadPoolExecutor _threadPool;

    private final ConcurrentHashMap<String, List<MatsSocketEnvelopeWithMetaDto>> _sessionIdToEnvelopes = new ConcurrentHashMap<>();

    private final ScheduledThreadPoolExecutor _scheduledExecutor;

    WebSocketOutgoingEnvelopes(DefaultMatsSocketServer defaultMatsSocketServer,
            int corePoolSize, int maxPoolSize) {
        _matsSocketServer = defaultMatsSocketServer;
        _threadPool = new SaneThreadPoolExecutor(corePoolSize, maxPoolSize, this.getClass().getSimpleName(),
                _matsSocketServer.serverId());
        log.info("Instantiated [" + this.getClass().getSimpleName() + "] for [" + _matsSocketServer.serverId()
                + "], ThreadPool:[" + _threadPool + "]");

        _scheduledExecutor = new MagicScheduledExecutor(NUMBER_OF_OUTGOING_ENVELOPES_SCHEDULER_THREADS);
    }

    void shutdown(int gracefulShutdownMillis) {
        log.info("Shutting down [" + this.getClass().getSimpleName() + "] for [" + _matsSocketServer.serverId()
                + "], Timer:[" + _scheduledExecutor + "], ThreadPool [" + _threadPool + "]");
        _scheduledExecutor.shutdown();
        _threadPool.shutdownNice(gracefulShutdownMillis);
    }

    /**
     * We need info about each task on the queue when we do scheduledExecutor.getQueue(), and this seems to be the only
     * way to ensure that. Read more in comments in {@link #dispatchEnvelopesForSession_OnThreadpool(String)}.
     */
    private static class MagicScheduledExecutor extends ScheduledThreadPoolExecutor {
        private static final AtomicInteger __threadId = new AtomicInteger();

        public MagicScheduledExecutor(int corePoolSize) {
            super(corePoolSize, r -> new Thread(r, THREAD_PREFIX + "OutgoingEnvelopesScheduler#"
                    + __threadId.getAndIncrement()));
        }

        @Override
        protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task) {
            return new MagicTask<>(runnable, task);
        }
    }

    /**
     * Annoyance-stuff, read {@link MagicScheduledExecutor}.
     */
    private static class MagicTask<V> implements RunnableScheduledFuture<V> {

        private final Runnable _runnable;
        private final RunnableScheduledFuture<V> _task;

        public MagicTask(Runnable runnable, RunnableScheduledFuture<V> task) {
            _runnable = runnable;
            _task = task;
        }

        public String getMatsSocketSessionId() {
            return ((MagicRunnable) _runnable).getMatsSocketSessionId();
        }

        @Override
        public boolean isPeriodic() {
            return _task.isPeriodic();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return _task.getDelay(unit);
        }

        @Override
        public int compareTo(Delayed o) {
            return _task.compareTo(o);
        }

        @Override
        public void run() {
            _task.run();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return _task.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return _task.isCancelled();
        }

        @Override
        public boolean isDone() {
            return _task.isDone();
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            return _task.get();
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return _task.get(timeout, unit);
        }
    }

    /**
     * Annoyance-stuff, read {@link MagicScheduledExecutor}.
     */
    private static class MagicRunnable implements Runnable {

        private final String _matsSocketSessionId;
        private final Runnable _runnable;

        public MagicRunnable(String matsSocketSessionId, Runnable runnable) {
            _matsSocketSessionId = matsSocketSessionId;
            _runnable = runnable;
        }

        @Override
        public void run() {
            _runnable.run();
        }

        public String getMatsSocketSessionId() {
            return _matsSocketSessionId;
        }
    }

    private void scheduleRun(String matsSocketSessionId, long delay, TimeUnit timeUnit) {
        // ?: Special handling for 0 delay (PONGs do this).
        if (delay == 0) {
            // -> Yes, zero delay - immediate dispatch
            dispatchEnvelopesForSession_OnThreadpool(matsSocketSessionId);
            return;
        }
        // E-> No, not zero delay - so send to timer.
        _scheduledExecutor.schedule(new MagicRunnable(matsSocketSessionId,
                () -> dispatchEnvelopesForSession_OnThreadpool(matsSocketSessionId)),
                delay, timeUnit);
    }

    void sendEnvelope(String matsSocketSessionId, MatsSocketEnvelopeWithMetaDto envelope, long delay,
            TimeUnit timeUnit) {
        // ConcurrentHashMap-atomically update the outstanding envelopes for the MatsSocketSessionId in question.
        _sessionIdToEnvelopes.compute(matsSocketSessionId, (key, currentEnvelopes) -> {
            if (currentEnvelopes == null) {
                currentEnvelopes = new ArrayList<>();
            }
            currentEnvelopes.add(envelope);
            return currentEnvelopes;
        });
        scheduleRun(matsSocketSessionId, delay, timeUnit);
    }

    void sendEnvelopes(String matsSocketSessionId, Collection<MatsSocketEnvelopeWithMetaDto> envelopes, long delay,
            TimeUnit timeUnit) {
        // ConcurrentHashMap-atomically update the outstanding envelopes for the MatsSocketSessionId in question.
        _sessionIdToEnvelopes.compute(matsSocketSessionId, (key, currentEnvelopes) -> {
            if (currentEnvelopes == null) {
                currentEnvelopes = new ArrayList<>();
            }
            currentEnvelopes.addAll(envelopes);
            return currentEnvelopes;
        });
        scheduleRun(matsSocketSessionId, delay, timeUnit);
    }

    public void removeAck(String matsSocketSessionId, String cmidAck) {
        // ConcurrentHashMap-atomically update the outstanding currentACKs for the MatsSocketSessionId in question.
        _sessionIdToEnvelopes.compute(matsSocketSessionId, (key, currentEnvelopes) -> {
            if (currentEnvelopes != null) {
                currentEnvelopes.removeIf(e -> (e.t == ACK) && (e.cmid.equals(cmidAck)));
                if (currentEnvelopes.isEmpty()) {
                    return null;
                }
            }
            return currentEnvelopes;
        });
        // NOT scheduling, since we /removed/ some messages.
    }

    void sendAck2s(String matsSocketSessionId, Collection<String> smidAck2s) {
        // ConcurrentHashMap-atomically update the outstanding currentACK2s for the MatsSocketSessionId in question.
        _sessionIdToEnvelopes.compute(matsSocketSessionId, (key, currentEnvelopes) -> {
            if (currentEnvelopes == null) {
                currentEnvelopes = new ArrayList<>();
            }
            for (String smidAck2 : smidAck2s) {
                MatsSocketEnvelopeWithMetaDto e = new MatsSocketEnvelopeWithMetaDto();
                e.t = ACK2;
                e.smid = smidAck2;
                currentEnvelopes.add(e);
            }
            return currentEnvelopes;
        });
        scheduleRun(matsSocketSessionId, 25, TimeUnit.MILLISECONDS);
    }

    private void dispatchEnvelopesForSession_OnThreadpool(String matsSocketSessionId) {
        /*
         * We are a task that shall send out any outgoing Envelopes waiting to be sent - we send everything pending with
         * this WebSocket message, even though those messages might have been scheduled to be sent later. Now, if there
         * currently are 50 such Envelopes, there might also be 50 ScheduledTasks in the ScheduledExecutor - possibly
         * spaced out somewhat in time. However, this particular run will clean out all these 50 Envelopes, so the other
         * 49 ScheduledTasks (since /this/ invocation is the first of them) will find an empty set of pending Envelopes.
         * Therefore, we wade through the Queue of the ScheduledExecutor, removing all other that concerns the same
         * MatsSocketSessionId as this task - their scheduling would have been meaningless.
         *
         * Make note: Only /after/ this removal of "same sessionId" ScheduledTasks, we get the Envelopes for this
         * MatsSocketSessionId.
         *
         * This is effectively "event coalescing" of outgoing sending.
         *
         * Discussion of why this cannot go wrong: Situation: Concurrently with running of this method, a 51st Envelope
         * is added, with some 75 ms delay for its ScheduledTask. Now, the situation will be one of these three:
         *
         * A. We find the 51st ScheduledTask (remove it), and then we pick up all 51 Envelopes: Good.
         *
         * B. We do NOT find the 51st ScheduledTask, but we DO find the 51st Envelope and send it - when the 51st
         * ScheduledTask is due, it will find an empty list of Envelopes, and do early exit: Good.
         *
         * C. We do NOT find the 51st ScheduledTask, and we do NOT find the 51st Envelope - when the 51st ScheduledTask
         * is due, it will find the single Envelope and send it: Good
         *
         * Note that the "fourth type" can NEVER occur: We DO find the 51st ScheduledTask (and remove it), but do NOT
         * find the 51st Envelope and only send 50, leaving the last one. Since we removed the 51st ScheduledTask, there
         * are no tasks that would clear it out - this would have been bad. HOWEVER, this situation CANNOT occur, since
         * there is a clear happens-before edge between the clearing of ScheduledTasks and clearing of Messages: We can
         * ONLY find (and send) LATER ADDED messages than we find ScheduledTasks, thus if we leave something out, it
         * will be a ScheduledTask that will then potentially find nothing to do.
         */
        BlockingQueue<Runnable> queue = _scheduledExecutor.getQueue();
        for (Iterator<Runnable> it = queue.iterator(); it.hasNext();) {
            MagicTask<?> t = (MagicTask<?>) it.next();
            if (t.getMatsSocketSessionId().equals(matsSocketSessionId)) {
                it.remove();
            }
        }

        // Pick out all Envelopes to send, do this ConcurrentHashMap-atomically:
        // (Any concurrently added Envelopes will either be picked up in this round, or in the next scheduled task)
        Object[] envelopes_x = new Object[] { null };
        _sessionIdToEnvelopes.compute(matsSocketSessionId, (key, currentEnvelopes) -> {
            envelopes_x[0] = currentEnvelopes;
            return null;
        });

        @SuppressWarnings("unchecked")
        List<MatsSocketEnvelopeWithMetaDto> envelopeList = (List<MatsSocketEnvelopeWithMetaDto>) envelopes_x[0];

        // ---- Gotten-and-cleared, now check whether there was anything there

        // ?: Was there anything to send?
        if ((envelopeList == null) || envelopeList.isEmpty()) {
            // -> Nothing to send, no use in dispatching this to the worker thread pool.
            return;
        }

        // ?: Check whether we still have the MatsSocketSession locally.
        Optional<MatsSocketSessionAndMessageHandler> session = _matsSocketServer.getRegisteredLocalMatsSocketSession(
                matsSocketSessionId);
        if (!session.isPresent()) {
            // -> The session is gone, nothing we can do.
            // If the session comes back, it will have to send the causes for these Envelopes again.
            log.info("When about to send [" + envelopeList.size() + "] Envelopes for MatsSocketSession ["
                    + matsSocketSessionId + "], the session was gone. Ignoring, if the session comes back, he will"
                    + " send the causes for these Envelopes again.");
            return;
        }

        // Dispatch sending on thread pool
        _threadPool.execute(() -> actualSendOfEnvelopes(session.get(), envelopeList));
    }

    private void actualSendOfEnvelopes(MatsSocketSessionAndMessageHandler session,
            List<MatsSocketEnvelopeWithMetaDto> envelopeList) {

        try { // try-finally: MDC.clear()
            session.setMDC();
            // :: Do "ACK/NACK/ACK2 compaction"
            List<String> acks = null;
            List<String> nacks = null;
            List<String> ack2s = null;
            for (Iterator<MatsSocketEnvelopeWithMetaDto> it = envelopeList.iterator(); it.hasNext();) {
                MatsSocketEnvelopeWithMetaDto envelope = it.next();
                if (envelope.t == ACK && envelope.desc == null) {
                    it.remove();
                    acks = acks != null ? acks : new ArrayList<>();
                    acks.add(envelope.cmid);
                }
                if (envelope.t == NACK && envelope.desc == null) {
                    it.remove();
                    nacks = nacks != null ? nacks : new ArrayList<>();
                    nacks.add(envelope.cmid);
                }
                if (envelope.t == ACK2 && envelope.desc == null) {
                    it.remove();
                    ack2s = ack2s != null ? ack2s : new ArrayList<>();
                    ack2s.add(envelope.smid);
                }
            }
            if (acks != null && acks.size() > 0) {
                MatsSocketEnvelopeWithMetaDto e_acks = new MatsSocketEnvelopeWithMetaDto();
                e_acks.t = ACK;
                if (acks.size() == 1) {
                    e_acks.cmid = acks.get(0);
                }
                else {
                    e_acks.ids = acks;
                }
                envelopeList.add(e_acks);
            }
            if (nacks != null && nacks.size() > 0) {
                MatsSocketEnvelopeWithMetaDto e_nacks = new MatsSocketEnvelopeWithMetaDto();
                e_nacks.t = NACK;
                if (nacks.size() == 1) {
                    e_nacks.cmid = nacks.get(0);
                }
                else {
                    e_nacks.ids = nacks;
                }
                envelopeList.add(e_nacks);
            }
            if (ack2s != null && ack2s.size() > 0) {
                MatsSocketEnvelopeWithMetaDto e_ack2s = new MatsSocketEnvelopeWithMetaDto();
                e_ack2s.t = ACK2;
                if (ack2s.size() == 1) {
                    e_ack2s.smid = ack2s.get(0);
                }
                else {
                    e_ack2s.ids = ack2s;
                }
                envelopeList.add(e_ack2s);
            }

            // :: Send them
            try {
                String outgoingEnvelopesJson = _matsSocketServer.getEnvelopeListObjectWriter()
                        .writeValueAsString(envelopeList);
                session.webSocketSendText(outgoingEnvelopesJson);
            }
            catch (IOException | RuntimeException e) { // Also RuntimeException, since Jetty throws WebSocketException
                // I believe IOExceptions here to be utterly final - the session is gone. So just ignore this.
                // If he comes back, he will have to send the causes for these Envelopes again.
                log.info("When trying to send [" + envelopeList.size() + "] Envelopes for MatsSocketSession ["
                        + session.getMatsSocketSessionId() + "], we got IOException. Assuming session is gone."
                        + " Ignoring, if the session comes back, he will send the causes for these Envelopes again.",
                        e);
                return;
            }

            // :: Calculate roundtrip-time if incoming-nanos is set, then null incoming-nanos.
            for (MatsSocketEnvelopeWithMetaDto e : envelopeList) {
                if (e.icnanos != null) {
                    e.rttm = msSince(e.icnanos);
                }
                e.icnanos = null;
            }

            // Record envelopes
            session.recordEnvelopes(envelopeList, System.currentTimeMillis(), Direction.S2C);
        }
        finally {
            MDC.clear();
        }
    }
}
