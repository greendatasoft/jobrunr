package org.jobrunr.server.degradation;

import org.jobrunr.utils.threadpool.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class CircuitBreaker {
    private static final int FAILURE_THRESHOLD = 5;
    private static final long COOLDOWN_MS = 60_000;

    private static final Logger LOGGER = LoggerFactory.getLogger(CircuitBreaker.class);

    private final int failureThreshold;
    private final long cooldownMs;
    private final CircuitBreakerHandler handler;

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicLong lastFailureTime = new AtomicLong(0);
    private final AtomicLong lastStateChangeTime = new AtomicLong(System.currentTimeMillis());
    private final ScheduledExecutorService recoveryExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("jobrunr-circuit-breaker-recovery", true));
    private final AtomicReference<ScheduledFuture<?>> recoveryFuture = new AtomicReference<>();

    public static CircuitBreaker createDefault(CircuitBreakerHandler handler) {
        return new CircuitBreaker(FAILURE_THRESHOLD, COOLDOWN_MS, handler);
    }

    public CircuitBreaker(int failureThreshold, long cooldownMs, CircuitBreakerHandler handler) {
        this.failureThreshold = failureThreshold;
        this.cooldownMs = cooldownMs;
        this.handler = handler;
    }

    public boolean canProceed() {
        return state.get() == State.CLOSED;
    }

    /**
     * Records a failure. Only <em>consecutive</em> failures open the circuit breaker: every successful run must call
     * {@link #recordSuccess()}, otherwise unrelated failures spread over hours or days accumulate and eventually stop
     * a perfectly healthy BackgroundJobServer.
     */
    public void recordFailure() {
        lastFailureTime.set(System.currentTimeMillis());

        if (failureCount.incrementAndGet() >= failureThreshold) {
            open();
        }
    }

    /**
     * Opens the circuit breaker immediately (e.g. when the BackgroundJobServer detected it is in an unrecoverable
     * state) so the {@link CircuitBreakerHandler} can stop and restart it after the cooldown period.
     */
    public void trip() {
        lastFailureTime.set(System.currentTimeMillis());
        failureCount.set(failureThreshold);
        open();
    }

    private void open() {
        if (state.compareAndSet(State.CLOSED, State.OPEN)) {
            lastStateChangeTime.set(System.currentTimeMillis());
            notifyStateChange(State.OPEN);
            scheduleRecovery();
        }
    }

    private void scheduleRecovery() {
        ScheduledFuture<?> future = recoveryExecutor.schedule(() -> {
            if (state.compareAndSet(State.OPEN, State.CLOSED)) {
                failureCount.set(0);
                lastStateChangeTime.set(System.currentTimeMillis());
                notifyStateChange(State.CLOSED);
            }
        }, cooldownMs, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> old = recoveryFuture.getAndSet(future);
        if (old != null) old.cancel(false);
    }

    /**
     * Cancels any pending recovery and silently resets the breaker to CLOSED.
     * Does not fire {@link CircuitBreakerHandler#onStateChange}. Intended to be
     * called from {@link org.jobrunr.server.BackgroundJobServer#start()} so a
     * manual (re)start always begins from a clean state.
     */
    public void reset() {
        ScheduledFuture<?> old = recoveryFuture.getAndSet(null);
        if (old != null) old.cancel(false);
        failureCount.set(0);
        state.set(State.CLOSED);
        lastStateChangeTime.set(System.currentTimeMillis());
    }

    public void recordSuccess() {
        failureCount.set(0);
    }

    public State getState() {
        return state.get();
    }

    public long getFailureCount() {
        return failureCount.get();
    }

    public long getLastFailureTime() {
        return lastFailureTime.get();
    }

    public long getLastStateChangeTime() {
        return lastStateChangeTime.get();
    }

    private void notifyStateChange(State newState) {
        if (handler != null) {
            try {
                handler.onStateChange(newState);
            } catch (Exception e) {
                LOGGER.warn("Exception in notifyStateChange method, msg: {}", e.getMessage(), e);
            }
        }
    }
}
