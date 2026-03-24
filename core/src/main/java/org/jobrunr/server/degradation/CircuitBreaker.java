package org.jobrunr.server.degradation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public static CircuitBreaker createDefault(CircuitBreakerHandler handler) {
    return new CircuitBreaker(FAILURE_THRESHOLD, COOLDOWN_MS, handler);
  }

  public CircuitBreaker(int failureThreshold, long cooldownMs, CircuitBreakerHandler handler) {
    this.failureThreshold = failureThreshold;
    this.cooldownMs = cooldownMs;
    this.handler = handler;
  }

  public boolean canProceed() {
    State currentState = state.get();
    if (currentState == State.CLOSED) {
      return true;
    }

    if (System.currentTimeMillis() - lastStateChangeTime.get() > cooldownMs) {
      if (state.compareAndSet(State.OPEN, State.CLOSED)) {
        failureCount.set(0);
        lastStateChangeTime.set(System.currentTimeMillis());
        notifyStateChange(State.CLOSED);
        return true;
      }
      return canProceed();
    }

    return false;
  }

  public void recordFailure() {
    lastFailureTime.set(System.currentTimeMillis());

    int currentFailures = failureCount.incrementAndGet();
    if (currentFailures >= failureThreshold) {
      if (state.compareAndSet(State.CLOSED, State.OPEN)) {
        lastStateChangeTime.set(System.currentTimeMillis());
        notifyStateChange(State.OPEN);
      }
    }
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
