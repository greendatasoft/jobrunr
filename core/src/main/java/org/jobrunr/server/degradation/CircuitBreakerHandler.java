package org.jobrunr.server.degradation;

public interface CircuitBreakerHandler {

    void onStateChange(State newState);

    void close();
}
