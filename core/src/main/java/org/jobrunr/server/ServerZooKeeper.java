package org.jobrunr.server;

import org.jobrunr.server.dashboard.CpuAllocationIrregularityNotification;
import org.jobrunr.server.dashboard.DashboardNotificationManager;
import org.jobrunr.storage.BackgroundJobServerStatus;
import org.jobrunr.storage.ServerTimedOutException;
import org.jobrunr.storage.StorageProvider;
import org.jobrunr.utils.exceptions.RepeatedExceptionFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.jobrunr.server.DesktopUtils.hasSystemSleptRecently;
import static org.jobrunr.server.DesktopUtils.systemSupportsSleepDetection;

public class ServerZooKeeper implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerZooKeeper.class);

    private final BackgroundJobServer backgroundJobServer;
    private final StorageProvider storageProvider;
    private final DashboardNotificationManager dashboardNotificationManager;
    private final Duration timeoutDuration;
    private final AtomicInteger restartAttempts;
    private final AtomicInteger stalledJobHandlerAttempts;
    // why: an outage of the StorageProvider would otherwise log the same stacktrace every poll interval
    private final RepeatedExceptionFilter unrecoverableErrorFilter;
    private UUID masterId;
    private Instant lastSignalAlive;
    private Instant lastServerTimeoutCheck;

    public ServerZooKeeper(BackgroundJobServer backgroundJobServer) {
        this.backgroundJobServer = backgroundJobServer;
        this.storageProvider = backgroundJobServer.getStorageProvider();
        this.dashboardNotificationManager = backgroundJobServer.getDashboardNotificationManager();
        this.timeoutDuration = backgroundJobServer.getConfiguration().getPollInterval().multipliedBy(backgroundJobServer.getConfiguration().getServerTimeoutPollIntervalMultiplicand());
        this.restartAttempts = new AtomicInteger();
        this.stalledJobHandlerAttempts = new AtomicInteger();
        this.unrecoverableErrorFilter = new RepeatedExceptionFilter();
        this.lastSignalAlive = Instant.now();
        this.lastServerTimeoutCheck = Instant.now();
        if (LOGGER.isTraceEnabled()) LOGGER.trace(systemSupportsSleepDetection()
                ? "JobRunr can detect desktop sleeping."
                : "JobRunr can not detect desktop sleeping.");
    }

    @Override
    public void run() {
        if (backgroundJobServer.isStopping() || backgroundJobServer.isStopped()) return;
        if (!backgroundJobServer.getCircuitBreaker().canProceed()) return;

        try {
            if (backgroundJobServer.isUnAnnounced()) {
                announceBackgroundJobServer();
                backgroundJobServer.getCircuitBreaker().recordSuccess();
            } else {
                signalBackgroundJobServerAliveAndDoZooKeeping();
            }
        } catch (Exception shouldNotHappen) {
            logUnrecoverableError(shouldNotHappen);
            if (masterId == null) backgroundJobServer.setIsMaster(null);
            backgroundJobServer.getCircuitBreaker().recordFailure();
        }
    }

    private void logUnrecoverableError(Exception e) {
        if (unrecoverableErrorFilter.isFirstOccurrence(e)) {
            LOGGER.error("An unrecoverable error occurred in {}.", backgroundJobServer, e);
        } else {
            LOGGER.error("{} keeps failing with the same error ({} times in a row, see the stacktrace above): {}", backgroundJobServer, unrecoverableErrorFilter.getRepeatCount(), e.toString());
        }
    }

    public synchronized void stop() {
        restartAttempts.set(0);
        try {
            storageProvider.signalBackgroundJobServerStopped(backgroundJobServer.getServerStatus());
        } catch (Exception e) {
            LOGGER.error("Error when signalling that {} stopped", backgroundJobServer, e);
        } finally {
            masterId = null;
        }
    }

    private void announceBackgroundJobServer() {
        final BackgroundJobServerStatus serverStatus = backgroundJobServer.getServerStatus();
        storageProvider.announceBackgroundJobServer(serverStatus);
        determineIfCurrentBackgroundJobServerIsMaster();
        lastSignalAlive = serverStatus.getLastHeartbeat();
    }

    private void signalBackgroundJobServerAliveAndDoZooKeeping() {
        try {
            signalBackgroundJobServerAlive();
            deleteServersThatTimedOut();
            determineIfCurrentBackgroundJobServerIsMaster();
            backgroundJobServer.retryStartupTasksIfNeeded();
            ensureJobHandlersAreRunning();
            // why: only failures that happen consecutively may open the circuit breaker and stop this server
            backgroundJobServer.getCircuitBreaker().recordSuccess();
        } catch (ServerTimedOutException e) {
            LOGGER.error("SEVERE ERROR - {} timed out while it's still alive. Are all servers using NTP and in the same timezone? Are you having long GC cycles? Restart attempt {} out of 3", backgroundJobServer, restartAttempts.incrementAndGet(), e);
            backgroundJobServer.getCircuitBreaker().recordFailure();
        }
    }

    private void ensureJobHandlersAreRunning() {
        List<JobHandler> stalledJobHandlers = backgroundJobServer.getStalledJobHandlers(timeoutDuration);
        if (stalledJobHandlers.isEmpty()) {
            stalledJobHandlerAttempts.set(0);
            return;
        }

        int attempt = stalledJobHandlerAttempts.incrementAndGet();
        if (attempt == 1 && backgroundJobServer.hasStalledMasterTasksOnly(stalledJobHandlers)) {
            LOGGER.error("SEVERE ERROR - {} did not run {} for more than {}. Restarting the master tasks.", backgroundJobServer, stalledJobHandlers, timeoutDuration);
            backgroundJobServer.restartMasterTasks();
        } else {
            LOGGER.error("SEVERE ERROR - {} did not run {} for more than {} (are all threads of the zookeeper thread pool blocked - e.g. on a database call without a socket timeout?). Restarting the BackgroundJobServer.",
                    backgroundJobServer, stalledJobHandlers, timeoutDuration);
            stalledJobHandlerAttempts.set(0);
            backgroundJobServer.getCircuitBreaker().trip();
        }
    }

    private void signalBackgroundJobServerAlive() {
        final BackgroundJobServerStatus serverStatus = backgroundJobServer.getServerStatus();
        storageProvider.signalBackgroundJobServerAlive(serverStatus);
        cpuAllocationIrregularity(lastSignalAlive, serverStatus.getLastHeartbeat()).ifPresent(amountOfSeconds -> dashboardNotificationManager.notify(new CpuAllocationIrregularityNotification(amountOfSeconds)));
        lastSignalAlive = serverStatus.getLastHeartbeat();
    }

    private void deleteServersThatTimedOut() {
        if (Instant.now().isAfter(this.lastServerTimeoutCheck.plus(timeoutDuration))) {
            final Instant now = Instant.now();
            final Instant defaultTimeoutInstant = now.minus(timeoutDuration);
            final Instant timedOutInstantUsingLastSignalAlive = lastSignalAlive.minusMillis(500);
            final Instant timedOutInstant = min(defaultTimeoutInstant, timedOutInstantUsingLastSignalAlive);

            final int amountOfServersThatTimedOut = storageProvider.removeTimedOutBackgroundJobServers(timedOutInstant);
            if (amountOfServersThatTimedOut > 0) {
                LOGGER.info("{} removed {} BackgroundJobServer(s) that timed out (no heartbeat since {})", backgroundJobServer, amountOfServersThatTimedOut, timedOutInstant);
            }
            this.lastServerTimeoutCheck = now;
        }
    }

    private void determineIfCurrentBackgroundJobServerIsMaster() {
        UUID longestRunningBackgroundJobServerId = storageProvider.getLongestRunningBackgroundJobServerId();
        if (this.masterId == null || !masterId.equals(longestRunningBackgroundJobServerId)) {
            this.masterId = longestRunningBackgroundJobServerId;
            if (masterId.equals(backgroundJobServer.getId())) {
                backgroundJobServer.setIsMaster(true);
                LOGGER.info("{} is master (this BackgroundJobServer)", backgroundJobServer);
            } else {
                backgroundJobServer.setIsMaster(false);
                LOGGER.info("{} is master (another BackgroundJobServer) - this is {}", describeBackgroundJobServer(masterId), backgroundJobServer);
            }
        }
    }

    private String describeBackgroundJobServer(UUID backgroundJobServerId) {
        try {
            return storageProvider.getBackgroundJobServers().stream()
                    .filter(serverStatus -> backgroundJobServerId.equals(serverStatus.getId()))
                    .findFirst()
                    .map(serverStatus -> String.format("BackgroundJobServer (%s - %s)", serverStatus.getName(), backgroundJobServerId))
                    .orElseGet(() -> String.format("BackgroundJobServer (%s)", backgroundJobServerId));
        } catch (Exception e) {
            // why: resolving the name of another server is best effort only and may never break the zookeeper run
            LOGGER.debug("Could not resolve the name of BackgroundJobServer {}", backgroundJobServerId, e);
            return String.format("BackgroundJobServer (%s)", backgroundJobServerId);
        }
    }

    private void resetServer() {
        backgroundJobServer.setIsMaster(false);
        backgroundJobServer.stop();
        backgroundJobServer.start();
    }

    private void stopServer() {
        backgroundJobServer.stop();
    }

    private static Instant min(Instant instant1, Instant instant2) {
        return instant1.isBefore(instant2) ? instant1 : instant2;
    }

    private Optional<Integer> cpuAllocationIrregularity(Instant lastSignalAlive, Instant lastHeartbeat) {
        if (systemSupportsSleepDetection() && hasSystemSleptRecently()) return Optional.empty();

        final Instant now = Instant.now();
        final int amount1OfSec = (int) Math.abs(lastHeartbeat.getEpochSecond() - lastSignalAlive.getEpochSecond());
        final int amount2OfSec = (int) (now.getEpochSecond() - lastSignalAlive.getEpochSecond());
        final int amount3OfSec = (int) (now.getEpochSecond() - lastHeartbeat.getEpochSecond());

        final int max = Math.max(amount1OfSec, Math.max(amount2OfSec, amount3OfSec));
        if (max > backgroundJobServer.getConfiguration().getPollInterval().getSeconds() * 2L) {
            return Optional.of(max);
        }
        return Optional.empty();
    }
}
