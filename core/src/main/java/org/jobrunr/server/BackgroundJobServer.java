package org.jobrunr.server;

import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.filters.JobDefaultFilters;
import org.jobrunr.jobs.filters.JobFilter;
import org.jobrunr.server.concurrent.ConcurrentJobModificationResolver;
import org.jobrunr.server.dashboard.DashboardNotificationManager;
import org.jobrunr.server.degradation.CircuitBreaker;
import org.jobrunr.server.degradation.CircuitBreakerHandler;
import org.jobrunr.server.degradation.State;
import org.jobrunr.server.jmx.BackgroundJobServerMBean;
import org.jobrunr.server.jmx.JobServerStats;
import org.jobrunr.server.lifecycle.BackgroundJobServerLifecycle;
import org.jobrunr.server.lifecycle.LifecycleChangeLock;
import org.jobrunr.server.lifecycle.LifecycleReadLock;
import org.jobrunr.server.runner.BackgroundJobRunner;
import org.jobrunr.server.runner.BackgroundJobWithIocRunner;
import org.jobrunr.server.runner.BackgroundJobWithoutIocRunner;
import org.jobrunr.server.runner.BackgroundStaticFieldJobWithoutIocRunner;
import org.jobrunr.server.runner.BackgroundStaticJobWithoutIocRunner;
import org.jobrunr.server.strategy.WorkDistributionStrategy;
import org.jobrunr.server.tasks.startup.CheckIfAllJobsExistTask;
import org.jobrunr.server.tasks.startup.CreateClusterIdIfNotExists;
import org.jobrunr.server.tasks.startup.MigrateFromV5toV6Task;
import org.jobrunr.server.tasks.startup.StartupTask;
import org.jobrunr.server.tasks.zookeeper.DeleteDeletedJobsPermanentlyTask;
import org.jobrunr.server.tasks.zookeeper.DeleteSucceededJobsTask;
import org.jobrunr.server.tasks.zookeeper.ProcessCarbonAwareAwaitingJobsTask;
import org.jobrunr.server.tasks.zookeeper.ProcessOrphanedJobsTask;
import org.jobrunr.server.tasks.zookeeper.ProcessRecurringJobsTask;
import org.jobrunr.server.tasks.zookeeper.ProcessScheduledJobsTask;
import org.jobrunr.server.threadpool.JobRunrExecutor;
import org.jobrunr.server.threadpool.PlatformThreadPoolJobRunrExecutor;
import org.jobrunr.storage.BackgroundJobServerStatus;
import org.jobrunr.storage.JobRunrMetadata;
import org.jobrunr.storage.StorageProvider;
import org.jobrunr.storage.ThreadSafeStorageProvider;
import org.jobrunr.utils.DurationUtils;
import org.jobrunr.utils.VersionNumber;
import org.jobrunr.utils.mapper.JsonMapper;
import org.jobrunr.utils.threadpool.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import static java.lang.Integer.compare;
import static java.time.Instant.now;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.jobrunr.JobRunrException.problematicConfigurationException;
import static org.jobrunr.server.lifecycle.BackgroundJobServerLifecycleEvent.PAUSE;
import static org.jobrunr.server.lifecycle.BackgroundJobServerLifecycleEvent.RESUME;
import static org.jobrunr.server.lifecycle.BackgroundJobServerLifecycleEvent.START;
import static org.jobrunr.server.lifecycle.BackgroundJobServerLifecycleEvent.STOP;
import static org.jobrunr.utils.JobUtils.assertJobExists;
import static org.jobrunr.utils.VersionNumber.v;

public class BackgroundJobServer implements BackgroundJobServerMBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackgroundJobServer.class);

    public static final int BACKGROUND_JOB_SERVER_COMMON_TASKS_THREAD_SIZE = 2;
    public static final int BACKGROUND_JOB_SERVER_MASTER_TASKS_THREAD_SIZE = 3;

    protected final BackgroundJobServerConfigurationReader configuration;
    protected final StorageProvider storageProvider;
    protected final DashboardNotificationManager dashboardNotificationManager;
    protected final JsonMapper jsonMapper;
    protected final List<BackgroundJobRunner> backgroundJobRunners;
    protected final JobDefaultFilters jobDefaultFilters;
    protected final JobServerStats jobServerStats;
    protected final WorkDistributionStrategy workDistributionStrategy;
    protected final JobSteward jobSteward;
    protected final ServerZooKeeper serverZooKeeper;
    protected final ConcurrentJobModificationResolver concurrentJobModificationResolver;
    protected final BackgroundJobServerLifecycle lifecycle;
    protected final BackgroundJobPerformerFactory backgroundJobPerformerFactory;
    protected final CircuitBreaker circuitBreaker;
    protected volatile Instant firstHeartbeat;
    protected volatile Boolean isMaster;
    protected volatile VersionNumber dataVersion;
    private final List<JobZooKeeper> masterTasks;
    private final AtomicBoolean startupTasksRunning;
    private volatile boolean startupTasksSucceeded;
    private volatile Instant lastStartupTasksAttempt;
    private volatile PlatformThreadPoolJobRunrExecutor zookeeperThreadPool;
    private JobRunrExecutor jobExecutor;

    public BackgroundJobServer(StorageProvider storageProvider, JsonMapper jsonMapper, JobActivator jobActivator, BackgroundJobServerConfiguration configuration) {
        this(storageProvider, jsonMapper, jobActivator, null, new BackgroundJobServerConfigurationReader(configuration));
    }

    protected BackgroundJobServer(StorageProvider storageProvider, JsonMapper jsonMapper, JobActivator jobActivator, CircuitBreaker circuitBreaker, BackgroundJobServerConfigurationReader configuration) {
        if (storageProvider == null) {
            throw new IllegalArgumentException("A StorageProvider is required to use a BackgroundJobServer. Please see the documentation on how to setup a job StorageProvider.");
        }

        this.configuration = configuration;
        this.storageProvider = new ThreadSafeStorageProvider(storageProvider);
        this.dashboardNotificationManager = new DashboardNotificationManager(this.configuration.getId(), storageProvider);
        this.jsonMapper = jsonMapper;
        this.backgroundJobRunners = initializeBackgroundJobRunners(jobActivator);
        this.jobDefaultFilters = new JobDefaultFilters();
        this.jobServerStats = new JobServerStats();
        this.workDistributionStrategy = createWorkDistributionStrategy();
        this.jobSteward = createJobSteward();
        this.serverZooKeeper = createServerZooKeeper();
        this.concurrentJobModificationResolver = createConcurrentJobModificationResolver();
        this.backgroundJobPerformerFactory = loadBackgroundJobPerformerFactory();
        this.storageProvider.validatePollInterval(this.configuration.getPollInterval());
        this.lifecycle = new BackgroundJobServerLifecycle();
        this.circuitBreaker = circuitBreaker == null ? createCircuitBreaker() : circuitBreaker;
        this.masterTasks = new CopyOnWriteArrayList<>();
        this.startupTasksRunning = new AtomicBoolean();
    }

    @Override
    public UUID getId() {
        return configuration.getId();
    }

    @Override
    public String toString() {
        return String.format("BackgroundJobServer (%s - %s)", configuration.getName(), configuration.getId());
    }

    @Override
    public void start() {
        start(true);
    }

    public void start(boolean guard) {
        if (guard) {
            try (LifecycleChangeLock lifecycleChange = lifecycle.goTo(START)) {
                if (isStarted()) return;
                circuitBreaker.reset();
                firstHeartbeat = now();
                startStewardAndServerZooKeeper();
                startWorkers();
                lifecycleChange.succeeded();
            }
        }
    }

    @Override
    public void pauseProcessing() {
        try (LifecycleChangeLock lifecycleChange = lifecycle.goTo(PAUSE)) {
            if (isStopped()) throw new IllegalStateException("First start the BackgroundJobServer before pausing");
            if (isPaused()) return;
            stopWorkers();
            LOGGER.info("{} Paused job processing", this);
            lifecycleChange.succeeded();
        }
    }

    @Override
    public void resumeProcessing() {
        try (LifecycleChangeLock lifecycleChange = lifecycle.goTo(RESUME)) {
            if (isStopped()) throw new IllegalStateException("First start the BackgroundJobServer before resuming");
            if (isProcessing()) return;
            startWorkers();
            LOGGER.info("{} Resumed job processing", this);
            lifecycleChange.succeeded();
        }
    }

    @Override
    public void stop() {
        stop(true);
    }

    public void stop(boolean resetCircuitBreaker) {
        // why: an explicit stop must always cancel a pending circuit breaker recovery, also when this server is
        // already being stopped by the circuit breaker itself. Otherwise it silently restarts itself after the cooldown.
        if (resetCircuitBreaker) circuitBreaker.reset();
        if (isStopping()) return;
        try (LifecycleChangeLock lifecycleChange = lifecycle.goTo(STOP)) {
            if (isStopped()) return;
            LOGGER.info("{} stopping (may take about {})", this, configuration.getInterruptJobsAwaitDurationOnStopBackgroundJobServer());
            isMaster = null;
            try {
                stopWorkers();
                stopZooKeepers();
                LOGGER.info("{} BackgroundJobServer and BackgroundJobPerformers stopped", this);
            } catch (Exception e) {
                // why: if stopping fails halfway, the server must still end up in the stopped state. Otherwise start()
                // silently does nothing (as isStarted() returns true) and this server never processes jobs again.
                LOGGER.error("{} could not be stopped gracefully - forcing it into the stopped state", this, e);
                this.jobExecutor = null;
                this.zookeeperThreadPool = null;
            } finally {
                masterTasks.clear();
                firstHeartbeat = null;
                lifecycleChange.succeeded();
            }
        }
    }

    boolean isStarted() {
        return !isStopped();
    }

    boolean isStopped() {
        try (LifecycleReadLock ignored = lifecycle.readLock()) {
            return zookeeperThreadPool == null;
        }
    }

    boolean isPaused() {
        return !isProcessing();
    }

    boolean isProcessing() {
        try (LifecycleReadLock ignored = lifecycle.readLock()) {
            return lifecycle.isRunning();
        }
    }

    public boolean isAnnounced() {
        if (isStopping()) return false;
        try (LifecycleReadLock ignored = lifecycle.readLock()) {
            return isMaster != null;
        }
    }

    public boolean isUnAnnounced() {
        return !isAnnounced();
    }

    public boolean isMaster() {
        return isAnnounced() && isMaster;
    }

    void setIsMaster(Boolean isMaster) {
        if (isStopping() || isStopped()) return;

        this.isMaster = isMaster;
        if (isMaster != null) {
            LOGGER.info("JobRunr {} using {} and {} BackgroundJobPerformers started successfully", this, storageProvider.getStorageProviderInfo().getName(), workDistributionStrategy.getWorkerCount());
            if (isMaster) {
                runStartupTasks();
                startMasterTasks();
            } else {
                stopMasterTasks();
            }
        } else {
            LOGGER.error("JobRunr {} could not announce itself to the StorageProvider - it will not schedule or process any jobs until it succeeds", this);
        }
    }

    void restartMasterTasks() {
        if (isStopping() || isStopped()) return;
        try (LifecycleReadLock ignored = lifecycle.readLock()) {
            if (zookeeperThreadPool == null || !Boolean.TRUE.equals(isMaster)) return;
            startMasterTasks();
        }
    }

    List<JobHandler> getStalledJobHandlers(Duration maxDurationWithoutRun) {
        if (isStopping() || isStopped()) return emptyList();
        Instant stalledIfBefore = now().minus(maxDurationWithoutRun);
        List<JobHandler> stalledJobHandlers = new ArrayList<>();
        if (jobSteward.getLastRunEndTime().isBefore(stalledIfBefore)) stalledJobHandlers.add(jobSteward);
        masterTasks.stream()
                .filter(masterTask -> masterTask.getLastRunEndTime().isBefore(stalledIfBefore))
                .forEach(stalledJobHandlers::add);
        return stalledJobHandlers;
    }

    boolean hasStalledMasterTasksOnly(List<JobHandler> stalledJobHandlers) {
        return !stalledJobHandlers.isEmpty() && stalledJobHandlers.stream().allMatch(JobZooKeeper.class::isInstance);
    }

    void retryStartupTasksIfNeeded() {
        if (startupTasksSucceeded || !isMaster() || startupTasksRunning.get()) return;
        Duration retryInterval = configuration.getPollInterval().multipliedBy(configuration.getServerTimeoutPollIntervalMultiplicand());
        if (lastStartupTasksAttempt != null && now().isBefore(lastStartupTasksAttempt.plus(retryInterval))) return;

        LOGGER.warn("JobRunr {} did not complete its startup tasks successfully - retrying (no jobs are processed until they succeed).", this);
        runStartupTasks();
    }

    @Override
    public boolean isRunning() {
        // why: otherwise all the workers querying this method when they onboard work can cause deadlock
        if (lifecycle.isTransitioning()) return false;
        try (LifecycleReadLock ignored = lifecycle.readLock()) {
            if (isStopping()) return false;
            return lifecycle.isRunning();
        }
    }

    public boolean isNotReadyToProcessJobs() {
        return !(isAnnounced() && hasDataVersion(v("6.0.0"))) || !getCircuitBreaker().canProceed();
    }

    @Override
    public BackgroundJobServerStatus getServerStatus() {
        return new BackgroundJobServerStatus(configuration.getId(), configuration.getName(), workDistributionStrategy.getWorkerCount(),
                (int) configuration.getPollInterval().getSeconds(), configuration.getDeleteSucceededJobsAfter(), configuration.getPermanentlyDeleteDeletedJobsAfter(),
                firstHeartbeat, now(), isRunning(), jobServerStats);
    }

    public JobSteward getJobSteward() {
        return jobSteward;
    }

    public StorageProvider getStorageProvider() {
        return storageProvider;
    }

    public ConcurrentJobModificationResolver getConcurrentJobModificationResolver() {
        return concurrentJobModificationResolver;
    }

    public BackgroundJobServerConfigurationReader getConfiguration() {
        return configuration;
    }

    public DashboardNotificationManager getDashboardNotificationManager() {
        return dashboardNotificationManager;
    }

    public JsonMapper getJsonMapper() {
        return jsonMapper;
    }

    public WorkDistributionStrategy getWorkDistributionStrategy() {
        return workDistributionStrategy;
    }

    public CircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    public void setJobFilters(List<JobFilter> jobFilters) {
        this.jobDefaultFilters.addAll(jobFilters);
    }

    public JobDefaultFilters getJobFilters() {
        return jobDefaultFilters;
    }

    BackgroundJobRunner getBackgroundJobRunner(Job job) {
        assertJobExists(job.getJobDetails());
        return backgroundJobRunners.stream()
                .filter(jobRunner -> jobRunner.supports(job))
                .findFirst()
                .orElseThrow(() -> problematicConfigurationException("Could not find a BackgroundJobRunner: either no JobActivator is registered, your Background Job Class is not registered within the IoC container or your Job does not have a default no-arg constructor."));
    }

    public void processJob(Job job) {
        BackgroundJobPerformer backgroundJobPerformer = backgroundJobPerformerFactory.newBackgroundJobPerformer(this, job);
        jobExecutor.execute(backgroundJobPerformer);
        LOGGER.debug("Submitted BackgroundJobPerformer for job {} to executor service", job.getId());
    }

    private void startStewardAndServerZooKeeper() {
        zookeeperThreadPool = new PlatformThreadPoolJobRunrExecutor(BACKGROUND_JOB_SERVER_COMMON_TASKS_THREAD_SIZE, "backgroundjob-zookeeper-pool");
        // why fixedDelay: in case of long stop-the-world garbage collections, the zookeeper tasks will queue up
        // and all will be launched one after another
        Duration jobStewardInitialDelay = DurationUtils.min(configuration.getPollInterval().dividedBy(5), Duration.ofSeconds(1));
        jobSteward.resetLastRunEndTime();
        zookeeperThreadPool.scheduleWithFixedDelay(serverZooKeeper, Duration.ZERO, configuration.getPollInterval());
        zookeeperThreadPool.scheduleWithFixedDelay(jobSteward, jobStewardInitialDelay, configuration.getPollInterval());
    }

    private synchronized void startMasterTasks() {
        // why: makes sure we never end up with 2 sets of master tasks scheduled at the same time (e.g. when this server
        // becomes master again without having been demoted first) as that results in duplicate recurring jobs
        stopMasterTasks();

        PlatformThreadPoolJobRunrExecutor threadPool = this.zookeeperThreadPool;
        if (threadPool == null) return;

        Duration masterTasksInitialDelay = DurationUtils.min(configuration.getPollInterval().dividedBy(5), Duration.ofSeconds(1));
        JobZooKeeper recurringAndCarbonAwareAndScheduledJobsZooKeeper = new JobZooKeeper(this, new ProcessRecurringJobsTask(this), new ProcessCarbonAwareAwaitingJobsTask(this), new ProcessScheduledJobsTask(this));
        JobZooKeeper orphanedJobsZooKeeper = new JobZooKeeper(this, new ProcessOrphanedJobsTask(this));
        JobZooKeeper janitorZooKeeper = new JobZooKeeper(this, new DeleteSucceededJobsTask(this), new DeleteDeletedJobsPermanentlyTask(this));
        threadPool.increasePoolSize(BACKGROUND_JOB_SERVER_MASTER_TASKS_THREAD_SIZE);
        threadPool.scheduleWithFixedDelay(recurringAndCarbonAwareAndScheduledJobsZooKeeper, masterTasksInitialDelay, configuration.getPollInterval());
        threadPool.scheduleWithFixedDelay(orphanedJobsZooKeeper, masterTasksInitialDelay, configuration.getPollInterval());
        threadPool.scheduleWithFixedDelay(janitorZooKeeper, masterTasksInitialDelay, configuration.getPollInterval());
        masterTasks.addAll(asList(recurringAndCarbonAwareAndScheduledJobsZooKeeper, orphanedJobsZooKeeper, janitorZooKeeper));
    }

    private synchronized void stopMasterTasks() {
        masterTasks.clear();
        PlatformThreadPoolJobRunrExecutor threadPool = this.zookeeperThreadPool;
        if (threadPool == null) return;
        threadPool.cancelScheduledFuturesOfType(JobZooKeeper.class);
    }

    private void stopZooKeepers() {
        try {
            // why: the thread pool is stopped first as serverZooKeeper.stop() talks to the StorageProvider. If that
            // is unreachable, it blocks until the connection timeout passes while the zookeeper tasks would otherwise
            // keep hammering the very same StorageProvider
            zookeeperThreadPool.stop(Duration.ofSeconds(10));
            serverZooKeeper.stop();
        } finally {
            this.zookeeperThreadPool = null;
        }
    }

    private void startWorkers() {
        jobExecutor = configuration.getBackgroundJobServerWorkerPolicy().toJobRunrExecutor();
        jobExecutor.start();
    }

    private void stopWorkers() {
        if (jobExecutor == null) return;
        LOGGER.info("{} BackgroundJobPerformers stopping (waiting at most {} for jobs to finish)", this, configuration.getInterruptJobsAwaitDurationOnStopBackgroundJobServer());
        jobExecutor.stop(configuration.getInterruptJobsAwaitDurationOnStopBackgroundJobServer());
        this.jobExecutor = null;
    }

    private void runStartupTasks() {
        if (!startupTasksRunning.compareAndSet(false, true)) return;

        lastStartupTasksAttempt = now();
        ExecutorService startupTasksExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("jobrunr-startup-task", false));
        try {
            startupTasksExecutor.execute(() -> {
                try {
                    new StartupTask(
                            new CreateClusterIdIfNotExists(this),
                            new CheckIfAllJobsExistTask(this),
                            new MigrateFromV5toV6Task(this)
                    ).run();
                    startupTasksSucceeded = true;
                } catch (Exception e) {
                    // why: without successful startup tasks no job is processed at all, so it must be retried
                    LOGGER.error("JobRunr {} could not run all startup tasks", this, e);
                } finally {
                    startupTasksRunning.set(false);
                }
            });
        } catch (Exception notImportant) {
            // server is shut down immediately
            startupTasksRunning.set(false);
        } finally {
            startupTasksExecutor.shutdown();
        }
    }

    private List<BackgroundJobRunner> initializeBackgroundJobRunners(JobActivator jobActivator) {
        return asList(
                new BackgroundJobWithIocRunner(jobActivator),
                new BackgroundJobWithoutIocRunner(),
                new BackgroundStaticJobWithoutIocRunner(),
                new BackgroundStaticFieldJobWithoutIocRunner()
        );
    }

    protected ServerZooKeeper createServerZooKeeper() {
        return new ServerZooKeeper(this);
    }

    protected JobSteward createJobSteward() {
        return new JobSteward(this);
    }

    protected ConcurrentJobModificationResolver createConcurrentJobModificationResolver() {
        return getConfiguration()
                .getConcurrentJobModificationPolicy()
                .toConcurrentJobModificationResolver(this);
    }

    private boolean hasDataVersion(VersionNumber expectedVersion) {
        if (expectedVersion.equals(dataVersion)) return true;
        JobRunrMetadata metadata = storageProvider.getMetadata("database_version", "cluster");
        if (metadata != null) {
            dataVersion = v(metadata.getValue());
            if (dataVersion.isNewerThan(expectedVersion)) {
                LOGGER.error("JobRunr Pro Version number {} is older than database version number {}. BackgroundJobServer will not process any jobs.", expectedVersion, dataVersion);
            }
            return expectedVersion.equals(dataVersion);
        }
        return false;
    }

    protected WorkDistributionStrategy createWorkDistributionStrategy() {
        return configuration.getBackgroundJobServerWorkerPolicy().toWorkDistributionStrategy(this);
    }

    protected CircuitBreaker createCircuitBreaker() {
        return CircuitBreaker.createDefault(new CircuitBreakerPauseHandler());
    }

    private BackgroundJobPerformerFactory loadBackgroundJobPerformerFactory() {
        ServiceLoader<BackgroundJobPerformerFactory> serviceLoader = ServiceLoader.load(BackgroundJobPerformerFactory.class);
        return stream(spliteratorUnknownSize(serviceLoader.iterator(), Spliterator.ORDERED), false)
                .min((a, b) -> compare(b.getPriority(), a.getPriority()))
                .orElseGet(BasicBackgroundJobPerformerFactory::new);
    }

    boolean isStopping() {
        return lifecycle.isTransitioningTo(STOP);
    }

    private static class BasicBackgroundJobPerformerFactory implements BackgroundJobPerformerFactory {
        @Override
        public int getPriority() {
            return 10;
        }

        @Override
        public BackgroundJobPerformer newBackgroundJobPerformer(BackgroundJobServer backgroundJobServer, Job job) {
            return new BackgroundJobPerformer(backgroundJobServer, job);
        }
    }

    protected class CircuitBreakerPauseHandler implements CircuitBreakerHandler {

        @Override
        public void onStateChange(State newState) {
            if (newState == State.OPEN) {
                LOGGER.warn("{} - Circuit Breaker OPENED, stopping this BackgroundJobServer until it recovers", BackgroundJobServer.this);
                // stop() must not run on the zookeeper pool thread that triggered the failure:
                // stopZooKeepers() awaits termination of that very pool and would deadlock.
                // Pass resetCircuitBreaker=false so the scheduled recovery can still fire.
                Thread t = new Thread(() -> BackgroundJobServer.this.stop(false), "jobrunr-circuit-breaker-stop");
                t.setDaemon(true);
                t.start();
            } else if (newState == State.CLOSED) {
                LOGGER.warn("{} - Circuit Breaker CLOSED, restarting this BackgroundJobServer", BackgroundJobServer.this);
                // Called from the circuit breaker's recovery executor, not from the zookeeper pool,
                // so start() can run inline without risking a deadlock.
                try {
                    BackgroundJobServer.this.start();
                } catch (Exception e) {
                    // why: if the restart fails, nothing else will ever restart this server. Reopening the circuit
                    // breaker schedules a new recovery attempt after the cooldown period.
                    LOGGER.error("{} could not be restarted - a new attempt is scheduled", BackgroundJobServer.this, e);
                    circuitBreaker.trip();
                }
            }
        }

        @Override
        public void close() {
        }
    }
}
