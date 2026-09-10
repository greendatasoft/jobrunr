package org.jobrunr.server;

import org.jobrunr.JobRunrException;
import org.jobrunr.server.tasks.PeriodicTaskRunInfo;
import org.jobrunr.server.tasks.Task;
import org.jobrunr.server.tasks.TaskStatistics;
import org.jobrunr.utils.exceptions.RepeatedExceptionFilter;
import org.jobrunr.utils.streams.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static java.time.Instant.now;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;

public abstract class JobHandler implements Runnable {

    private final Logger LOGGER;
    private final BackgroundJobServer backgroundJobServer;
    private final TaskStatistics taskStatistics;
    private final List<Task> tasks;
    // why: the 3 JobZooKeepers of the master are otherwise indistinguishable in the logs
    private final String description;
    // why: an outage of the StorageProvider would otherwise log a stacktrace per task per poll interval
    private final Map<Task, RepeatedExceptionFilter> taskExceptionFilters;
    private final RepeatedExceptionFilter jobHandlerExceptionFilter;
    // why: allows the ServerZooKeeper to detect a JobHandler that is no longer being run by the zookeeper thread pool
    private volatile Instant lastRunEndTime;

    protected JobHandler(BackgroundJobServer backgroundJobServer, Task... tasks) {
        this.LOGGER = LoggerFactory.getLogger(this.getClass());
        this.backgroundJobServer = backgroundJobServer;
        this.taskStatistics = new TaskStatistics(backgroundJobServer.getDashboardNotificationManager());
        this.tasks = asList(tasks);
        this.description = this.tasks.stream().map(task -> task.getClass().getSimpleName()).collect(joining(", ", this.getClass().getSimpleName() + " [", "]"));
        this.taskExceptionFilters = new IdentityHashMap<>();
        this.tasks.forEach(task -> taskExceptionFilters.put(task, new RepeatedExceptionFilter()));
        this.jobHandlerExceptionFilter = new RepeatedExceptionFilter();
        this.lastRunEndTime = now();
    }

    @Override
    public String toString() {
        return description;
    }

    @Override
    public void run() {
        try {
            if (backgroundJobServer.isNotReadyToProcessJobs()) return;

            try (PeriodicTaskRunInfo runInfo = taskStatistics.startRun(backgroundJobServerConfiguration())) {
                boolean allTasksSucceeded = true;
                for (Task task : tasks) {
                    // why: a task that keeps failing (e.g. a corrupt RecurringJob) may not stop the other tasks
                    // (e.g. enqueueing scheduled jobs) from running
                    allTasksSucceeded &= runTask(task, runInfo);
                }
                if (allTasksSucceeded) runInfo.markRunAsSucceeded();
            }
        } catch (Throwable shouldNotHappen) {
            // why: a Throwable escaping this method cancels the periodic task forever (see ScheduledExecutorService#scheduleWithFixedDelay)
            // and would silently stop this BackgroundJobServer from scheduling and processing jobs
            if (jobHandlerExceptionFilter.isFirstOccurrence(shouldNotHappen)) {
                LOGGER.error(JobRunrException.SHOULD_NOT_HAPPEN_MESSAGE + " - {} of {} will continue. Error: ", this, backgroundJobServer, shouldNotHappen);
            } else {
                LOGGER.error("{} of {} keeps failing with the same error ({} times in a row, see the stacktrace above): {}", this, backgroundJobServer, jobHandlerExceptionFilter.getRepeatCount(), shouldNotHappen.toString());
            }
        } finally {
            this.lastRunEndTime = now();
        }
    }

    public Instant getLastRunEndTime() {
        return lastRunEndTime;
    }

    void resetLastRunEndTime() {
        this.lastRunEndTime = now();
    }

    protected BackgroundJobServerConfigurationReader backgroundJobServerConfiguration() {
        return backgroundJobServer.getConfiguration();
    }

    protected <T extends Task> T getTaskOfType(Class<T> clazz) {
        return StreamUtils.ofType(tasks, clazz).findFirst().orElseThrow(() -> new IllegalStateException("Unknown task of type " + clazz.getName()));
    }

    private boolean runTask(Task task, PeriodicTaskRunInfo runInfo) {
        try {
            task.run(runInfo);
            return true;
        } catch (Exception e) {
            taskStatistics.handleException(e);
            logTaskException(task, e);
            return false;
        }
    }

    private void logTaskException(Task task, Exception e) {
        String taskName = task.getClass().getSimpleName();
        RepeatedExceptionFilter exceptionFilter = taskExceptionFilters.get(task);
        if (exceptionFilter == null || exceptionFilter.isFirstOccurrence(e)) {
            LOGGER.warn(JobRunrException.SHOULD_NOT_HAPPEN_MESSAGE + " - {} of {} failed, the other tasks will continue. Exception count: {}. Error: ", taskName, backgroundJobServer, taskStatistics.getExceptionCounter(), e);
        } else {
            LOGGER.warn("{} of {} keeps failing with the same error ({} times in a row, see the stacktrace above). Exception count: {}. Error: {}", taskName, backgroundJobServer, exceptionFilter.getRepeatCount(), taskStatistics.getExceptionCounter(), e.toString());
        }
    }
}
