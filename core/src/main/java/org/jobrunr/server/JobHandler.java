package org.jobrunr.server;

import org.jobrunr.JobRunrException;
import org.jobrunr.server.tasks.PeriodicTaskRunInfo;
import org.jobrunr.server.tasks.Task;
import org.jobrunr.server.tasks.TaskStatistics;
import org.jobrunr.storage.StorageException;
import org.jobrunr.utils.streams.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static java.util.Arrays.asList;
import static org.jobrunr.JobRunrException.shouldNotHappenException;

public abstract class JobHandler implements Runnable {

    private final Logger LOGGER;
    private final BackgroundJobServer backgroundJobServer;
    private final TaskStatistics taskStatistics;
    private final List<Task> tasks;

    protected JobHandler(BackgroundJobServer backgroundJobServer, Task... tasks) {
        this.LOGGER = LoggerFactory.getLogger(this.getClass());
        this.backgroundJobServer = backgroundJobServer;
        this.taskStatistics = new TaskStatistics(backgroundJobServer.getDashboardNotificationManager());
        this.tasks = asList(tasks);
    }

    @Override
    public void run() {
        if (backgroundJobServer.isNotReadyToProcessJobs()) return;

        try (PeriodicTaskRunInfo runInfo = taskStatistics.startRun(backgroundJobServerConfiguration())) {
            tasks.forEach(task -> task.run(runInfo));
            runInfo.markRunAsSucceeded();
        } catch (Exception e) {
            taskStatistics.handleException(e);
            //TODO: Maybe add database fault tolerance
            LOGGER.warn(JobRunrException.SHOULD_NOT_HAPPEN_MESSAGE + " - Processing will continue. Exception count: " + taskStatistics.getExceptionCounter() + ". Error: ", e);
        }
    }

    protected BackgroundJobServerConfigurationReader backgroundJobServerConfiguration() {
        return backgroundJobServer.getConfiguration();
    }

    protected <T extends Task> T getTaskOfType(Class<T> clazz) {
        return StreamUtils.ofType(tasks, clazz).findFirst().orElseThrow(() -> new IllegalStateException("Unknown task of type " + clazz.getName()));
    }
}