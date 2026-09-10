package org.jobrunr.server.tasks.startup;

import org.jobrunr.JobRunrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class StartupTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StartupTask.class);

    private final List<Runnable> tasks;

    public StartupTask(Runnable... tasks) {
        this.tasks = Arrays.asList(tasks);
    }

    @Override
    public void run() {
        Exception firstException = null;
        for (Runnable task : tasks) {
            try {
                task.run();
            } catch (Exception e) {
                // why: a failing startup task may not stop the other startup tasks (e.g. the one storing the data
                // version, without which no BackgroundJobServer in the cluster processes any job)
                LOGGER.error("JobRunr startup task {} failed", task.getClass().getSimpleName(), e);
                if (firstException == null) firstException = e;
            }
        }
        if (firstException != null) {
            throw JobRunrException.shouldNotHappenException(firstException);
        }
    }
}
