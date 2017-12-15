package com.jivesoftware.data.impl;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.exceptions.CloneException;
import com.jivesoftware.data.resources.entities.Database;
import org.apache.commons.exec.*;
import org.apache.log4j.Priority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.codahale.metrics.MetricRegistry.name;


public class CloneManager {


    private final static Logger logger = LoggerFactory.getLogger(DatabaseManager.class);

    private final DBaaSConfiguration.CloneConfiguration cloneConfiguration;
    private final Timer cloneTimer;

    @Inject
    public CloneManager(DBaaSConfiguration dBaaSConfiguration,
                        MetricRegistry metricRegistry) {
        this.cloneConfiguration = dBaaSConfiguration.getCloneConfiguration();
        cloneTimer = metricRegistry.timer(name(CloneManager
                .class, "cloneCommand"));
    }

    protected void clone(MasterDatabase sourceDatabase, String sourceSchema,
                         Database targetDatabase, String targetPassword) throws CloneException {

        final AtomicReference<String> error = new AtomicReference<>();

        try {
            Map<String, String> params = ImmutableMap.<String, String>builder()
                    .put("source_user", sourceDatabase.getUsername())
                    .put("source_password", sourceDatabase.getPassword())
                    .put("source_host", sourceDatabase.getHost())
                    .put("source_port", String.valueOf(sourceDatabase.getPort()))
                    .put("source_schema", sourceSchema)
                    .put("target_user", targetDatabase.getUser())
                    .put("target_password", targetPassword)
                    .put("target_host", targetDatabase.getHost())
                    .put("target_port", String.valueOf(targetDatabase.getPort()))
                    .put("target_schema", targetDatabase.getSchema()).build();

            CommandLine command = CommandLine.parse(cloneConfiguration.getCommand(), params);
            DefaultExecuteResultHandler resultHandler = getNewResultHandler();
            ExecuteWatchdog watchdog = new ExecuteWatchdog(cloneConfiguration.getExecutionTimeout());
            org.apache.commons.exec.Executor executor = getNewExecutor();
            executor.setExitValue(0);

            LogOutputStream outputStream = new LogOutputStream(Priority.INFO_INT) {

                @Override
                protected void processLine(final String line, final int level) {
                    logger.info(String.format("Cloning %s of %s into %s of %s logged %s",
                            sourceDatabase.getSchema(), sourceDatabase.getHost(),
                            targetDatabase.getSchema(), targetDatabase.getHost(), line));
                }
            };

            LogOutputStream errorStream = new LogOutputStream(Priority.ERROR_INT) {

                @Override
                protected void processLine(final String line, final int level) {
                    String lastError = error.get();
                    if (lastError != null) {
                        error.set(lastError + "\n" + line);
                    } else {
                        error.set(line);
                    }

                    logger.info(String.format("Cloning %s of %s into %s of %s logged error %s",
                            sourceDatabase.getSchema(), sourceDatabase.getHost(),
                            targetDatabase.getSchema(), targetDatabase.getHost(), line));
                }
            };

            executor.setStreamHandler(new PumpStreamHandler(outputStream, errorStream));

            executor.setProcessDestroyer(new ShutdownHookProcessDestroyer());
            executor.setWatchdog(watchdog);

            final Timer.Context context = cloneTimer.time();
            try {
                executor.execute(command, resultHandler);
                resultHandler.waitFor(cloneConfiguration.getExecutionTimeout());
            } finally {
                context.stop();
            }

            if (resultHandler.getException() != null) {
                logger.error("Error running cloning command", resultHandler.getException());
                throw new RuntimeException(resultHandler.getException().getMessage());
            }

            if (!resultHandler.hasResult() || resultHandler.getExitValue() != 0) {
                error.set(String.format("Cloning %s of %s into %s of %s logged produced no result in %d",
                        sourceDatabase.getSchema(), sourceDatabase.getHost(),
                        targetDatabase.getSchema(), targetDatabase.getHost(),
                        cloneConfiguration.getExecutionTimeout()));
            }


        } catch (InterruptedException ie) {
            logger.error("Error cloning", ie);
            error.set(ie.getMessage());
        } catch (Exception e) {
            logger.error("Error cloning", e);
            error.set(e.getMessage());
        }

        if (error.get() != null) {
            logger.error(error.get());
            throw new CloneException(error.get());
        }

    }

    DefaultExecuteResultHandler getNewResultHandler(){
        return new DefaultExecuteResultHandler();
    }

    Executor getNewExecutor(){
        return new DefaultExecutor();
    }
}


