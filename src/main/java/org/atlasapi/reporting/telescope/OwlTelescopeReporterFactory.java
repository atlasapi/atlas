package org.atlasapi.reporting.telescope;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.metabroadcast.columbus.telescope.api.Environment;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.client.TelescopeReporterFactory;
import com.metabroadcast.columbus.telescope.client.TelescopeReporterName;
import com.metabroadcast.common.properties.Configurer;

import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OwlTelescopeReporterFactory extends TelescopeReporterFactory {
    private static final Logger log = LoggerFactory.getLogger(OwlTelescopeReporterFactory.class);


    private static OwlTelescopeReporterFactory INSTANCE;
    public static synchronized OwlTelescopeReporterFactory getInstance() {
        if (INSTANCE == null) {
            ThreadPoolExecutor executor = new ThreadPoolExecutor(
                    1,  //If fewer than this threads are running, a new thread is created. Else things are queued.
                    1, //If the queue is full, spawn a new thread up to this number.
                    10, //time to put idle threads to sleep.
                    TimeUnit.SECONDS,
                    //Max queue size, after which things go to the RejectedExecutionHandler
                    new ArrayBlockingQueue<>(5),
                    new RejectedExecutionHandlerImpl()
            );

            INSTANCE = new OwlTelescopeReporterFactory( //this should always produce an instance.
                    Configurer.get("telescope.environment").get(),
                    Configurer.get("telescope.host").get(),
                    executor,
                    null
            );
        }

        return INSTANCE;
    }

    //What to do with stuff overflowing the queue.
    private static class RejectedExecutionHandlerImpl implements RejectedExecutionHandler {

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            log.error("{} was rejected. Executor status={}", r.toString(), executor);
        }
    }


    /**
     * @param env             Accepts a String that should match {@link Environment}.
     * @param host            the url of the telescope server we are reporting to.
     * @param executor        The executor will be shared between all Reporters produced by this
     *                        factory
     * @param metricsRegistry Can be null if you don't wish metrics to be registered.
     * @throws IllegalArgumentException if it dislikes any of the passed arguments.
     */
    private OwlTelescopeReporterFactory(
            @Nonnull String env,
            @Nonnull String host,
            @Nonnull ThreadPoolExecutor executor,
            @Nullable MetricRegistry metricsRegistry) {
        super(env, host, executor, metricsRegistry);
    }

    public OwlTelescopeReporter getTelescopeReporter(TelescopeReporterName reporterName, Event.Type eventType){
       return new OwlTelescopeReporter(reporterName, eventType, this.getEnvironment(), this.getClient());
    }
}
