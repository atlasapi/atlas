package org.atlasapi;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.ProtectionDomain;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.atlasapi.util.jetty.InstrumentedQueuedThreadPool;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.JvmAttributeGaugeSet;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import org.eclipse.jetty.io.ByteBufferPool;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.Scheduler;
import org.eclipse.jetty.webapp.WebAppContext;

public class AtlasMain {

    private static final String METRIC_METHOD_NAME = "getMetrics";
    private static final String METRIC_REGISTRY_METHOD_NAME = "getMetricRegistry";
    private static final String SERVER_REQUEST_THREADS_OVERRIDE_PROPERTY_NAME = "request.threads";
    private static final int DEFAULT_SERVER_REQUEST_THREADS = 100;
    private static final String SERVER_REQUEST_THREAD_PREFIX = "api-request-thread";
    private static final int SERVER_ACCEPT_QUEUE_SIZE = 500;

    private static final String SERVER_PORT_OVERRIDE_PROPERTY_NAME = "server.port";
    private static final int API_DEFAULT_PORT = 8080;
    private static final int PROCESSING_DEFAULT_PORT = 8282;

    private static final int MONITORING_REQUEST_THREADS = 20;
    private static final String MONITORING_REQUEST_THREAD_PREFIX = "monitoring-request-thread";
    private static final int MONITORING_DEFAULT_PORT = 8081;
    private static final String MONITORING_PORT_OVERRIDE_PROPERTY_NAME = "monitoring.port";

    public static final String CONTEXT_ATTRIBUTE = "ATLAS_MAIN";
    private static final String LOCAL_WAR_DIR = "./src/main/webapp";

    private static final boolean IS_PROCESSING = Boolean.parseBoolean(
            System.getProperty("processing.config")
    );

    private static final String SAMPLING_PERIOD_PROPERTY = "samplingPeriodMinutes";
    private static final int DEFAULT_SAMPLING_PERIOD_MINUTES = 3;
    private static final InetSocketAddress GRAPHITE_ADDRESS = new InetSocketAddress(
            "graphite.mbst.tv",
            2003
    );
    private static final boolean GRAPHITE_REPORTING_ENABLED = Boolean.getBoolean(
            System.getProperty("metrics.graphite.enabled", "true")
    );

    public final MetricRegistry metrics = new MetricRegistry();
    private final GraphiteReporter reporter = startGraphiteReporter();

    public static void main(String[] args) throws Exception {
        if (IS_PROCESSING) {
            System.out.println(">>> Launching processing configuration");
        }
        new AtlasMain().start();
    }

    public void start() throws Exception {
        WebAppContext apiContext = createWebApp(warBase() + "/WEB-INF/web.xml", createApiServer());
        apiContext.setAttribute(CONTEXT_ATTRIBUTE, this);
        if (!IS_PROCESSING) {
            WebAppContext monitoringContext = createWebApp(
                    warBase() + "/WEB-INF/web-monitoring.xml", createMonitoringServer()
            );
            monitoringContext.setAttribute(CONTEXT_ATTRIBUTE, this);
        }
    }

    private WebAppContext createWebApp(String descriptor, final Server server) throws Exception {
        WebAppContext ctx = new WebAppContext(warBase(), "/");
        ctx.setDescriptor(descriptor);
        server.setHandler(ctx);
        server.start();
        return ctx;
    }

    private String warBase() {
        if (new File(LOCAL_WAR_DIR).exists()) {
            return LOCAL_WAR_DIR;
        }
        ProtectionDomain domain = AtlasMain.class.getProtectionDomain();
        return domain.getCodeSource().getLocation().toString();
    }

    private Server createApiServer() throws Exception {
        int requestThreads;
        String requestThreadsString = System.getProperty(
                SERVER_REQUEST_THREADS_OVERRIDE_PROPERTY_NAME
        );
        if (Strings.isNullOrEmpty(requestThreadsString)) {
            requestThreads = DEFAULT_SERVER_REQUEST_THREADS;
        } else {
            requestThreads = Integer.parseInt(requestThreadsString);
        }

        return createServer(
                defaultPort(),
                SERVER_PORT_OVERRIDE_PROPERTY_NAME,
                requestThreads,
                SERVER_ACCEPT_QUEUE_SIZE,
                SERVER_REQUEST_THREAD_PREFIX
        );
    }

    private Server createMonitoringServer() throws Exception {
        int defaultAcceptQueueSize = 0;
        return createServer(
                MONITORING_DEFAULT_PORT,
                MONITORING_PORT_OVERRIDE_PROPERTY_NAME,
                MONITORING_REQUEST_THREADS,
                defaultAcceptQueueSize,
                MONITORING_REQUEST_THREAD_PREFIX
        );
    }

    private Server createServer(
            int defaultPort,
            String portPropertyName,
            int maxThreads,
            int acceptQueueSize,
            String threadNamePrefix
    ) {
        Server server = new Server(createRequestThreadPool(maxThreads, threadNamePrefix));
        createServerConnector(
                server,
                createHttpConnectionFactory(),
                defaultPort,
                portPropertyName,
                acceptQueueSize
        );
        return server;
    }

    private QueuedThreadPool createRequestThreadPool(int maxThreads, String threadNamePrefix) {
        QueuedThreadPool pool = new InstrumentedQueuedThreadPool(
                metrics,
                getSamplingPeriod(),
                maxThreads
        );
        pool.setName(threadNamePrefix);
        return pool;
    }

    private void createServerConnector(
            Server server,
            HttpConnectionFactory connectionFactory,
            int defaultPort,
            String portPropertyName,
            int acceptQueueSize
    ) {
        Executor defaultExecutor = null;
        Scheduler defaultScheduler = null;
        ByteBufferPool defaultByteBufferPool = null;

        ServerConnector connector = new ServerConnector(
                server,
                defaultExecutor,
                defaultScheduler,
                defaultByteBufferPool,
                -1, // use default == number of logical CPU cores
                -1, // use default
                connectionFactory
        );

        connector.setPort(getPort(defaultPort, portPropertyName));
        connector.setAcceptQueueSize(acceptQueueSize);
        server.setConnectors(new Connector[]{connector});
    }

    private HttpConnectionFactory createHttpConnectionFactory() {
        HttpConfiguration config = new HttpConfiguration();
        config.setRequestHeaderSize(8192);
        config.setResponseHeaderSize(1024);
        return new HttpConnectionFactory(config);
    }

    private int getPort(int defaultPort, String portProperty) {
        String customPort = System.getProperty(portProperty);
        if (customPort != null) {
            return Integer.parseInt(customPort);
        }
        return defaultPort;
    }

    private int getSamplingPeriod() {
        String customSamplingDuration = System.getProperty(SAMPLING_PERIOD_PROPERTY);
        if (customSamplingDuration != null) {
            return Integer.parseInt(customSamplingDuration);
        }
        return DEFAULT_SAMPLING_PERIOD_MINUTES;
    }

    private int defaultPort() {
        return IS_PROCESSING ? PROCESSING_DEFAULT_PORT : API_DEFAULT_PORT;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> getMetrics(
            Object atlasMain
    ) throws
            IllegalArgumentException,
            SecurityException,
            IllegalAccessException,
            InvocationTargetException {

        Class<?> clazz = atlasMain.getClass();
        if (!Objects.equals(clazz.getCanonicalName(), AtlasMain.class.getCanonicalName())) {
            throw new IllegalArgumentException(
                    "Parameter must be instance of " + AtlasMain.class.getCanonicalName()
            );
        }
        try {
            return (Map<String, String>) clazz.getDeclaredMethod(METRIC_METHOD_NAME)
                    .invoke(atlasMain);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                    "Couldn't find method " +
                            METRIC_METHOD_NAME +
                            ": Perhaps a mismatch between AtlasMain objects across classloaders?",
                    e
            );
        }
    }

    @SuppressWarnings("unchecked")
    public static MetricRegistry getMetricRegistry(
            Object atlasMain
    ) throws
            IllegalArgumentException,
            SecurityException,
            IllegalAccessException,
            InvocationTargetException {

        Class<?> clazz = atlasMain.getClass();
        if (!Objects.equals(clazz.getCanonicalName(), AtlasMain.class.getCanonicalName())) {
            throw new IllegalArgumentException(
                    "Parameter must be instance of " + AtlasMain.class.getCanonicalName()
            );
        }
        try {
            return (MetricRegistry) clazz.getDeclaredMethod(METRIC_REGISTRY_METHOD_NAME)
                    .invoke(atlasMain);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                    "Couldn't find method " +
                            METRIC_REGISTRY_METHOD_NAME +
                            ": Perhaps a mismatch between AtlasMain objects across classloaders?",
                    e
            );
        }
    }

    private GraphiteReporter startGraphiteReporter() {
        metrics.registerAll(
                new GarbageCollectorMetricSet(ManagementFactory.getGarbageCollectorMXBeans())
        );
        metrics.registerAll(new MemoryUsageGaugeSet());
        metrics.registerAll(new ThreadStatesGaugeSet());
        metrics.registerAll(new JvmAttributeGaugeSet());

        try {
            GraphiteReporter reporter = GraphiteReporter.forRegistry(metrics)
                    .prefixedWith("atlas-owl-api.".concat(InetAddress.getLocalHost().getHostName()))
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .filter(MetricFilter.ALL)
                    .build(new Graphite(GRAPHITE_ADDRESS));

            if (!IS_PROCESSING && GRAPHITE_REPORTING_ENABLED) {
                reporter.start(30, TimeUnit.SECONDS);
                System.out.println("Started Graphite reporter");
            }

            return reporter;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public Map<String, String> getMetrics() {
        DecimalFormat dpsFormat = new DecimalFormat("0.00");

        Builder<String, String> metricsResults = ImmutableMap.builder();
        for (Entry<String, Histogram> entry : metrics.getHistograms().entrySet()) {
            metricsResults.put(entry.getKey() + "-mean", dpsFormat.format(entry.getValue().getSnapshot().getMean()));
            metricsResults.put(entry.getKey() + "-max", Long.toString(entry.getValue().getSnapshot().getMax()));
            metricsResults.put(entry.getKey() + "-99th", dpsFormat.format(entry.getValue().getSnapshot().get99thPercentile()));
        }

        return metricsResults.build();
    }

    public MetricRegistry getMetricRegistry() {
        return metrics;
    }
}
