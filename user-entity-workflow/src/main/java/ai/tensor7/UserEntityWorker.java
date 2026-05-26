package ai.tensor7;

import ai.tensor7.util.MetricsUtils;
import com.sun.net.httpserver.HttpServer;
import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.temporal.client.WorkflowClient;
import io.temporal.common.reporter.MicrometerClientStatsReporter;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.workflow.Workflow;
import org.slf4j.Logger;


import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class UserEntityWorker {

    public static void main(String[] args) {
        final Logger log = Workflow.getLogger(UserEntityWorker.class);

        Properties appProps = new Properties();

        // Load from classpath root
        try (InputStream input = ClassLoader.getSystemResourceAsStream("app.properties")) {
            if (input == null) {
                log.error("Sorry, unable to find app.properties");
                System.exit(1);
            }
            appProps.load(input);
        }
        catch (IOException ex) {
            log.error("An IOException occurred", ex);
        }
        // capture ENV settings for potential override
        Map<String, String> env = System.getenv();

        String temporalServer = appProps.getProperty("temporal-server-target");
        // override setting from environment
        if (env.containsKey("TEMPORAL_SERVER_TARGET")) {
            temporalServer = env.get("TEMPORAL_SERVER_TARGET");
        }
        log.info("trying to connect to temporal server at {}", temporalServer);

        boolean temporalEnableHttps = Boolean.parseBoolean(appProps.getProperty("enable-https"));
        if (env.containsKey("TEMPORAL_SERVER_HTTPS_ENABLED")) {
            temporalEnableHttps = Boolean.parseBoolean(env.get("TEMPORAL_SERVER_HTTPS_ENABLED"));
        }
        log.info("temporalEnableHttps = {}", temporalEnableHttps);

        String temporalTaskQueue = appProps.getProperty("temporal-task-queue");
        if (env.containsKey("TEMPORAL_TASK_QUEUE")) {
            temporalTaskQueue = env.get("TEMPORAL_TASK_QUEUE");
        }
        log.info("temporalTaskQueue = {}", temporalTaskQueue);

        int temporalPrometheusMetricsPort = Integer.parseInt(appProps.getProperty("temporal-prometheus-metrics-port"));
        if (env.containsKey("TEMPORAL_PROMETHEUS_METRICS_PORT")) {
            temporalPrometheusMetricsPort = Integer.parseInt(env.get("TEMPORAL_PROMETHEUS_METRICS_PORT"));
        }
        log.info("Worker metrics are available at http://entity-demo-java-entity-#n:{}/metrics", temporalPrometheusMetricsPort);

        // Set up prometheus registry and stats reported
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        // Set up a new scope, report every 1 second
        Scope scope =
                new RootScopeBuilder()
                        // shows how to set custom tags
/*
                        .tags(
                                ImmutableMap.of(
                                        "workerCustomTag1",
                                        "workerCustomTag1Value",
                                        "workerCustomTag2",
                                        "workerCustomTag2Value"))
*/
                        .reporter(new MicrometerClientStatsReporter(registry))
                        .reportEvery(com.uber.m3.util.Duration.ofSeconds(1));
        // Start the prometheus scrape endpoint
        HttpServer scrapeEndpoint = MetricsUtils.startPrometheusScrapeEndpoint(registry, temporalPrometheusMetricsPort);
        // Stopping the worker will stop the http server that exposes the
        // scrape endpoint.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> scrapeEndpoint.stop(1)));

        WorkflowServiceStubs temporalService =
                WorkflowServiceStubs.newServiceStubs(
                        WorkflowServiceStubsOptions.newBuilder()
                                .setMetricsScope(scope)
                                .setTarget(temporalServer)
                                .setEnableHttps(temporalEnableHttps)
                                .build());
        WorkflowClient temporalClient = WorkflowClient.newInstance(temporalService);

        WorkerFactory factory = WorkerFactory.newInstance(temporalClient);

        Worker worker = factory.newWorker(temporalTaskQueue);

        worker.registerWorkflowImplementationTypes(UserEntityWorkflowImpl.class);

        factory.start();
    }
}
