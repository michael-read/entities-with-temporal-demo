package ai.tensor7.app;

import ai.tensor7.util.MetricsUtils;
import com.sun.net.httpserver.HttpServer;
import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import ai.tensor7.actors.PulsarCumulativePositiveAcksActor;
import ai.tensor7.actors.PulsarNegativeAcksActor;
import ai.tensor7.app.pulsar.serialization.UserPurchaseProto;
import ai.tensor7.model.UserInput;
import ai.tensor7.model.UserPurchaseEvent;
import ai.tensor7.util.CoordinatedShutdownNoPulsarBroker;
import akka.Done;
import akka.NotUsed;
import akka.actor.CoordinatedShutdown;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.ActorTags;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.KillSwitches;
import akka.stream.RestartSettings;
import akka.stream.SharedKillSwitch;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.*;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.common.reporter.MicrometerClientStatsReporter;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;

import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static akka.actor.typed.javadsl.AskPattern.ask;

public class UserEventConsumer {
    private static final Logger log = LoggerFactory.getLogger(UserEventConsumer.class);

    public interface Command {}
    private enum CreatePulsarClient implements Command {INSTANCE}
    private enum StartMsgConsumption implements Command {INSTANCE}
    private record InternalInitiateShutdown(ActorRef<Done> replyTo) implements Command {}
    public record ConsumerStopped(String reason) implements Command {}
    public enum StopProcessing implements Command { INSTANCE }

    private final ActorContext<Command> context;
    private final String consumerTopic;
    private final String subscriptionName;
    private PulsarClient pulsarClient;

    private int childTerminatedCount = 0;

    final SharedKillSwitch killSwitch = KillSwitches.shared("UserEventsProcessorKillSwitch");

    private ActorRef<PulsarNegativeAcksActor.Command> pulsarNegativeAcksActor;
    private ActorRef<PulsarCumulativePositiveAcksActor.Command> pulsarPositiveAcksActor;
    private Consumer<byte[]> consumer;

    private final HttpServer scrapeEndpoint;
    private final WorkflowClient temporalClient;
    private final String temporalWorkflowIdPrefix;
    private final String temporalTaskQueue;

    public static Behavior<Command> createGuardian() {
        return Behaviors.setup(context -> new UserEventConsumer(context).init());
    }
    public UserEventConsumer(ActorContext<Command> context) {
        this.context = context;
        this.consumerTopic = context.getSystem().settings().config().getString("app.pulsar-consumer-topic");
        this.subscriptionName = context.getSystem().settings().config().getString("app.pulsar-subscription-name");
        context.getLog().info("The Pulsar Consumer Topic is {}", consumerTopic);

        // Set up prometheus registry and stats reported
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        // Set up a new scope, report every 1 second
        Scope scope =
                new RootScopeBuilder()
                        // shows how to set custom tags
/*
                        .tags(
                                ImmutableMap.of(
                                        "starterCustomTag1",
                                        "starterCustomTag1Value",
                                        "starterCustomTag2",
                                        "starterCustomTag2Value"))
*/
                        .reporter(new MicrometerClientStatsReporter(registry))
                        .reportEvery(com.uber.m3.util.Duration.ofSeconds(1));
        // Start the prometheus scrape endpoint for starter metrics
        scrapeEndpoint = MetricsUtils.startPrometheusScrapeEndpoint(registry, context.getSystem().settings().config().getInt("app.temporal-prometheus-metrics-port"));

        // Add metrics scope to workflow service stub options, preserving env config
        WorkflowServiceStubs temporalService = WorkflowServiceStubs.newServiceStubs(
                WorkflowServiceStubsOptions.newBuilder()
                        .setMetricsScope(scope) // Add metrics scope to workflow service stub options, preserving env config
                        .setTarget(context.getSystem().settings().config().getString("app.temporal-server-target"))
                        .setEnableHttps(context.getSystem().settings().config().getBoolean("app.temporal-server-https-enabled"))
                        .build());
        temporalClient = WorkflowClient.newInstance(temporalService);

        temporalWorkflowIdPrefix = context.getSystem().settings().config().getString("app.temporal-workflow-id-prefix");
        temporalTaskQueue = context.getSystem().settings().config().getString("app.temporal-task-queue");
    }

    private Behavior<Command> init() {
        return Behaviors.setup(context -> {

            CoordinatedShutdown.get(context.getSystem())
                    .addTask(
                            CoordinatedShutdown.PhaseBeforeServiceUnbind(),
                            "shutdownStreams",
                            () ->
                                    ask(context.getSelf(), InternalInitiateShutdown::new, Duration.ofSeconds(5), context.getSystem().scheduler()));

            context.getSelf().tell(CreatePulsarClient.INSTANCE);

            return Behaviors.receive(Command.class)
                    .onMessageEquals(CreatePulsarClient.INSTANCE, this::onCreatePulsarClient)
                    .onMessage(InternalInitiateShutdown.class, this::onInternalInitiateShutdown)
                    .onMessageEquals(StartMsgConsumption.INSTANCE, this::onStartMsgConsumption)
                    .build();
        });
    }

    private String getPulsarConnectionURL() {
        return String.format("pulsar://%s:6650", context.getSystem().settings().config().getString("app.pulsar-host"));
    }

    private void initiateCoordinatedShutdown() {
        CompletionStage<Done> done =
                CoordinatedShutdown.get(context.getSystem()).runAll(new CoordinatedShutdownNoPulsarBroker(getPulsarConnectionURL()));
        done.toCompletableFuture().join();
        scrapeEndpoint.stop(1);
        log.error("Start Coordinated Shutdown done");
    }

    private Behavior<Command> onCreatePulsarClient() {
        return Behaviors.setup(context -> {
            String pulsarConnection = getPulsarConnectionURL();
            context.getLog().info("Pulsar Connection is {}", pulsarConnection);

            // set up Pulsar Client
            try {
                pulsarClient = PulsarClient.builder()
                        .serviceUrl(pulsarConnection)
                        .build();
                context.getLog().info("Connected to Pulsar via {}", pulsarConnection);
                context.pipeToSelf(
                        // just testing to make sure the broker is there first
                        pulsarClient.getPartitionsForTopic(consumerTopic), (response, throwable) -> {
                            if (throwable != null) {
                                context.getLog().error("An exception occurred while retrieving the partition count", throwable);
                                initiateCoordinatedShutdown();
                                return null;
                            } else {
                                return StartMsgConsumption.INSTANCE;
                            }
                        }
                );
            } catch (PulsarClientException ex) {
                context.getLog().error("An exception occurred in the Pulsar client:", ex);
                initiateCoordinatedShutdown();
            }
            return Behaviors.same();
        });
    }

    private Behavior<Command> onInternalInitiateShutdown(InternalInitiateShutdown msg) {
        log.info("onInternalInitiateShutdown starting coordinated shutdown.");
        return Behaviors.same();
    }

    private Behavior<Command> onStartMsgConsumption() {
        return Behaviors.setup(context -> {
            try {
                this.consumer = pulsarClient.newConsumer()
                        .topic(consumerTopic)
                        .subscriptionName(subscriptionName)
                        .ackTimeout(20, TimeUnit.SECONDS)
//                        .subscriptionType(SubscriptionType.Key_Shared)
                        .subscriptionType(SubscriptionType.Failover)
                        .subscribe();
                context.getLog().info("Pulsar Consumer Name is: {}", consumer.getConsumerName());

                // create a separate actor on its own blocking dispatcher to handle negative Pulsar acks
                this.pulsarNegativeAcksActor = context.spawn(
                        PulsarNegativeAcksActor.create(consumer),
                        String.format("PulsarNegativeAcksActor-%s", consumer.getConsumerName()),
                        ActorTags.create("PulsarNegativeAcksActor").withDispatcherFromConfig("negative-acks-dispatcher")
                );
                context.watch(pulsarNegativeAcksActor);

                // since we're using an exclusive subscription we can use cumalative actor
                this.pulsarPositiveAcksActor = context.spawn(
                        PulsarCumulativePositiveAcksActor.create(consumer),
                        String.format("PulsarPositiveAcksActor-%s", consumer.getConsumerName()),
                        ActorTags.create("PulsarPositiveAcksActor")
                );
                context.watch(pulsarPositiveAcksActor);

            } catch (PulsarClientException ex) {
                context.getLog().error("Pulsar consumer creation failure", ex);
                initiateCoordinatedShutdown();
            }

            CompletionStage<Done> stream = startConsumingFromTopic(context);
            context.pipeToSelf(stream,
                    (ok, ex) -> {
                        if (ok != null) {
                            return new ConsumerStopped("Consuming from topic received Done");
                        }
                        else if (ex != null) {
                            return new ConsumerStopped(String.format("Consumer stopped because of %s", ex.getMessage()));
                        }
                        else {
                            return new ConsumerStopped("Consuming stopped for some unknown reason");
                        }
                    }
            );

            return Behaviors.receive(Command.class)
                    .onSignal(Terminated.class, this::onWatchedActorTerminated)
                    .onMessage(ConsumerStopped.class, this::onConsumerStopped)
                    .onMessageEquals(StopProcessing.INSTANCE, this::onStopProcessing)
                    .onMessage(InternalInitiateShutdown.class, this::onInternalInitiateShutdown)
                    .build();

        });
    }

    CompletionStage<Done> startConsumingFromTopic(ActorContext<Command> context) {
        return getPulsarAsyncSource(consumer)
//                .viaMat(killSwitch.flow(), Keep.right())
                .via(getProtoFlow())
                .toMat(Sink.ignore(), Keep.right())
                .named("consumer")
                .run(context.getSystem()
                );
    }


    private Behavior<Command> onConsumerStopped(Command message) {
        context.getLog().info("onConsumerStopped received... reason {}.", message);
        try {
            consumer.close();
        } catch (PulsarClientException e) {
            throw new IllegalStateException("Closing consumer", e);
        }
        // making sure these stop in an orderly fashion
        pulsarNegativeAcksActor.tell(PulsarNegativeAcksActor.StopProcessing.INSTANCE);
        pulsarPositiveAcksActor.tell(PulsarCumulativePositiveAcksActor.StopProcessing.INSTANCE);
        return Behaviors.same();
    }

    private Behavior<Command> onWatchedActorTerminated(Terminated terminated) {
        childTerminatedCount++;
        /*
        accounting for both positive and negative ack actors being stopped.
         */
        if (childTerminatedCount >= 2) {
            context.getLog().info("positive / negative acks actors have stopped. Shutting down.");
            initiateCoordinatedShutdown();
        }

        return Behaviors.same();

    }

    private Behavior<Command> onStopProcessing() {
        log.info("onStopProccessing received...");
        if (killSwitch != null) {
            killSwitch.shutdown();
            log.info("onStopProccessing killSwitch activated...");
        }
        return Behaviors.same();
    }

    private Source<Message<byte[]>, NotUsed> getPulsarAsyncSource(Consumer<byte[]> consumer) {
        RestartSettings settings =
                RestartSettings.create(
                                Duration.ofSeconds(3), // min backoff
                                Duration.ofSeconds(30), // max backoff
                                0.2 // adds 20% "noise" to vary the intervals slightly
                        )
                        .withMaxRestarts(
                                20, Duration.ofMinutes(5)
                        ); // limits the amount of restarts to 20 within 5 minutes
        return RestartSource.withBackoff(
                settings, () -> Source.repeat(NotUsed.getInstance())
                        .mapAsyncUnordered(20, (element) ->  {
                            try {
                                return consumer.receiveAsync();
                            }
                            catch (Exception ex) {
                                log.error("An exception occurred while consuming from Pulsar {}, skipping...", ex.getMessage());
                                return null;
                            }
                        })
                        .named("PulsarAsyncSource")
        );
    }

    public Flow<Message<byte[]>, WorkflowExecution, SharedKillSwitch> getProtoFlow() {
        //return Flow.<Message<byte[]>>create().mapAsyncUnordered(1200, msg -> {
        return Flow.<Message<byte[]>>create().map(msg -> {
                    try {
                        final UserPurchaseProto record = UserPurchaseProto.parseFrom(msg.getData());
                        UserPurchaseEvent event = new UserPurchaseEvent(
                                record.getMsgId(),
                                record.getMsgTime(),
                                record.getUserId(),
                                record.getProduct(),
                                record.getQuantity(),
                                record.getPrice()
                        );
                        UserInput userInput = new UserInput(record.getUserId(), Optional.empty(), false);

                        WorkflowStub untypedWorkflowStub = temporalClient.newUntypedWorkflowStub("UserEntityWorkflow",
                                WorkflowOptions.newBuilder()
                                        .setWorkflowId(temporalWorkflowIdPrefix + record.getUserId())
                                        .setTaskQueue(temporalTaskQueue)
                                        .build());

                        WorkflowExecution wfexec = untypedWorkflowStub.signalWithStart("purchaseEvent", new Object[]{event}, new Object[]{userInput});
                        pulsarPositiveAcksActor.tell(new PulsarCumulativePositiveAcksActor.DoPositiveAck(msg.getMessageId()));
                        return wfexec;

                    } catch (Exception e) {
                        pulsarNegativeAcksActor.tell(new PulsarNegativeAcksActor.DoNegativeAck(msg.getMessageId()));
                        log.error("An exception occurred while consuming message id:" + msg.getMessageId(), e);
                        return null;
                    }
                })
                .viaMat(killSwitch.flow(), Keep.right())
                .named("askFlow");
    }


    public static void main(String[] args) {
        ActorSystem<Command> system = ActorSystem.create(createGuardian(), "PulsarPartitionTracker");
        system.getWhenTerminated();
    }

}
