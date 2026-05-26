package ai.tensor7.app;

import ai.tensor7.actors.CounterActor;
import ai.tensor7.app.pulsar.serialization.UserPurchaseProto;
import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public class UserEventProducer {
    private static final Logger log = LoggerFactory.getLogger(UserEventProducer.class);

    private UserEventProducer(ActorContext<NotUsed> context) {
    }

    public static Behavior<NotUsed> createGuardian() {
        return Behaviors.setup(context -> new UserEventProducer(context).init());
    }

    private Behavior<NotUsed> init() {
        return Behaviors.setup(context -> {

            String pulsarConnection = String.format("pulsar://%s:6650", context.getSystem().settings().config().getString("app.pulsar-host"));
            String pulsarTopic = context.getSystem().settings().config().getString("app.pulsar-consumer-topic");

            int batchSize = context.getSystem().settings().config().getInt("app.batch-size");
            int batchWaitMilliseconds = context.getSystem().settings().config().getInt("app.batch-wait-milliseconds");

            PulsarClient pulsarClient = PulsarClient.builder()
                    .serviceUrl(pulsarConnection)
                    .build();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic(pulsarTopic)
                    .enableBatching(true)
                    .batcherBuilder(BatcherBuilder.KEY_BASED)
                    .batchingMaxMessages(batchSize)
                    .batchingMaxPublishDelay(batchWaitMilliseconds, TimeUnit.MILLISECONDS)
                    .create();

            int nrUsers = 2000;
            int maxPrice = 100;
            int maxQuantity = 5;

            List<String> products = new ArrayList<>();
            products.add("java t-shirt");
            products.add("scala t-shirt");
            products.add("skis");
            products.add("climbing shoes");
            products.add("rope");

            Random random = new Random();

            ActorRef<CounterActor.Command> counterActor = context.spawn(CounterActor.create(), "CounterActor");

            CompletionStage<Done> result =
                    Source.repeat(NotUsed.getInstance())
                            .throttle(75, Duration.ofSeconds(1))
                            .map((notUsed) -> {
                                String randomEntityId = Integer.valueOf(random.nextInt(nrUsers)).toString();
                                long price = random.nextInt(1, maxPrice);
                                long quantity = random.nextInt(1, maxQuantity);
                                String product = products.get(random.nextInt(products.size()));
                                if (log.isDebugEnabled()) {
                                    log.debug("Sending message to user {} Product {} Qty {}, Price {}", randomEntityId, product, quantity, price);
                                }
                                return UserPurchaseProto.newBuilder()
                                        .setMsgId(UUID.randomUUID().toString())
                                        .setMsgTime(Instant.now().toEpochMilli())
                                        .setUserId(randomEntityId)
                                        .setProduct(product)
                                        .setQuantity(quantity)
                                        .setPrice(price)
                                        .build();

                            })
                            .mapAsyncUnordered(1000, userPurchase -> {
                                CompletableFuture<MessageId> futureId = producer.newMessage()
                                        .key(userPurchase.getUserId())
                                        .value(userPurchase.toByteArray())
                                        .sendAsync();
                                return futureId.thenApply(msgId -> {
                                    counterActor.tell(CounterActor.IncrementCount.INSTANCE);
                                    return msgId;
                                });
                            })
                            .runWith(Sink.ignore(), context.getSystem());

            // tear down
            result.thenRun (() -> context.getSystem().terminate());

            return Behaviors.empty();
        });
    }

    public static void main(String[] args) {
        ActorSystem<NotUsed> system = ActorSystem.create(createGuardian(), "producer");
        system.getWhenTerminated();
    }
}
