package ai.tensor7.actors;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;

import java.util.Optional;

public class PulsarPositiveAcksActor {

    public interface Command {}

    public record DoPositiveAck(MessageId messageId) implements Command {}
    public enum StopProcessing implements Command { INSTANCE }

    private final Consumer<byte[]> consumer;

    private final ActorContext<Command> context;

    private PulsarPositiveAcksActor(ActorContext<Command> context, Consumer<byte[]> consumer) {
        this.context = context;
        this.consumer = consumer;
    }

    public static Behavior<Command> create(Consumer<byte[]> consumer) {
        return Behaviors.setup(context -> new PulsarPositiveAcksActor(context, consumer).init());
    }

    private Behavior<Command> init() {
        return Behaviors.setup(context -> {
            return Behaviors.receive(Command.class)
                .onMessage(DoPositiveAck.class, msg -> {
                        // we're not waiting for the future to complete here
                        consumer.acknowledgeAsync(msg.messageId)
                            .exceptionally(throwable -> {
                                context.getLog().error("An exception occurred while cumulatively acknowledging Pulsar for message id " + msg.messageId, throwable);
                                return null;
                            });
                    return Behaviors.same();
                })
                .onMessageEquals(StopProcessing.INSTANCE, this::onStopProcessing)
                .build();
        });
    }

    private Behavior onStopProcessing() {
        return Behaviors.stopped();
    }
}
