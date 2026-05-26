package ai.tensor7.actors;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;

public class PulsarNegativeAcksActor {

    public interface Command {}

    public record DoNegativeAck(MessageId messageId) implements Command {}
    public enum StopProcessing implements Command { INSTANCE }

    private final ActorContext<Command> context;
    private final Consumer<byte[]> consumer;

    private PulsarNegativeAcksActor(ActorContext<Command> context, Consumer<byte[]> consumer) {
        this.context = context;
        this.consumer = consumer;
    }

    public static Behavior<Command> create(Consumer<byte[]> consumer) {
        return Behaviors.setup(context -> new PulsarNegativeAcksActor(context, consumer).init());
    }

    private Behavior<Command> init() {
        return Behaviors.receive(Command.class)
                .onMessage(DoNegativeAck.class, msg -> {
                    consumer.negativeAcknowledge(msg.messageId);
                    context.getLog().warn("Negatively acked message Id {}", msg.messageId);
                    return Behaviors.same();
                })
                .onMessageEquals(StopProcessing.INSTANCE, this::onStopProcessing)
                .build();
    }

    private Behavior onStopProcessing() {
        return Behaviors.stopped();
    }
}
