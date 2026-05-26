package ai.tensor7.actors;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class PulsarCumulativePositiveAcksActor {
    private static final Logger log = LoggerFactory.getLogger(PulsarPositiveAcksActor.class);

    public interface Command {}

    public record DoPositiveAck(MessageId messageId) implements Command {}
    public enum StopProcessing implements Command { INSTANCE }

    private final Consumer<byte[]> consumer;
    private long msgCounter = 0; // we don't care if this rolls over
    private long cumulativeAckOnMsgCount;
    private Optional<MessageId> lastMessageId = Optional.empty();
    private final ActorContext<Command> context;

    private PulsarCumulativePositiveAcksActor(ActorContext<Command> context, Consumer<byte[]> consumer) {
        this.context = context;
        this.consumer = consumer;
    }

    public static Behavior<Command> create(Consumer<byte[]> consumer) {
        return Behaviors.setup(context -> new PulsarCumulativePositiveAcksActor(context, consumer).init());
    }

    private Behavior<Command> init() {
        return Behaviors.setup(context -> {
            cumulativeAckOnMsgCount = context.getSystem().settings().config().getLong("app.cumulative-ack-on-msg-count");
            return Behaviors.receive(Command.class)
                    .onMessage(DoPositiveAck.class, msg -> {
                        msgCounter++;
                        if (msgCounter % cumulativeAckOnMsgCount == 0) {
                            consumer.acknowledgeCumulativeAsync(msg.messageId)
                                    .exceptionally(throwable -> {
                                        context.getLog().error("An exception occurred while cumulatively acknowledging Pulsar for message id " + msg.messageId, throwable);
                                        return null;
                                    });
                        }
                        else {
                            lastMessageId = Optional.of(msg.messageId);
                        }
                        return Behaviors.same();
                    })
                    .onMessageEquals(StopProcessing.INSTANCE, this::onStopProcessing)
                    .build();
        });
    }

    private Behavior onStopProcessing() {
        if (!lastMessageId.isEmpty()) {
            MessageId msgId = lastMessageId.get();
            consumer.acknowledgeCumulativeAsync(msgId)
                    .exceptionally(throwable -> {
                        context.getLog().error("An exception occurred (while stopping) with cumulatively acknowledgement for message id " + msgId, throwable);
                        return null;
                    });
        }
        return Behaviors.stopped();
    }
}
