package ai.tensor7.actors;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

public class CounterActor extends AbstractBehavior<CounterActor.Command> {

    public interface Command {}

    public enum IncrementCount implements Command {INSTANCE}

    private long msgsProduced = 0L;

    public static Behavior<Command> create() {
        return Behaviors.setup(CounterActor::new);
    }

    private CounterActor(ActorContext<Command> context) {
        super(context);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessageEquals(IncrementCount.INSTANCE, this::onIncrementCount)
                .build();
    }

    private Behavior<Command> onIncrementCount() {
/*
        msgsProduced = msgsProduced + 1L;
        if ((msgsProduced % 100000) == 0) {
            context.getLog().info("{} messages have been produced...", msgsProduced);
        }
*/
        return Behaviors.same();
    }

}