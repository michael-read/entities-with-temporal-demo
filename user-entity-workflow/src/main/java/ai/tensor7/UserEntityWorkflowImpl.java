package ai.tensor7;

import ai.tensor7.model.UserInput;
import ai.tensor7.model.UserPurchaseEvent;
import ai.tensor7.model.UserState;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class UserEntityWorkflowImpl implements UserEntityWorkflow {

    private final Logger log = Workflow.getLogger(UserEntityWorkflowImpl.class);

    private Duration maxAwaitTime;

    boolean exitRequested = false;
    int maxHistoryLength = 0;

    UserState userState = UserState.empty();

    public UserEntityWorkflowImpl() {
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

        maxAwaitTime = Duration.parse(appProps.getProperty("user-entity-polling-rate"));
        // override setting from environment
        if (env.containsKey("USER_ENTITY_POLLLING_RATE")) {
            maxAwaitTime = Duration.parse(env.get("TEMPORAL_SERVER_TARGET"));
        }

        log.info("User Entity Workflow polling rate: {} minutes", maxAwaitTime.toMinutes());

    }

    @Override
    public String create(UserInput input) {
        if (log.isDebugEnabled()) {
            log.debug("create on UserEntityWorkflowImpl called for user ID: {}", input.userId());
        }
        // Make sure we capature the UserID as it's possible the state was initally created through a signal
        if (userState.userId().length() == 0) {
            userState = userState.withUserId(input.userId());
        }
        // is state coming from continueAsNew?
        if (input.userState().isPresent()) {
            userState = input.userState().get();
        }
        if (input.testContinueAsNew()) maxHistoryLength = 7;

        do {
            Workflow.await(maxAwaitTime, () -> exitRequested);
        } while (!exitRequested && !shouldContinueAsNew());

        if (log.isDebugEnabled()) {
            log.debug("do while stopped: exitRequested: {}, shouldContinueAsNew: {}", exitRequested, shouldContinueAsNew());
        }
        // wait until all singles are processed before moving forward
        Workflow.await(Workflow::isEveryHandlerFinished);
        if (exitRequested) {
            String msg = "exit requested, workflow terminated for user ID: " + input.userId();
            if (log.isDebugEnabled()) { log.debug(msg); }
            return msg;
        }
        else {
            if (log.isDebugEnabled()) {
                log.debug("ContinueAsNew being requested for user {}", input.userId());
            }
            // Create a workflow stub that will be used to continue this workflow as a new
            UserEntityWorkflow continueAsNew = Workflow.newContinueAsNewStub(UserEntityWorkflow.class);

            // Request that the new run will be invoked by the Temporal system:
            continueAsNew.create(new UserInput(input.userId(), Optional.of(userState), input.testContinueAsNew()));
            String msg = "continued as new; results passed to next run for user ID: " + input.userId();
            if (log.isDebugEnabled()) { log.debug(msg); }
            return msg;
        }
    }

    private boolean shouldContinueAsNew() {
        if (Workflow.getInfo().isContinueAsNewSuggested()) {
            return true;
        }
        // This is just for ease-of-testing.  In production, we trust temporal to tell us when to
        // continue as new.
        long historyLength = Workflow.getInfo().getHistoryLength();
        boolean continueAsNew = maxHistoryLength > 0 && historyLength > maxHistoryLength;
        if (log.isDebugEnabled() && maxHistoryLength > 0) {
            log.debug("shouldContinueAsNew for userId {}: historyLenght: {}, continueAsNew: {}", userState.userId(), historyLength, continueAsNew);
        }
        return continueAsNew;
    }

    @Override
    public void purchaseEvent(UserPurchaseEvent event) {
/*
        if (log.isDebugEnabled()) {
            log.debug("UserEntityWorkflowImpl.purchaseEvent received for user ID {}: {}", userState.userId(), event);
        }
*/
        userState = userState.increment(event.price(), Optional.of(event.msgId()));
    }

    @Override
    public void exit() {
        if (log.isDebugEnabled()) {
            log.debug("UserEntityWorkflowImpl.exit() called for user ID: {}", userState.userId());
        }
        exitRequested = true;
    }

    @Override
    public UserState getEntity() {
        return userState;
    }

}
