package ai.tensor7;

import ai.tensor7.model.UserInput;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.tensor7.model.UserPurchaseEvent;
import ai.tensor7.model.UserState;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowStub;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.testing.TestWorkflowExtension;
import io.temporal.worker.Worker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;

public class UserEntityWorkflowTest {

    static final String userId = "Mike";

    @RegisterExtension
    public static final TestWorkflowExtension testWorkflowExtension =
            TestWorkflowExtension.newBuilder()
                    .setWorkflowTypes(UserEntityWorkflowImpl.class)
                    .setDoNotStart(true)
                    .build();

    @Test
    public void startUserEntityWorkflowAndExit(TestWorkflowEnvironment testEnv, Worker worker,
                                          UserEntityWorkflow workflow) {

        testEnv.start();

        // Start workflow asynchronously
        WorkflowClient.start(workflow::create, new UserInput(userId, Optional.empty(), true));

        // end workflow
        workflow.exit();

        // Wait for result and assert
        String result = WorkflowStub.fromTyped(workflow).getResult(String.class);
        assertEquals("exit requested, workflow terminated for user ID: " + userId, result);

    }

    @Test
    public void testUserEntityWorkflowSignals(TestWorkflowEnvironment testEnv, Worker worker,
                                       UserEntityWorkflow workflow) {

        testEnv.start();

        // Start workflow asynchronously
        WorkflowClient.start(workflow::create, new UserInput(userId, Optional.empty(), true));

        // Send signals via the typed stub
        workflow.purchaseEvent(new UserPurchaseEvent("1", 1, userId, "t-shirt", 1,10));
        workflow.purchaseEvent(new UserPurchaseEvent("2", 1, userId, "shorts", 2,15));
        workflow.purchaseEvent(new UserPurchaseEvent("3", 1, userId, "briefs", 3,10));
        workflow.purchaseEvent(new UserPurchaseEvent("4", 1, userId, "sox", 4,5));

        // end workflow
        workflow.exit();

        // Wait for result and assert
        String result = WorkflowStub.fromTyped(workflow).getResult(String.class);
        assertEquals("exit requested, workflow terminated for user ID: " + userId, result);

        // query the state after the workflow completes to make sure all signals have been processed
        UserState state = workflow.getEntity();

        assertEquals(4, state.totalPurchases());
        assertEquals(40, state.amountSpent());

    }

    @Test
    public void testUserEntityWorkflowIgnoreDups(TestWorkflowEnvironment testEnv, Worker worker,
                                              UserEntityWorkflow workflow) {

        testEnv.start();

        // Start workflow asynchronously
        WorkflowClient.start(workflow::create, new UserInput(userId, Optional.empty(), true));

        // Send signals via the typed stub
        workflow.purchaseEvent(new UserPurchaseEvent("1", 1, userId, "t-shirt", 1,10));
        workflow.purchaseEvent(new UserPurchaseEvent("2", 1, userId, "shorts", 2,15));
        workflow.purchaseEvent(new UserPurchaseEvent("3", 1, userId, "briefs", 3,10));
        workflow.purchaseEvent(new UserPurchaseEvent("4", 1, userId, "sox", 4,5));

        // send duplicates
        workflow.purchaseEvent(new UserPurchaseEvent("3", 1, userId, "briefs", 3,10));
        workflow.purchaseEvent(new UserPurchaseEvent("4", 1, userId, "sox", 4,5));

        // end workflow
        workflow.exit();

        // Wait for result and assert
        String result = WorkflowStub.fromTyped(workflow).getResult(String.class);
        assertEquals("exit requested, workflow terminated for user ID: " + userId, result);

        // query the state after the workflow completes to make sure all signals have been processed
        UserState state = workflow.getEntity();

        assertEquals(4, state.totalPurchases());
        assertEquals(40, state.amountSpent());
    }

    @Test
    public void testUserEntityWorkflowStartAsNew(TestWorkflowEnvironment testEnv, Worker worker,
                                                 UserEntityWorkflow workflow) throws InterruptedException {

        testEnv.start();

        // Start workflow asynchronously
        WorkflowClient.start(workflow::create, new UserInput(userId, Optional.empty(), true));

        // Send signals via the typed stub
        ArrayList<UserPurchaseEvent> events = new ArrayList<>();
        
        events.add(new UserPurchaseEvent("1", 1, userId, "t-shirt", 1,10));
        events.add(new UserPurchaseEvent("2", 1, userId, "shorts", 2,15));
        events.add(new UserPurchaseEvent("3", 1, userId, "briefs", 3,10));
        events.add(new UserPurchaseEvent("4", 1, userId, "sox", 4,5));
        events.add(new UserPurchaseEvent("5", 1, userId, "t-shirt", 1,10));
        events.add(new UserPurchaseEvent("6", 1, userId, "shorts", 2,15));
        events.add(new UserPurchaseEvent("7", 1, userId, "briefs", 3,10));
        events.add(new UserPurchaseEvent("8", 1, userId, "sox", 4,5));
        events.add(new UserPurchaseEvent("9", 1, userId, "briefs", 3,10));
        events.add(new UserPurchaseEvent("10", 1, userId, "sox", 4,5));

        for (UserPurchaseEvent event : events) {
            workflow.purchaseEvent(event);
            Thread.sleep(Duration.ofSeconds(9)); // we need to wait since the workflow only checks once a minute
        }
        
        // end workflow
        workflow.exit();

        // Wait for result and assert
        String result = WorkflowStub.fromTyped(workflow).getResult(String.class);
        assertEquals("exit requested, workflow terminated for user ID: " + userId, result);

        // query the state after the workflow completes to make sure all signals have been processed
        UserState state = workflow.getEntity();

        assertEquals(10, state.totalPurchases());
        assertEquals(95, state.amountSpent());
    }
}