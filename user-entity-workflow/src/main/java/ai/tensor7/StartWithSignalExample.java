package ai.tensor7;

import ai.tensor7.model.UserInput;
import ai.tensor7.model.UserPurchaseEvent;
import ai.tensor7.model.UserState;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;

import java.util.Optional;

public class StartWithSignalExample {

  public static void main(String[] args) throws Exception {

    final WorkflowServiceStubs temporalService = WorkflowServiceStubs.newLocalServiceStubs();
    final WorkflowClient temporalClient = WorkflowClient.newInstance(temporalService);

    String userId = "Mike";
    UserInput userInput = new UserInput(userId, Optional.empty(), false);

    UserPurchaseEvent event = new UserPurchaseEvent("1", 1, userId, "t-shirt", 1,10);

    WorkflowStub untypedWorkflowStub = temporalClient.newUntypedWorkflowStub("UserEntityWorkflow",
            WorkflowOptions.newBuilder()
                    .setWorkflowId("user-entity-mike")
                    .setTaskQueue("user-entity-tasks")
                    .build());

    WorkflowExecution wfexec = untypedWorkflowStub.signalWithStart("purchaseEvent", new Object[]{event}, new Object[]{userInput});

    UserState state = untypedWorkflowStub.query("getEntity", UserState.class);

    System.out.println(state);

    System.exit(0);
  }
}
