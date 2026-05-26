package ai.tensor7;

import ai.tensor7.model.UserInput;
import ai.tensor7.model.UserPurchaseEvent;
import ai.tensor7.model.UserState;
import io.temporal.workflow.*;

@WorkflowInterface
public interface UserEntityWorkflow {
    @WorkflowMethod
    String create(UserInput input);

    @SignalMethod
    void purchaseEvent(UserPurchaseEvent name);

    @SignalMethod
    void exit();

    @QueryMethod
    UserState getEntity();

}
