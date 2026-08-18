package ai.tensor7;

import ai.tensor7.model.EntityConfig;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface(namePrefix = "OrderLocalConfig")
public interface LocalConfigActivities {
    @ActivityMethod
    EntityConfig getEntityConfig();

}
