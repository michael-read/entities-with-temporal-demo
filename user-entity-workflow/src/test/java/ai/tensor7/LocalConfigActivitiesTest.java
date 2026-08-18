package ai.tensor7;

import ai.tensor7.model.EntityConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class LocalConfigActivitiesTest {
    
    @Test
    public void testMockedActivity() {
        LocalConfigActivities mockLocalConfigActivities = mock(LocalConfigActivities.class, withSettings().withoutAnnotations());

        EntityConfig config = new EntityConfig(
                Duration.ofMillis(100)
        );
        when(mockLocalConfigActivities.getEntityConfig()).thenReturn(config);

        assertEquals(Duration.ofMillis(100), mockLocalConfigActivities.getEntityConfig().maxPollingAwaitTime());
    }
    
}
