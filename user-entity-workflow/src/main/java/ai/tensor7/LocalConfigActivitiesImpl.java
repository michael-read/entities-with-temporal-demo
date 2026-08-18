package ai.tensor7;

import ai.tensor7.model.EntityConfig;
import io.temporal.workflow.Workflow;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public class LocalConfigActivitiesImpl implements LocalConfigActivities {
    private final Logger log = Workflow.getLogger(LocalConfigActivitiesImpl.class);

    @Override
    public EntityConfig getEntityConfig() {
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

        Duration maxAwaitTime;

        maxAwaitTime = Duration.parse(appProps.getProperty("user-entity-polling-rate"));
        // override setting from environment
        if (env.containsKey("USER_ENTITY_POLLLING_RATE")) {
            maxAwaitTime = Duration.parse(env.get("USER_ENTITY_POLLLING_RATE"));
        }
        return new EntityConfig(maxAwaitTime);
    }
}
