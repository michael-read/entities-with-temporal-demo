package ai.tensor7.model;

import java.util.Optional;

public record UserInput(
        String userId,
        Optional<UserState> userState,
        boolean testContinueAsNew
) {}
