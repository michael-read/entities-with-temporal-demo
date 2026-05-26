package ai.tensor7.model;

public record UserPurchaseEvent(
        String msgId,
        long msgTime,
        String userId,
        String product,
        long quantity,
        long price
) {}
