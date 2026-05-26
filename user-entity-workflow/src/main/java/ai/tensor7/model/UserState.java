package ai.tensor7.model;

import java.util.ArrayList;
import java.util.Optional;

public record UserState(String userId, long totalPurchases, long amountSpent, ArrayList<String> msgIds) {

    private static final int MAXIDS = 14; // max idepempotent window size

    public static UserState empty() {
        return new UserState("", 0, 0, new ArrayList<>());
    }

    public UserState withUserId(String userId) {
        return new UserState(userId, totalPurchases, amountSpent, msgIds);
    }

    public UserState increment(long price, Optional<String> msgId) {
        // make state idempotent by ignoring messages we've seen before within the window size
        if (msgId.isPresent() && msgIds().contains(msgId.get())) {
            return this;
        }
        ArrayList<String> ids = new ArrayList<>(msgIds);
        if (msgId.isPresent()) {
            // just hold on to max IDs
            if (ids.size() >= MAXIDS) {
                ids.removeFirst();
                ids.add(msgId.get());
            }
            else {
                ids.add(msgId.get());
            }
        }
        return new UserState(this.userId, totalPurchases + 1, amountSpent + price, ids);
    }
}