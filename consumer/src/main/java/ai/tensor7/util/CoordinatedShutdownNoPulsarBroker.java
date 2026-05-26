package ai.tensor7.util;

import akka.actor.CoordinatedShutdown;

public class CoordinatedShutdownNoPulsarBroker implements CoordinatedShutdown.Reason {

    final String brokerName;
    public CoordinatedShutdownNoPulsarBroker(String brokerName) {
        this.brokerName = brokerName;
    }
    @Override
    public String toString() {
        return String.format("The Pulsar Broker couldn't be found for %s", brokerName);
    }
}