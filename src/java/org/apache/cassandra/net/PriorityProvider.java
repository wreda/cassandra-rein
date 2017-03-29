package org.apache.cassandra.net;

public interface PriorityProvider {
        PriorityTuple getPriority();
        int getBatchSize();
}