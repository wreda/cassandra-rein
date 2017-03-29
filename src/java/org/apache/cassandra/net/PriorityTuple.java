package org.apache.cassandra.net;

import org.apache.cassandra.utils.Pair;

public class PriorityTuple extends Pair<Double, Long> implements Comparable<PriorityTuple>
{

    public PriorityTuple(Double priority, Long timestamp)
    {
        super(priority, timestamp);
    }

    public int compareTo(PriorityTuple other)
    {
        int k = left.compareTo(other.left);
        if (k > 0) return 1;
        if (k < 0) return -1;

        k = right.compareTo(other.right);
        if (k > 0) return 1;
        if (k < 0) return -1;

        return 0;
    }

    public static PriorityTuple create(Double priority, Long timestamp)
    {
        return new PriorityTuple(priority, timestamp);
    }
}