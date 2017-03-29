package org.apache.cassandra.net;

import java.util.Comparator;

import org.apache.cassandra.concurrent.AbstractTracingAwareExecutorService.FutureTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Priority comparator for BRB (only used for read runnables)
 */
public class PriorityComparator implements Comparator<FutureTask<?>> {
    protected static final Logger logger = LoggerFactory.getLogger(PriorityComparator.class);
    @Override
    public int compare(FutureTask<?> first, FutureTask<?> second) {
         if(first instanceof PriorityProvider && second instanceof PriorityProvider)
                 return ((PriorityProvider)first).getPriority().compareTo(((PriorityProvider)second).getPriority());
         else
             logger.info(first.toString() + second.toString());
             throw new UnsupportedOperationException();
    }

}