/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.net;

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.util.concurrent.AtomicLongMap;
import org.apache.log4j.Priority;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.AbstractTracingAwareExecutorService;

public class MultiConcurrentLinkedPriorityQueueDRR<E> extends AbstractQueue<E>
{
    private static final Logger logger = LoggerFactory.getLogger(MultiConcurrentLinkedPriorityQueueDRR.class);

    int QUANTUM = 2;
    Random r;
    int qcount;
    List<ConcurrentLinkedQueue<E>> queues;
    List<Integer> weights;
    List<Integer> rates;
    //List<Float> DC;
    AtomicLongMap<Integer> DC;
    //Map<Integer, Float> DC = new HashMap<Integer, Float>();
    int weightSum;
    Thread scheduler;

    public MultiConcurrentLinkedPriorityQueueDRR(List<Integer> weights)
    {
        qcount = weights.size();
        for (Integer x: weights)
            weightSum += x;
        this.weights = weights;
        r =  new Random();
        queues = new ArrayList<ConcurrentLinkedQueue<E>>();
        DC = AtomicLongMap.create();
        for (int i=0; i<qcount; i++)
        {
            queues.add(new ConcurrentLinkedQueue<E>());
            DC.put(i, 0);
        }

        Runnable runnable = new Runnable()
        {
            public void run()
            {
                while (true)
                {
                    schedule();
                }
            }
        };
        scheduler = new Thread(runnable, "REQUEST-SCHEDULER");
        scheduler.start();
    }

    /**
     * Returns an iterator over the elements contained in this collection.
     *
     * @return an iterator over the elements contained in this collection
     */
    public Iterator<E> iterator()
    {
        throw new UnsupportedOperationException();
    }

    public int size()
    {
        int size = 0;
        for (int i=0; i<qcount; i++)
            size += queues.get(i).size();
        return size;
    }

    public E poll() {

        return pollAndCount(1);
    }

    public E pollAndCount(int count)
    {
        boolean force = false;

        //find a non-empty queue with non-zero DC
        for(int i=0; i < qcount; i++)
        {
            //to avoid stackoverflows force polls() regardless of DC after we reach 50 function calls
            if(count>50)
                force = true;

            E operation = tryPoll(i, force);
            if(operation != null)
            {
                //logger.trace("Count: {}", count);
                return operation;
            }
        }

        return pollAndCount(count+1);
    }

    public E tryPoll(int i, boolean force)
    {
        if(DC.get(i) >= 1 || force)
        {
            E operation = queues.get(i).poll();
            if(operation != null)
            {
                DC.decrementAndGet(i);
                return operation;
            }
        }
        return null;
    }

    public void updateQueueDC(int i, int factor)
    {
        DC.addAndGet(i, weights.get(i)*QUANTUM*factor);
    }

    public void schedule()
    {
        List<Integer> queueSizes = new ArrayList<Integer>();
        int totalSize = 0;
        int minSize = Integer.MAX_VALUE;
        int size;

        //log queue sizes and DCs (sample 1 in 1 mil)
        if(r.nextFloat() < 0.000001)
        {
            for(int i=0; i < qcount; i++)
            {
                logger.trace("Q[{}] size = {}, DC= {}",i,queues.get(i).size(), DC.get(i));
            }
        }

        //find a non-empty queue with non-zero DC
        for(int i=0; i < qcount; i++)
        {
            size = queues.get(i).size();
            if(size<minSize && size!=0)
                minSize = size;
            queueSizes.add(size);
            totalSize += size;
            if (size == 0)
                DC.put(i, 0);
            else if(DC.get(i)>0)
                return;
        }

        //if we fail and we have outstanding elements in the queue, iterate over all queues and update DC
        if(totalSize>0)
        {
            for (int i = 0; i < qcount; i++)
            {
                float factor = queues.get(i).size()/minSize;

                //FIXME remove this
                factor = 1;

                if (factor > 0)
                    updateQueueDC(i, (int)(factor));
                //updateQueueDC(i, 1);
            }
        }
    }

    @Override
    public boolean isEmpty()
    {
        for(int i=0; i<qcount; i++)
        {
            if(queues.get(i).isEmpty())
                return true;
        }
        return false;
    }

    /**
     * Retrieves, but does not remove, the head of this queue,
     * or returns <tt>null</tt> if this queue is empty.
     *
     * @return the head of this queue, or <tt>null</tt> if this queue is empty
     */
    public E peek()
    {
        for(int i=0; i<qcount; i++)
        {
            E peekabo = queues.get(i).peek();
            if(peekabo != null)
                return peekabo;
        }
        return null;
    }

    @Override
    public boolean offer(E key) {
        if (key == null)
            throw new NullPointerException();
        if(key instanceof PriorityProvider)
        {
            PriorityTuple priority = ((PriorityProvider)key).getPriority();
            //logger.info(">>> Entering queue with priority {} and batchSize {}",
            // ((PriorityProvider)key).getPriority(), ((PriorityProvider)key).getBatchSize());
            int chosenWeight = priority.left.intValue();
            int choice = weights.indexOf(chosenWeight);
            if(choice != -1)
            {
                queues.get(choice).offer(key);
                return true;
            }
        }
        int choice = r.nextInt(qcount);
        queues.get(choice).offer(key);
        return true;
    }

    public boolean add(E e) {
        return offer(e);
    }

    public void stopScheduling() throws InterruptedException {
        scheduler.join();
    }

    public String toString() {
        String queueSerialize = "";

        for(int i=0; i < qcount; i++)
        {
            queueSerialize += ("Q["+i+"]: ("+queues.get(i).size()+") ");
        }
        return queueSerialize;
    }


}
