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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData.SpeculativeRetry.RetryType;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.ReadRepairDecision;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadMultiCommand;
import org.apache.cassandra.db.ReadMultiResponse;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy.LocalReadRunnable;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Sends a read request to the replicas needed to satisfy a given ConsistencyLevel.
 *
 * Optionally, may perform additional requests to provide redundancy against replica failure:
 * AlwaysSpeculatingReadExecutor will always send a request to one extra replica, while
 * SpeculatingReadExecutor will wait until it looks like the original request is in danger
 * of timing out before performing extra reads.
 */
public abstract class AbstractReadMultiExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractReadMultiExecutor.class);

    protected final ReadMultiCommand command;
    protected final RowDigestMultiResolver resolver;
    protected final ReadCallback<ReadMultiResponse, List<Row>> handler;
    protected final TraceState traceState;
    protected List<InetAddress> targetReplicas;

    AbstractReadMultiExecutor(ReadMultiCommand command, ConsistencyLevel consistencyLevel, List<InetAddress> targetReplicas)
    {
        this.command = command;
        this.targetReplicas = targetReplicas;
        resolver = new RowDigestMultiResolver(command.ksName, command.keys.get(0), targetReplicas.size());
        traceState = Tracing.instance.get();
        handler = new ReadCallback<>(resolver, consistencyLevel, command, targetReplicas);
    }

    void execute()
    {
        //logger.info(System.currentTimeMillis() + " Sending message to actor " + this);
        ActorRef actor = MessagingService.instance().getActor(handler.endpoints);
        actor.tell(this, null);
    }

    private static boolean isLocalRequest(InetAddress replica)
    {
        return replica.equals(FBUtilities.getBroadcastAddress()) && StorageProxy.OPTIMIZE_LOCAL_REQUESTS;
    }

    protected void makeDataRequests(Iterable<InetAddress> endpoints)
    {
        makeRequests(command, endpoints);

    }

    protected void makeDigestRequests(Iterable<InetAddress> endpoints)
    {
        makeRequests(command.copy().setIsDigestQuery(true), endpoints);
    }

    private void makeRequests(ReadMultiCommand ReadMultiCommand, Iterable<InetAddress> endpoints)
    {
        MessageOut<ReadMultiCommand> message = null;
        boolean hasLocalEndpoint = false;

        for (InetAddress endpoint : endpoints)
        {
            if (isLocalRequest(endpoint))
            {
                hasLocalEndpoint = true;
                continue;
            }

            if (traceState != null)
                traceState.trace("reading {} from {}", ReadMultiCommand.isDigestQuery() ? "digest" : "data", endpoint);
            logger.trace("reading {} from {}", ReadMultiCommand.isDigestQuery() ? "digest" : "data", endpoint);
            if (message == null)
                message = ReadMultiCommand.createMessage();
            MessagingService.instance().sendRRWithFailure(message, endpoint, handler);
        }

        // We delay the local (potentially blocking) read till the end to avoid stalling remote requests.
        if (hasLocalEndpoint)
        {
            logger.trace("reading {} locally", ReadMultiCommand.isDigestQuery() ? "digest" : "data");
            //FIXME for now serialize multiread requests
            StageManager.getStage(Stage.READ).maybeExecuteImmediately(new StorageProxy.LocalReadMultiRunnable(command, handler));
        }
    }

    /**
     * Perform additional requests if it looks like the original will time out.  May block while it waits
     * to see if the original requests are answered first.
     */
    public abstract void maybeTryAdditionalReplicas();

    /**
     * Get the replicas involved in the [finished] request.
     *
     * @return target replicas + the extra replica, *IF* we speculated.
     */
    public abstract Collection<InetAddress> getContactedReplicas();

    /**
     * send the initial set of requests
     */
    public abstract void executeAsync();

    /**
     * wait for an answer.  Blocks until success or timeout, so it is caller's
     * responsibility to call maybeTryAdditionalReplicas first.
     */
    public List<Row> get() throws ReadFailureException, ReadTimeoutException, DigestMismatchException
    {
        return handler.get();
    }

    /**
     * @return an executor appropriate for the configured speculative read policy
     */
    public static AbstractReadMultiExecutor getReadExecutor(ReadMultiCommand command, ConsistencyLevel consistencyLevel) throws UnavailableException
    {
        Keyspace keyspace = Keyspace.open(command.ksName);
        List<InetAddress> allReplicas = StorageProxy.getLiveSortedEndpoints(keyspace, command.keys.get(0));
        ReadRepairDecision repairDecision = Schema.instance.getCFMetaData(command.ksName, command.cfName).newReadRepairDecision();
        List<InetAddress> targetReplicas = consistencyLevel.filterForQuery(keyspace, allReplicas, repairDecision);

        // Throw UAE early if we don't have enough replicas.
        consistencyLevel.assureSufficientLiveNodes(keyspace, targetReplicas);

        if (repairDecision != ReadRepairDecision.NONE)
        {
            Tracing.trace("Read-repair {}", repairDecision);
            ReadRepairMetrics.attempted.mark();
        }

        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.cfName);
        RetryType retryType = cfs.metadata.getSpeculativeRetry().type;

        // Speculative retry is disabled *OR* there are simply no extra replicas to speculate.
        if (retryType == RetryType.NONE || consistencyLevel.blockFor(keyspace) == allReplicas.size())
            return new NeverSpeculatingReadExecutor(command, consistencyLevel, targetReplicas);

        if (targetReplicas.size() == allReplicas.size())
        {
            // CL.ALL, RRD.GLOBAL or RRD.DC_LOCAL and a single-DC.
            // We are going to contact every node anyway, so ask for 2 full data requests instead of 1, for redundancy
            // (same amount of requests in total, but we turn 1 digest request into a full blown data request).
            return new AlwaysSpeculatingReadExecutor(cfs, command, consistencyLevel, targetReplicas);
        }

        // RRD.NONE or RRD.DC_LOCAL w/ multiple DCs.
        InetAddress extraReplica = allReplicas.get(targetReplicas.size());
        // With repair decision DC_LOCAL all replicas/target replicas may be in different order, so
        // we might have to find a replacement that's not already in targetReplicas.
        if (repairDecision == ReadRepairDecision.DC_LOCAL && targetReplicas.contains(extraReplica))
        {
            for (InetAddress address : allReplicas)
            {
                if (!targetReplicas.contains(address))
                {
                    extraReplica = address;
                    break;
                }
            }
        }
        targetReplicas.add(extraReplica);

        if (retryType == RetryType.ALWAYS)
            return new AlwaysSpeculatingReadExecutor(cfs, command, consistencyLevel, targetReplicas);
        else // PERCENTILE or CUSTOM.
            return new SpeculatingReadExecutor(cfs, command, consistencyLevel, targetReplicas);
    }

    private static class NeverSpeculatingReadExecutor extends AbstractReadMultiExecutor
    {
        public NeverSpeculatingReadExecutor(ReadMultiCommand command, ConsistencyLevel consistencyLevel, List<InetAddress> targetReplicas)
        {
            super(command, consistencyLevel, targetReplicas);
        }

        public void executeAsync()
        {
            makeDataRequests(targetReplicas.subList(0, 1));
            if (targetReplicas.size() > 1)
                makeDigestRequests(targetReplicas.subList(1, targetReplicas.size()));
        }

        public void maybeTryAdditionalReplicas()
        {
            // no-op
        }

        public Collection<InetAddress> getContactedReplicas()
        {
            return targetReplicas;
        }
    }

    private static class SpeculatingReadExecutor extends AbstractReadMultiExecutor
    {
        private final ColumnFamilyStore cfs;
        private volatile boolean speculated = false;

        public SpeculatingReadExecutor(ColumnFamilyStore cfs,
                                       ReadMultiCommand command,
                                       ConsistencyLevel consistencyLevel,
                                       List<InetAddress> targetReplicas)
        {
            super(command, consistencyLevel, targetReplicas);
            this.cfs = cfs;
        }

        public void executeAsync()
        {
            // if CL + RR result in covering all replicas, getReadExecutor forces AlwaysSpeculating.  So we know
            // that the last replica in our list is "extra."
            List<InetAddress> initialReplicas = targetReplicas.subList(0, targetReplicas.size() - 1);

            if (handler.blockfor < initialReplicas.size())
            {
                // We're hitting additional targets for read repair.  Since our "extra" replica is the least-
                // preferred by the snitch, we do an extra data read to start with against a replica more
                // likely to reply; better to let RR fail than the entire query.
                makeDataRequests(initialReplicas.subList(0, 2));
                if (initialReplicas.size() > 2)
                    makeDigestRequests(initialReplicas.subList(2, initialReplicas.size()));
            }
            else
            {
                // not doing read repair; all replies are important, so it doesn't matter which nodes we
                // perform data reads against vs digest.
                makeDataRequests(initialReplicas.subList(0, 1));
                if (initialReplicas.size() > 1)
                    makeDigestRequests(initialReplicas.subList(1, initialReplicas.size()));
            }
        }

        public void maybeTryAdditionalReplicas()
        {
            // no latency information, or we're overloaded
            if (cfs.sampleLatencyNanos > TimeUnit.MILLISECONDS.toNanos(command.getTimeout()))
                return;

            if (!handler.await(cfs.sampleLatencyNanos, TimeUnit.NANOSECONDS))
            {
                // Could be waiting on the data, or on enough digests.
                ReadMultiCommand retryCommand = command;
                if (resolver.getData() != null)
                    retryCommand = command.copy().setIsDigestQuery(true);

                InetAddress extraReplica = Iterables.getLast(targetReplicas);
                if (traceState != null)
                    traceState.trace("speculating read retry on {}", extraReplica);
                logger.trace("speculating read retry on {}", extraReplica);
                MessagingService.instance().sendRRWithFailure(retryCommand.createMessage(), extraReplica, handler);
                speculated = true;

                cfs.metric.speculativeRetries.inc();
            }
        }

        public Collection<InetAddress> getContactedReplicas()
        {
            return speculated
                   ? targetReplicas
                   : targetReplicas.subList(0, targetReplicas.size() - 1);
        }
    }

    private static class AlwaysSpeculatingReadExecutor extends AbstractReadMultiExecutor
    {
        private final ColumnFamilyStore cfs;

        public AlwaysSpeculatingReadExecutor(ColumnFamilyStore cfs,
                                             ReadMultiCommand command,
                                             ConsistencyLevel consistencyLevel,
                                             List<InetAddress> targetReplicas)
        {
            super(command, consistencyLevel, targetReplicas);
            this.cfs = cfs;
        }

        public void maybeTryAdditionalReplicas()
        {
            // no-op
        }

        public Collection<InetAddress> getContactedReplicas()
        {
            return targetReplicas;
        }

        @Override
        public void executeAsync()
        {
            makeDataRequests(targetReplicas.subList(0, targetReplicas.size() > 1 ? 2 : 1));
            if (targetReplicas.size() > 2)
                makeDigestRequests(targetReplicas.subList(2, targetReplicas.size()));
            cfs.metric.speculativeRetries.inc();
        }
    }

    public double pushRead()
    {
        // Block until the upstream queues look all right if we're using the c3 strategy
        if (DatabaseDescriptor.getScoreStrategy().equals(Config.SelectionStrategy.c3_strategy))
        {
            List<InetAddress> newEndpointList = StorageProxy.getLiveSortedEndpoints(Keyspace.open(command.ksName), command.keys.get(0));
            int originalSize = handler.endpoints.size();
            boolean shouldWait = false;
            int dataEndpointIndex;

            // If c3's rate-limiter is enabled, we allow requests to be blocked if we hit backpressure
            if(DatabaseDescriptor.getC3RateLimiterEnabled())
                shouldWait = true;

            // This is our backpressure knob. If we exceed the rate, bail.
            // Every token bucket's tryAcquire() gives us the duration we need
            // to wait until the rate is available again. If all token buckets
            // are empty, then we tell the corresponding actor to wait for the
            // minimum duration required until one of the rate limiters is available.
            double minimumDurationToWait = Double.MAX_VALUE;
            for (int i = 0; i < newEndpointList.size(); i++)
            {
                final InetAddress endpoint = newEndpointList.get(i);
                double timeToNextRefill = 0L;
                if (!endpoint.equals(FBUtilities.getBroadcastAddress()))
                {
                    timeToNextRefill = MessagingService.instance().sendingRateTryAcquire(endpoint);
                }

                if (timeToNextRefill == 0L)
                {
                    dataEndpointIndex = i;
                    shouldWait = false;

                    // We found the best endpoint that is within rate, put it first in the list
                    Collections.<InetAddress>swap(newEndpointList, dataEndpointIndex, 0);
                    break;
                }

                minimumDurationToWait = Math.min(minimumDurationToWait, timeToNextRefill);
            }

            if (shouldWait)
            {
                return minimumDurationToWait;
            }

            // We're within our expected rate. Update endpoints.
            this.targetReplicas = newEndpointList.subList(0, originalSize);
            handler.endpoints = this.targetReplicas;
        }

        executeAsync();
        return 0L;
    }
}
