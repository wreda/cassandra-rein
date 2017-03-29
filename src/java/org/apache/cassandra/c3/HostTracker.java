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

package org.apache.cassandra.c3;

/**
 * Created by reda on 28/07/16.
 */

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.routing.SmallestMailboxPool;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class HostTracker
{

    private static final Logger logger = LoggerFactory.getLogger(HostTracker.class);
    private final Config config = ConfigFactory.parseString("dispatcher {\n" +
                                                            "  type = Dispatcher\n" +
                                                            //"  mailbox-type = \"akka.dispatch.UnboundedDequeBasedMailbox\"\n" +
                                                            "  executor = \"fork-join-executor\"\n" +
                                                            "  fork-join-executor {\n" +
                                                            "    parallelism-min = 2\n" +
                                                            "    parallelism-factor = 2.0\n" +
                                                            "    parallelism-max = 20\n" +
                                                            "  }\n" +
                                                            "  throughput = 10\n" +
                                                            "}\n");

    private final ConcurrentHashMap<InetAddress, ActorRef> actors = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<InetAddress, AtomicInteger> pendingRequests = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<InetAddress, RateController> rateControllers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Pair<InetAddress,Double>, ScoreTracker> scoreTrackers = new ConcurrentHashMap<>();
    private final ActorSystem actorSystem = ActorSystem.create("C3", config);

    public ActorRef getActor(List<InetAddress> endpoints)
    {
        final InetAddress endpoint = endpoints.get(0);
        ActorRef actor = actors.get(endpoint);
        if (actor == null)
        {
            synchronized (this)
            {
                if (!actors.containsKey(endpoint))
                {
                    //actor = actorSystem.actorOf(Props.create(ReplicaGroupActor.class).withDispatcher("dispatcher"), endpoint.getHostName());
                    actor = actorSystem.actorOf(new SmallestMailboxPool(5).props(Props.create(ReplicaGroupActor.class).withDispatcher("dispatcher")), endpoint.getHostName());
                    actors.putIfAbsent(endpoint, actor);
                    logger.info("Creating actor for: " + endpoints);
                }
            }
            return actors.get(endpoint);
        }
        return actor;
    }

    public RateController getRateController(InetAddress endpoint) {
        return rateControllers.get(endpoint);
    }

    public ScoreTracker getScoreTracker(Pair<InetAddress, Double> key) {
        if (!scoreTrackers.containsKey(key)) {
            scoreTrackers.putIfAbsent(key, new ScoreTracker());
        }
        return scoreTrackers.get(key);
    }

    public boolean containsKey(InetAddress key) {
        return pendingRequests.containsKey(key);
    }

    public AtomicInteger put(InetAddress key, AtomicInteger value) {
        return pendingRequests.put(key, value);
    }

    public AtomicInteger get(InetAddress key) {
        return pendingRequests.get(key);
    }

    public double sendingRateTryAcquire(InetAddress endpoint)
    {
        RateController rateController = rateControllers.get(endpoint);

        if (rateController == null)
        {
            rateControllers.putIfAbsent(endpoint, new RateController());
            rateController = rateControllers.get(endpoint);
        }

        assert (rateController != null);
        return rateController.tryAcquire();
    }

    public void receiveRateTick(InetAddress endpoint)
    {
        RateController rateController = rateControllers.get(endpoint);

        if (rateController == null)
        {
            rateControllers.putIfAbsent(endpoint, new RateController());
            rateController = rateControllers.get(endpoint);
        }

        assert (rateController != null);
        rateController.receiveRateTrackerTick();
    }

    public double getExpectedResponseTime(Pair<InetAddress,Double> key, int opSize) {
        //RateController rateController = rateControllers.get(key);
        ScoreTracker scoreTracker = scoreTrackers.get(key);

        //if (rateController == null) {
        //    rateControllers.putIfAbsent(endpoint, new RateController());
        //    rateController = rateControllers.get(endpoint);
        //}

        if (scoreTracker == null) {
            scoreTrackers.putIfAbsent(key, new ScoreTracker());
            scoreTracker = scoreTrackers.get(key);
        }

        //assert(rateController != null);
        assert(scoreTracker != null);

        return scoreTracker.getExpectedResponseTime(opSize);
    }

    public AtomicInteger getPendingRequestsCounter(final InetAddress endpoint)
    {
        AtomicInteger counter = pendingRequests.get(endpoint);
        if (counter == null)
        {
            pendingRequests.put(endpoint, new AtomicInteger(0));
            counter = pendingRequests.get(endpoint);
        }

        return counter;
    }

    public void updateMetrics(MessageIn message, long latency) {
        receiveRateTick(message.from);
        final RateController rateController = getRateController(message.from);
        assert (rateController != null);
        rateController.updateCubicSendingRate();
        int count = pendingRequests.get(message.from).decrementAndGet();
        logger.trace("Decrementing pendingJob count Endpoint: {}, Count: {} ", message.from, count);

        int queueSize = ByteBuffer.wrap((byte[]) message.parameters.get(C3Metrics.QSZ)).getInt();
        double serviceTimeInMillis = ByteBuffer.wrap((byte[]) message.parameters.get(C3Metrics.MU)).getLong() / 1000000.0;
        //divide service time by # of operations in opset
        serviceTimeInMillis = serviceTimeInMillis/ByteBuffer.wrap((byte[]) message.parameters.get(C3Metrics.OPSZ)).getInt();
        double waitingTimeInMillis = ByteBuffer.wrap((byte[]) message.parameters.get(C3Metrics.WT)).getLong() / 1000000.0;
        double latencyInMillis = (latency / 1000000.0) - waitingTimeInMillis - serviceTimeInMillis;
        double priority = ByteBuffer.wrap((byte[]) message.parameters.get(C3Metrics.PRT)).getDouble();

        Pair<InetAddress,Double> key = new Pair(message.from, priority);
        ScoreTracker scoreTracker = getScoreTracker(key);
        scoreTracker.updateNodeScore(queueSize, serviceTimeInMillis, latencyInMillis, waitingTimeInMillis);
    }

    public void updateLocalMetrics(double serviceTime, int opSize, double priority)
    {
        double serviceTimeInMillis = serviceTime/opSize/1000000.0;

        Pair<InetAddress,Double> key = new Pair(FBUtilities.getBroadcastAddress(), priority);
        ScoreTracker scoreTracker = getScoreTracker(key);
        scoreTracker.updateNodeScore(0, serviceTimeInMillis, 0, 0);

    }
}