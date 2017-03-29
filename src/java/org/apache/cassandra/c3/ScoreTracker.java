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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScoreTracker
{
    // Cubic score for replica selection, updated on a per-request level
    private static final double ALPHA = 0.9;
    private double queueSizeEMA = 0;
    private double serviceTimeEMA = 0;
    private double latencyEMA = 0;
    private double waitingTimeEMA = 0;

    protected static final Logger logger = LoggerFactory.getLogger(ScoreTracker.class);

    public synchronized void updateNodeScore(int queueSize, double serviceTime, double latency, double waitingTime)
    {
        final double responseTime = latency - serviceTime;
        //logger.info("Updating node scores -> st:{}, qs:{}, rt:{}",queueSize, serviceTime, responseTime);
        queueSizeEMA = getEMA(queueSize, queueSizeEMA);
        serviceTimeEMA = getEMA(serviceTime, serviceTimeEMA);
        latencyEMA = getEMA(latency, latencyEMA);
        waitingTimeEMA = getEMA(waitingTime, waitingTimeEMA);

        //assert serviceTime < latency;
    }

    private synchronized double getEMA(double value, double previousEMA)
    {
        return ALPHA * value + (1 - ALPHA) * previousEMA;
    }

    public synchronized double getExpectedResponseTime(int opSize)
    {
        return opSize*serviceTimeEMA + latencyEMA + waitingTimeEMA;
    }
}