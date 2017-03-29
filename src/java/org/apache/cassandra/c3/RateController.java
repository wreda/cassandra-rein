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

import org.apache.cassandra.config.DatabaseDescriptor;

import java.net.InetAddress;

public class RateController
{
    // Constants for send/receive rate tracking
    private static final long RECEIVE_RATE_INITIAL = 100;
    private static final long RATE_INTERVAL_MS = DatabaseDescriptor.getC3RateIntervalMs();
    private static final double RATE_LIMITER_MAX_TOKENS = DatabaseDescriptor.getC3RateLimiterMaxTokens();

    // Constants for cubic function
    private static final double CUBIC_BETA = 0.2;
    private static final double CUBIC_C = 0.000004;
    private static final double CUBIC_SMAX = 10;
    private static final double CUBIC_HYSTERISIS_FACTOR = 4;
    private static final double CUBIC_BETA_BY_C = CUBIC_BETA / CUBIC_C;
    private static final double CUBIC_HYSTERISIS_DURATION = RATE_INTERVAL_MS * CUBIC_HYSTERISIS_FACTOR;

    // Cubic growth variables
    private long timeOfLastRateDecrease = 0L;
    private long timeOfLastRateIncrease = 0L;
    private double Rmax = 0;

    private final SimpleRateLimiter sendingRateLimiter;
    private final SlottedRateTracker receiveRateTracker;

    public RateController()
    {
        this.sendingRateLimiter = new SimpleRateLimiter(1, RATE_INTERVAL_MS, RATE_LIMITER_MAX_TOKENS);
        this.receiveRateTracker = new SlottedRateTracker(RECEIVE_RATE_INITIAL, RATE_INTERVAL_MS);
    }

    public synchronized void updateCubicSendingRate()
    {
        final double currentReceiveRate = receiveRateTracker.getCurrentRate();
        final double currentSendingRate = sendingRateLimiter.getRate();
        final long now = System.currentTimeMillis();

        if (currentSendingRate > currentReceiveRate
            && (now - timeOfLastRateIncrease > CUBIC_HYSTERISIS_DURATION))
        {
            Rmax = currentSendingRate;
            sendingRateLimiter.setRate(Math.max(currentSendingRate * CUBIC_BETA, 0.1));
            timeOfLastRateDecrease = now;
        }
        else if (currentSendingRate < currentReceiveRate)
        {
            final double T = System.currentTimeMillis() - timeOfLastRateDecrease;
            timeOfLastRateIncrease = now;
            final double scalingFactor = Math.cbrt(Rmax * CUBIC_BETA_BY_C);
            final double newSendingRate = CUBIC_C * Math.pow(T - scalingFactor, 3) + Rmax;

            if (newSendingRate - currentSendingRate > CUBIC_SMAX)
            {
                sendingRateLimiter.setRate(currentSendingRate + CUBIC_SMAX);
            }
            else
            {
                sendingRateLimiter.setRate(newSendingRate);
            }

            assert (newSendingRate > 0);
        }
    }

    public double tryAcquire()
    {
        return sendingRateLimiter.tryAcquire();
    }

    public void receiveRateTrackerTick()
    {
        receiveRateTracker.add(1);
    }

    private class SlottedRateTracker
    {

        private double currentRate;
        private long interval;
        private long lastTick = 0;
        private long eventCount = 0;
        private double lastRate = 0.0;

        /**
         * @param initialRate Initial setting for the rate parameter
         * @param interval    Interval in milliseconds over which rate is calculated
         */
        public SlottedRateTracker(double initialRate, long interval)
        {
            this.currentRate = initialRate;
            this.interval = interval;
        }

        public synchronized double getCurrentRate()
        {
            add(0);
            return this.currentRate;
        }

        public synchronized void setInterval(long interval)
        {
            this.interval = interval;
        }


        /**
         * Add to the rate counter. During an interval, an exponential
         * weighted average of the rate is maintained. Once we're outside
         * the granularity of an interval, this rate is reset. In that sense,
         * we're not really tracking a moving average.
         *
         * @param requests
         */
        public synchronized void add(long requests)
        {
            final long now = System.currentTimeMillis() / interval;
            if (now - lastTick < 2)
            {
                eventCount += requests;
                if (now > lastTick)
                {
                    final double alpha = (now - lastTick) / (float) interval;
                    currentRate = alpha * ((double) eventCount) + (1 - alpha) * currentRate;
                    lastTick = now;
                    eventCount = 0;
                }
            }
            else
            {
                final double alpha = (now - lastTick) / (float) interval;
                currentRate = alpha * ((double) eventCount) + (1 - alpha) * currentRate;
                lastTick = now;
                eventCount = 0;
            }
        }
    }

    private class SimpleRateLimiter
    {
        private long lastSent;
        private double tokens;

        private double rate;
        private double rateIntervalInMillis; // in milliseconds
        private final double maxTokens;


        public SimpleRateLimiter(double initialRate, double rateIntervalInNanos, double maxTokens)
        {
            this.rate = initialRate;
            this.rateIntervalInMillis = rateIntervalInNanos * 1000000;
            this.maxTokens = maxTokens;
            this.tokens = maxTokens;
            this.lastSent = System.nanoTime();
        }

        public synchronized double tryAcquire()
        {
            double currentTokens = Math.min(maxTokens,
                                            tokens + (rate / rateIntervalInMillis * (System.nanoTime() - lastSent)));
            if (currentTokens >= 1)
            {
                tokens = currentTokens - 1;
                lastSent = System.nanoTime();
                return 0;
            }
            else
            {
                return (1 - currentTokens) * rateIntervalInMillis / rate; // Nanoseconds
            }
        }

        public synchronized double getRate()
        {
            return rate;
        }

        public synchronized void setRate(final double rate)
        {
            this.rate = rate;
        }
    }

}