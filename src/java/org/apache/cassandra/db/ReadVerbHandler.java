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
package org.apache.cassandra.db;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.c3.C3Metrics;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class ReadVerbHandler implements IVerbHandler<ReadCommand>
{
    public void doVerb(MessageIn<ReadCommand> message, int id)
    {
        if (StorageService.instance.isBootstrapMode())
        {
            throw new RuntimeException("Cannot service reads while bootstrapping!");
        }

        AtomicInteger counter = MessagingService.instance().getPendingRequestsCounter(FBUtilities.getBroadcastAddress());
        counter.incrementAndGet();
        long startRead = System.nanoTime();
        long waitingTimeNanos = startRead - message.getConstructionTime();

        ReadCommand command = message.payload;
        Keyspace keyspace = Keyspace.open(command.ksName);
        Row row = command.getRow(keyspace);

        long serviceTimeInNanos = System.nanoTime() - startRead;
        int queueSize = counter.decrementAndGet();

        MessageOut<ReadResponse> reply =
        new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE,
                         getResponse(command, row),
                         ReadResponse.serializer)
        .withParameter(C3Metrics.MU, ByteBufferUtil.bytes(serviceTimeInNanos).array())
        .withParameter(C3Metrics.QSZ, ByteBufferUtil.bytes(queueSize).array())
        .withParameter(C3Metrics.PRT, ByteBufferUtil.bytes(command.getPriority()).array())
        .withParameter(C3Metrics.OPSZ, ByteBufferUtil.bytes(command.getBatchSize()).array())
        .withParameter(C3Metrics.WT, ByteBufferUtil.bytes(waitingTimeNanos).array());

        Tracing.trace("Enqueuing response to {}", message.from);
        MessagingService.instance().sendReply(reply, id, message.from);
    }

    public static ReadResponse getResponse(ReadCommand command, Row row)
    {
        if (command.isDigestQuery())
        {
            return new ReadResponse(ColumnFamily.digest(row.cf));
        }
        else
        {
            return new ReadResponse(row);
        }
    }
}
