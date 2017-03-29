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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.IReadCommand;
import org.apache.cassandra.service.RowDataResolver;
import org.apache.cassandra.service.pager.Pageable;

public abstract class ReadMultiCommand implements IReadCommand, Pageable
{
    public enum Type
    {
        GET_BY_NAMES((byte)1),
        GET_SLICES((byte)2);

        public final byte serializedValue;

        private Type(byte b)
        {
            this.serializedValue = b;
        }

        public static Type fromSerializedValue(byte b)
        {
            return b == 1 ? GET_BY_NAMES : GET_SLICES;
        }
    }

    public static final ReadMultiCommandSerializer serializer = new ReadMultiCommandSerializer();

    public MessageOut<ReadMultiCommand> createMessage()
    {
        return new MessageOut<>(MessagingService.Verb.READMULTI, this, serializer);
    }

    public final String ksName;
    public final String cfName;
    public final List<ByteBuffer> keys;
    public final int keycount;
    public double priority=0; //Rein: Used for server-side scheduling
    public int batchSize=-1;
    public final long timestamp;
    private boolean isDigestQuery = false;
    protected final Type commandType;

    protected ReadMultiCommand(String ksName, List<ByteBuffer> keys, String cfName, long timestamp, Type cmdType)
    {
        this.ksName = ksName;
        this.keys = keys;
        this.keycount = keys.size();
        this.cfName = cfName;
        this.timestamp = timestamp;
        this.commandType = cmdType;
    }

    public static ReadMultiCommand create(String ksName, List<ByteBuffer> keys, String cfName, long timestamp, IDiskAtomFilter filter)
    {
        if (filter instanceof SliceQueryFilter)
            return new SliceFromReadMultiCommand(ksName, keys, cfName, timestamp, (SliceQueryFilter)filter);
        else
            return new SliceByNamesReadMultiCommand(ksName, keys, cfName, timestamp, (NamesQueryFilter)filter);
    }

    public boolean isDigestQuery()
    {
        return isDigestQuery;
    }

    public ReadMultiCommand setIsDigestQuery(boolean isDigestQuery)
    {
        this.isDigestQuery = isDigestQuery;
        return this;
    }

    public String getColumnFamilyName()
    {
        return cfName;
    }

    public abstract ReadMultiCommand copy();

    public abstract List<Row> getRows(Keyspace keyspace);

    public abstract IDiskAtomFilter filter();

    public String getKeyspace()
    {
        return ksName;
    }

    // maybeGenerateRetryCommand is used to generate a retry for short reads
    public ReadMultiCommand maybeGenerateRetryCommand(RowDataResolver resolver, Row row)
    {
        return null;
    }

    // maybeTrim removes columns from a response that is too long
    public Row maybeTrim(Row row)
    {
        return row;
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getReadRpcTimeout();
    }

    public int getKeycount()
    {
        return keycount;
    }

    public void setPriority(double priority)
    {
        this.priority = priority;
    }

    public double getPriority()
    {
        return this.priority;
    }

    public void setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
    }

    public int getBatchSize() { return this.batchSize; }

}

class ReadMultiCommandSerializer implements IVersionedSerializer<ReadMultiCommand>
{
    public void serialize(ReadMultiCommand command, DataOutputPlus out, int version) throws IOException
    {
        out.writeByte(command.commandType.serializedValue);
        switch (command.commandType)
        {
            case GET_BY_NAMES:
                SliceByNamesReadMultiCommand.serializer.serialize(command, out, version);
                break;
            case GET_SLICES:
                SliceFromReadMultiCommand.serializer.serialize(command, out, version);
                break;
            default:
                throw new AssertionError();
        }
    }

    public ReadMultiCommand deserialize(DataInput in, int version) throws IOException
    {
        ReadMultiCommand.Type msgType = ReadMultiCommand.Type.fromSerializedValue(in.readByte());
        switch (msgType)
        {
            case GET_BY_NAMES:
                return SliceByNamesReadMultiCommand.serializer.deserialize(in, version);
            case GET_SLICES:
                return SliceFromReadMultiCommand.serializer.deserialize(in, version);
            default:
                throw new AssertionError();
        }
    }

    public long serializedSize(ReadMultiCommand command, int version)
    {
        switch (command.commandType)
        {
            case GET_BY_NAMES:
                return 1 + SliceByNamesReadMultiCommand.serializer.serializedSize(command, version);
            case GET_SLICES:
                return 1 + SliceFromReadMultiCommand.serializer.serializedSize(command, version);
            default:
                throw new AssertionError();
        }
    }
}
