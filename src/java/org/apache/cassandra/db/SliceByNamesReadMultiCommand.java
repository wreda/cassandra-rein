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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SliceByNamesReadMultiCommand extends ReadMultiCommand
{
    static final SliceByNamesReadMultiCommandSerializer serializer = new SliceByNamesReadMultiCommandSerializer();

    public final NamesQueryFilter filter;

    public SliceByNamesReadMultiCommand(String keyspaceName, List<ByteBuffer> keys, String cfName, long timestamp, NamesQueryFilter filter)
    {
        super(keyspaceName, keys, cfName, timestamp, Type.GET_BY_NAMES);
        this.filter = filter;
    }

    public ReadMultiCommand copy()
    {
        ReadMultiCommand newCommand = new SliceByNamesReadMultiCommand(ksName, keys, cfName, timestamp, filter).setIsDigestQuery(isDigestQuery());
        newCommand.setPriority(this.getPriority());
        return newCommand;
    }

    public Row getRow(Keyspace keyspace, ByteBuffer key)
    {
        DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);
        return keyspace.getRow(new QueryFilter(dk, cfName, filter.cloneShallow(), timestamp));
    }

    public List<Row> getRows(Keyspace keyspace)
    {
        List<Row> rows = new ArrayList<Row>();
        for(ByteBuffer k : keys)
        {
            rows.add(getRow(keyspace, k));
        }
        return rows;
    }

    @Override
    public String toString()
    {
        List<String> keyNames = new ArrayList<String>();

        for(ByteBuffer k : keys)
        {
            keyNames.add(ByteBufferUtil.bytesToHex(k));
        }

        return Objects.toStringHelper(this)
                      .add("ksName", ksName)
                      .add("cfName", cfName)
                      .add("keys", keyNames)
                      .add("filter", filter)
                      .add("timestamp", timestamp)
                      .toString();
    }

    public IDiskAtomFilter filter()
    {
        return filter;
    }
}

class SliceByNamesReadMultiCommandSerializer implements IVersionedSerializer<ReadMultiCommand>
{
    public void serialize(ReadMultiCommand cmd, DataOutputPlus out, int version) throws IOException
    {
        SliceByNamesReadMultiCommand command = (SliceByNamesReadMultiCommand) cmd;
        out.writeBoolean(command.isDigestQuery());
        out.writeUTF(command.ksName);
        out.writeDouble(command.getPriority());
        out.writeInt(command.getKeycount());
        for(ByteBuffer k : command.keys)
        {
            ByteBufferUtil.writeWithShortLength(k, out);
        }
        out.writeUTF(command.cfName);
        out.writeLong(cmd.timestamp);

        CFMetaData metadata = Schema.instance.getCFMetaData(cmd.ksName, cmd.cfName);
        metadata.comparator.namesQueryFilterSerializer().serialize(command.filter, out, version);
    }

    public ReadMultiCommand deserialize(DataInput in, int version) throws IOException
    {
        boolean isDigest = in.readBoolean();
        String keyspaceName = in.readUTF();
        double priority = in.readDouble();
        int keycount = in.readInt();
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        for(int i=0; i<keycount; i++)
        {
            keys.add(ByteBufferUtil.readWithShortLength(in));
        }
        String cfName = in.readUTF();
        long timestamp = in.readLong();
        CFMetaData metadata = Schema.instance.getCFMetaData(keyspaceName, cfName);
        if (metadata == null)
        {
            String message = String.format("Got slice command for nonexistent table %s.%s.  If the table was just " +
                                           "created, this is likely due to the schema not being fully propagated.  Please wait for schema " +
                                           "agreement on table creation.", keyspaceName, cfName);
            throw new UnknownColumnFamilyException(message, null);
        }
        NamesQueryFilter filter = metadata.comparator.namesQueryFilterSerializer().deserialize(in, version);
        ReadMultiCommand newCommand = new SliceByNamesReadMultiCommand(keyspaceName, keys, cfName, timestamp, filter).setIsDigestQuery(isDigest);
        newCommand.setPriority(priority);
        return newCommand;
    }

    public long serializedSize(ReadMultiCommand cmd, int version)
    {
        TypeSizes sizes = TypeSizes.NATIVE;
        SliceByNamesReadMultiCommand command = (SliceByNamesReadMultiCommand) cmd;
        int size = sizes.sizeof(command.isDigestQuery());
        int keySize = 0;

        for(ByteBuffer k: command.keys)
        {
            keySize += k.remaining();
        }

        CFMetaData metadata = Schema.instance.getCFMetaData(cmd.ksName, cmd.cfName);

        size += sizes.sizeof(command.ksName);
        size += sizes.sizeof((long)command.getPriority());
        size += sizes.sizeof((short)keySize) + keySize;
        size += sizes.sizeof(command.cfName);
        size += sizes.sizeof(cmd.timestamp);
        size += metadata.comparator.namesQueryFilterSerializer().serializedSize(command.filter, version);

        return size;
    }
}
