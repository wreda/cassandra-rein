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
 import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.RowDataResolver;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

public class SliceFromReadMultiCommand extends ReadMultiCommand
{
    private static final Logger logger = LoggerFactory.getLogger(SliceFromReadMultiCommand.class);

    static final SliceFromReadMultiCommandSerializer serializer = new SliceFromReadMultiCommandSerializer();

    public final SliceQueryFilter filter;

    public SliceFromReadMultiCommand(String keyspaceName, ByteBuffer key, String cfName, long timestamp, SliceQueryFilter filter)
    {
        super(keyspaceName, key, cfName, timestamp, Type.GET_SLICES);
        this.filter = filter;
    }

    public ReadMultiCommand copy()
    {
        ReadMultiCommand newCommand = new SliceFromReadMultiCommand(ksName, key, cfName, timestamp, filter).setIsDigestQuery(isDigestQuery());
        newCommand.setPriority(this.getPriority());
        return newCommand;
    }

    public Row getRow(Keyspace keyspace)
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(ksName, cfName);
        DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);

        // If we're doing a reversed query and the filter includes static columns, we need to issue two separate
        // reads in order to guarantee that the static columns are fetched.  See CASSANDRA-8502 for more details.
        if (filter.reversed && filter.hasStaticSlice(cfm))
        {
            logger.trace("Splitting reversed slice with static columns into two reads");
            Pair<SliceQueryFilter, SliceQueryFilter> newFilters = filter.splitOutStaticSlice(cfm);

            Row normalResults =  keyspace.getRow(new QueryFilter(dk, cfName, newFilters.right, timestamp));
            Row staticResults =  keyspace.getRow(new QueryFilter(dk, cfName, newFilters.left, timestamp));

            // add the static results to the start of the normal results
            if (normalResults.cf == null)
                return staticResults;

            if (staticResults.cf != null)
                for (Cell cell : staticResults.cf.getReverseSortedColumns())
                    normalResults.cf.addColumn(cell);

            return normalResults;
        }

        return keyspace.getRow(new QueryFilter(dk, cfName, filter, timestamp));
    }

    @Override
    public ReadMultiCommand maybeGenerateRetryCommand(RowDataResolver resolver, Row row)
    {
        int maxLiveColumns = resolver.getMaxLiveCount();

        int count = filter.count;
        // We generate a retry if at least one node reply with count live columns but after merge we have less
        // than the total number of column we are interested in (which may be < count on a retry).
        // So in particular, if no host returned count live columns, we know it's not a short read.
        if (maxLiveColumns < count)
            return null;

        int liveCountInRow = row == null || row.cf == null ? 0 : filter.getLiveCount(row.cf, timestamp);
        if (liveCountInRow < getOriginalRequestedCount())
        {
            // We asked t (= count) live columns and got l (=liveCountInRow) ones.
            // From that, we can estimate that on this row, for x requested
            // columns, only l/t end up live after reconciliation. So for next
            // round we want to ask x column so that x * (l/t) == t, i.e. x = t^2/l.
            int retryCount = liveCountInRow == 0 ? count + 1 : ((count * count) / liveCountInRow) + 1;
            SliceQueryFilter newFilter = filter.withUpdatedCount(retryCount);
            return new RetriedSliceFromReadMultiCommand(ksName, key, cfName, timestamp, newFilter, getOriginalRequestedCount());
        }

        return null;
    }

    @Override
    public Row maybeTrim(Row row)
    {
        if ((row == null) || (row.cf == null))
            return row;

        return new Row(row.key, filter.trim(row.cf, getOriginalRequestedCount(), timestamp));
    }

    public IDiskAtomFilter filter()
    {
        return filter;
    }

    public SliceFromReadMultiCommand withUpdatedFilter(SliceQueryFilter newFilter)
    {
        return new SliceFromReadMultiCommand(ksName, key, cfName, timestamp, newFilter);
    }

    /**
     * The original number of columns requested by the user.
     * This can be different from count when the slice command is a retry (see
     * RetriedSliceFromReadMultiCommand)
     */
package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ReadMultiCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.UnknownColumnFamilyException;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.RowDataResolver;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

public class SliceFromReadMultiCommand extends ReadMultiCommand
{
    private static final Logger logger = LoggerFactory.getLogger(SliceFromReadMultiCommand.class);

    static final SliceFromReadMultiCommandSerializer serializer = new SliceFromReadMultiCommandSerializer();

    public final SliceQueryFilter filter;

    public SliceFromReadMultiCommand(String keyspaceName, List<ByteBuffer> keys, String cfName, long timestamp, SliceQueryFilter filter)
    {
        super(keyspaceName, keys, cfName, timestamp, Type.GET_SLICES);
        this.filter = filter;
    }

    public ReadMultiCommand copy()
    {
        ReadMultiCommand newCommand = new SliceFromReadMultiCommand(ksName, keys, cfName, timestamp, filter).setIsDigestQuery(isDigestQuery());
        newCommand.setPriority(this.getPriority());
        return newCommand;
    }

    public Row getRow(Keyspace keyspace, ByteBuffer key)
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(ksName, cfName);
        DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);

        // If we're doing a reversed query and the filter includes static columns, we need to issue two separate
        // reads in order to guarantee that the static columns are fetched.  See CASSANDRA-8502 for more details.
        if (filter.reversed && filter.hasStaticSlice(cfm))
        {
            logger.trace("Splitting reversed slice with static columns into two reads");
            Pair<SliceQueryFilter, SliceQueryFilter> newFilters = filter.splitOutStaticSlice(cfm);

            Row normalResults =  keyspace.getRow(new QueryFilter(dk, cfName, newFilters.right, timestamp));
            Row staticResults =  keyspace.getRow(new QueryFilter(dk, cfName, newFilters.left, timestamp));

            // add the static results to the start of the normal results
            if (normalResults.cf == null)
                return staticResults;

            if (staticResults.cf != null)
                for (Cell cell : staticResults.cf.getReverseSortedColumns())
                    normalResults.cf.addColumn(cell);

            return normalResults;
        }

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
    public ReadMultiCommand maybeGenerateRetryCommand(RowDataResolver resolver, Row row)
    {
        //FIXME fow now we don't retry read multi commands
        return null;
    }

    @Override
    public Row maybeTrim(Row row)
    {
        if ((row == null) || (row.cf == null))
            return row;

        return new Row(row.key, filter.trim(row.cf, getOriginalRequestedCount(), timestamp));
    }

    public IDiskAtomFilter filter()
    {
        return filter;
    }

    public SliceFromReadMultiCommand withUpdatedFilter(SliceQueryFilter newFilter)
    {
        return new SliceFromReadMultiCommand(ksName, keys, cfName, timestamp, newFilter);
    }

    /**
     * The original number of columns requested by the user.
     * This can be different from count when the slice command is a retry (see
     * RetriedSliceFromReadMultiCommand)
     */
    protected int getOriginalRequestedCount()
    {
        return filter.count;
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
}

class SliceFromReadMultiCommandSerializer implements IVersionedSerializer<ReadMultiCommand>
{
    public void serialize(ReadMultiCommand rm, DataOutputPlus out, int version) throws IOException
    {
        SliceFromReadMultiCommand realRM = (SliceFromReadMultiCommand)rm;
        out.writeBoolean(realRM.isDigestQuery());
        out.writeUTF(realRM.ksName);
        out.writeDouble(rm.getPriority());
        out.writeInt(rm.getKeycount());
        for(ByteBuffer k : realRM.keys)
        {
            ByteBufferUtil.writeWithShortLength(k, out);
        }
        out.writeUTF(realRM.cfName);
        out.writeLong(realRM.timestamp);
        CFMetaData metadata = Schema.instance.getCFMetaData(realRM.ksName, realRM.cfName);
        metadata.comparator.sliceQueryFilterSerializer().serialize(realRM.filter, out, version);
    }

    public ReadMultiCommand deserialize(DataInput in, int version) throws IOException
    {
        boolean isDigest = in.readBoolean();
        String keyspaceName = in.readUTF();
        double  priority = in.readDouble();
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
        SliceQueryFilter filter = metadata.comparator.sliceQueryFilterSerializer().deserialize(in, version);
        ReadMultiCommand newCommand = new SliceFromReadMultiCommand(keyspaceName, keys, cfName, timestamp, filter).setIsDigestQuery(isDigest);
        newCommand.setPriority(priority);
        return newCommand;
    }

    public long serializedSize(ReadMultiCommand cmd, int version)
    {
        TypeSizes sizes = TypeSizes.NATIVE;
        SliceFromReadMultiCommand command = (SliceFromReadMultiCommand) cmd;
        int keySize = 0;

        for(ByteBuffer k: command.keys)
        {
            keySize += k.remaining();
        }

        CFMetaData metadata = Schema.instance.getCFMetaData(cmd.ksName, cmd.cfName);

        int size = sizes.sizeof(cmd.isDigestQuery()); // boolean
        size += sizes.sizeof(command.ksName);
        size += sizes.sizeof((long)command.getPriority());
        size += sizes.sizeof((short) keySize) + keySize;
        size += sizes.sizeof(command.cfName);
        size += sizes.sizeof(cmd.timestamp);
        size += metadata.comparator.sliceQueryFilterSerializer().serializedSize(command.filter, version);

        return size;
    }
}
