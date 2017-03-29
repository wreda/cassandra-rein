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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/*
 * The read response message is sent by the server when reading data
 * this encapsulates the keyspacename and the row that has been read.
 * The keyspace name is needed so that we can use it to create repairs.
 */
public class ReadMultiResponse
{
    public static final IVersionedSerializer<ReadMultiResponse> serializer = new ReadMultiResponseSerializer();
    //private static final AtomicReferenceFieldUpdater<ReadMultiResponse, ByteBuffer> digestUpdater = AtomicReferenceFieldUpdater.newUpdater(ReadMultiResponse.class, ByteBuffer.class, "digest");

    private final List<Row> rows;
    private volatile ByteBuffer digests;

    public ReadMultiResponse(List<Row> rows)
    {
        this(rows, null);
    }

    public ReadMultiResponse(List<Row> rows, List<ByteBuffer> digests)
    {
        this.rows = rows;
        this.digests = null;
    }

    public List<Row> rows()
    {
        return rows;
    }

    public ByteBuffer digest()
    {
        //FIXME for now disable readmulti for digests
        //assert false;
        return null;
    }

    public void setDigest(ByteBuffer digest)
    {
        //FIXME for now disable readmulti for digests
        //assert false;
    }

    public boolean isDigestQuery()
    {
        return false;
    }

    public int digestSize()
    {
        return 0;
    }

    public int rowSize()
    {
        return rows().size();
    }
}

class ReadMultiResponseSerializer implements IVersionedSerializer<ReadMultiResponse>
{
    public void serialize(ReadMultiResponse response, DataOutputPlus out, int version) throws IOException
    {
        out.writeInt(response.isDigestQuery() ? response.digestSize() : 0);
        ByteBuffer buffer = response.isDigestQuery() ? response.digest() : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        out.write(buffer);
        out.writeBoolean(response.isDigestQuery());
        out.writeInt(response.isDigestQuery() ? 0 : response.rowSize());
        if (!response.isDigestQuery())
        {
            for(Row r: response.rows())
            {
                Row.serializer.serialize(r, out, version);
            }
        }
    }

    public ReadMultiResponse deserialize(DataInput in, int version) throws IOException
    {
        byte[] digest = null;
        int digestSize = in.readInt();
        if (digestSize > 0)
        {
            digest = new byte[digestSize];
            in.readFully(digest, 0, digestSize);
        }
        boolean isDigest = in.readBoolean();
        assert isDigest == digestSize > 0;
        int rowSize = in.readInt();
        assert !isDigest == rowSize > 0;
        List<Row> rows = new ArrayList<Row>();
        if (!isDigest)
        {
            for(int i=0; i<rowSize; i++)
            {
                // This is coming from a remote host
                rows.add(Row.serializer.deserialize(in, version, ColumnSerializer.Flag.FROM_REMOTE));
            }
        }

        return isDigest ? new ReadMultiResponse(null) : new ReadMultiResponse(rows);
    }

    public long serializedSize(ReadMultiResponse response, int version)
    {
        TypeSizes typeSizes = TypeSizes.NATIVE;
        ByteBuffer buffer = response.isDigestQuery() ? response.digest() : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        int size = typeSizes.sizeof(buffer.remaining());
        size += buffer.remaining();
        size += typeSizes.sizeof(response.isDigestQuery());
        size += typeSizes.sizeof(response.rowSize());
        if (!response.isDigestQuery())
        {
            for(int i=0; i<response.rowSize(); i++)
            {
                size += Row.serializer.serializedSize(response.rows().get(i), version);
            }
        }
        return size;
    }
}
