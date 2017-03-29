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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ReadMultiResponse;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.net.MessageIn;

public class RowDigestMultiResolver extends AbstractRowMultiResolver
{
    public RowDigestMultiResolver(String keyspaceName, ByteBuffer key, int maxResponseCount)
    {
        super(key, keyspaceName, maxResponseCount);
    }

    /**
     * Special case of resolve() so that CL.ONE reads never throw DigestMismatchException in the foreground
     */
    public List<Row> getData()
    {
        for (MessageIn<ReadMultiResponse> message : replies)
        {
            ReadMultiResponse result = message.payload;
            if (!result.isDigestQuery())
            {
                if (result.digest() == null)
                    result.setDigest(ColumnFamily.digest(result.rows().get(0).cf));

                return result.rows();
            }
        }
        return null;
    }

    /*
     * This method handles two different scenarios:
     *
     * a) we're handling the initial read, of data from the closest replica + digests
     *    from the rest.  In this case we check the digests against each other,
     *    throw an exception if there is a mismatch, otherwise return the data row.
     *
     * b) we're checking additional digests that arrived after the minimum to handle
     *    the requested ConsistencyLevel, i.e. asynchronous read repair check
     */
    public List<Row> resolve() throws DigestMismatchException
    {
        assert false;
        return null;
    }

    public boolean isDataPresent()
    {
        return true;
    }
}
