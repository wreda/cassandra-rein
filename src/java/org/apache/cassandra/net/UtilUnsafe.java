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

package org.apache.cassandra.net;

/*
 * Copyright (c) 2015, Parallel Universe Software Co. All rights reserved.
 *
 * This program and the accompanying materials are license under the terms of the
 * MIT license.
 */

import java.lang.reflect.Field;
import sun.misc.Unsafe;

/**
 * Simple class to obtain access to the {@link Unsafe} object. {@link Unsafe}
 * is required to allow efficient CAS operations on arrays. Note that the
 * versions in {@code java.util.concurrent.atomic}, such as {@link
 * java.util.concurrent.atomic.AtomicLongArray}, require extra memory ordering
 * guarantees which are generally not needed in these algorithms and are also
 * expensive on most processors.
 */
class UtilUnsafe {
    private UtilUnsafe() {
    }

    public static Unsafe getUnsafe1() {
        // Not on bootclasspath
        if (UtilUnsafe.class.getClassLoader() == null)
            return Unsafe.getUnsafe();
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(UtilUnsafe.class);
        } catch (Exception e) {
            throw new RuntimeException("Could not obtain access to sun.misc.Unsafe", e);
        }
    }

    public static Unsafe getUnsafe() {
        try {
            return Unsafe.getUnsafe();
        } catch (SecurityException se) {
            try {
                return java.security.AccessController.doPrivileged(new java.security.PrivilegedExceptionAction<Unsafe>() {
                    @Override
                    public Unsafe run() throws Exception {
                        final Class<sun.misc.Unsafe> k = sun.misc.Unsafe.class;
                        if (true) {
                            final Field f = k.getDeclaredField("theUnsafe");
                            f.setAccessible(true);
                            final Object x = f.get(null);
                            return k.cast(x);
                        } else {
                            for (Field f : k.getDeclaredFields()) {
                                f.setAccessible(true);
                                final Object x = f.get(null);
                                if (k.isInstance(x))
                                    return k.cast(x);
                            }
                            throw new NoSuchFieldError("the Unsafe");
                        }
                    }
                });
            } catch (java.security.PrivilegedActionException e) {
                throw new RuntimeException("Could not initialize intrinsics", e.getCause());
            }
        }
    }
}