// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.rocksdb.*;

import java.lang.System;
import java.lang.Thread;
import java.util.Arrays;

public class TtlDBTest {
    static final String db_path = "/tmp/ttljni_db";
    static {
        RocksDB.loadLibrary();
    }

    public static void main(String[] args) {

        Options opt = new Options();
        opt.setCreateIfMissing(true);


        TtlDB ttldb = null;

        try {
            ttldb = TtlDB.open(opt, 2, db_path);
            ttldb.put("key1".getBytes(), "val1".getBytes());

            byte[] value = ttldb.get("key1".getBytes());
            assert(value != null);

            try {
                Thread.sleep( 3000 );
            } catch(InterruptedException ex) {
                Thread.currentThread().interrupt();
            }

            ttldb.compactRange(null,null);
            //ttldb.compactRange( "key1".getBytes(), "key2".getBytes() );

            value = ttldb.get("key1".getBytes());
            assert(value == null);

            ttldb.close();

            System.out.println("TTL DB test passed");
         } catch (RocksDBException e) {
            System.err.format("[ERROR]: %s%n", e);
            e.printStackTrace();
         } finally {
            opt.dispose();
            if (ttldb != null) {
                ttldb.close();
            }
         }
    
    }
}
