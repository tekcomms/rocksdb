// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * A subclass of RocksDB which supports TTL operations.
 *
 * @see TtlDB
 */
public class TtlDB extends RocksDB {
  /**
   * Open a TtlDB under the specified path.
   *
   * @param opt options for db.
   * @param Time to live in seconds
   * @param the db path for storing data.
   * @return reference to the opened TtlDB.
   */
  public static TtlDB open(
      Options opt, int ttl, String db_path )
      throws RocksDBException {

      TtlDB ttlDB = new TtlDB();
          
      ttlDB.open(opt.nativeHandle_, db_path, ttl);
      ttlDB.transferCppRawPointersOwnershipFrom(opt);

      return ttlDB;
  }


  /**
   * Close the TtlDB instance and release resource.
   *
   * Internally, TtlDB owns the rocksdb::DB pointer to its
   * associated RocksDB.  The release of that RocksDB pointer is
   * handled in the destructor of the c++ rocksdb::TtlDB and
   * should be transparent to Java developers.
   */
  @Override public synchronized void close() {
    if (isInitialized()) {
        super.close();
    }
  }

  /**
   * A protected construction that will be used in the static factory
   * method TtlDB.open().
   */
  protected TtlDB() {
    super();
  }

  @Override protected void finalize() {
    close();
  }

  protected native void open( long option, String dbName, int ttl );

}
