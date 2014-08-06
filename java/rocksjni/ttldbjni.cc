// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::BackupableDB and rocksdb::BackupableDBOptions methods
// from Java side.

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>
#include <string>

#include "include/org_rocksdb_TtlDB.h"
#include "rocksjni/portal.h"
#include "utilities/db_ttl.h"
#include "rocksdb/db.h"

/*
 * Class:     org_rocksdb_TtlDB
 * Method:    open
 * Signature: (LSL)V
 */
void Java_org_rocksdb_TtlDB_open(
    JNIEnv* env, jobject jdb, jlong jopt_handle, jstring jdb_path, jint jopt_ttl) {

  rocksdb::DBWithTTL* ttlDB = nullptr;
      
  auto opt = reinterpret_cast<rocksdb::Options*>(jopt_handle);
  const char* dbpath = env->GetStringUTFChars(jdb_path, 0);
  rocksdb::Status s = rocksdb::DBWithTTL::Open(*opt, dbpath, &ttlDB, static_cast<int32_t>(jopt_ttl), false);
  env->ReleaseStringUTFChars(jdb_path, dbpath);

  if (  s.ok() ) {
       // as TtlDB extends RocksDB on the java side, we can reuse
       // the RocksDB portal here.
       rocksdb::RocksDBJni::setHandle(env, jdb, ttlDB);
       return;
  }

  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);

}
