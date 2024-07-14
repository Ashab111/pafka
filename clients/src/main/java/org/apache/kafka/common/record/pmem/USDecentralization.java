/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.record.pmem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class USDecentralization {
    public UnitedStorage.SelectMode mode;
    private final Logger log;
    private final Object lock;
    private final String[] dirs;
    private final long[] frees;
    private long free = 0;
    protected volatile int maxDir = 0;
    public void updateStat() {
        long[] tmpFrees = null;
        if (mode == UnitedStorage.SelectMode.SYS_FREE) {
            tmpFrees = new long[frees.length];
            for (int i = 0; i < this.dirs.length; i++) {
                File file = new File(this.dirs[i]);
                tmpFrees[i] = file.getFreeSpace();
            }

            synchronized (lock) {
                free = 0;
                for (int i = 0; i < this.dirs.length; i++) {
                    frees[i] = tmpFrees[i];
                    free += frees[i];
                }
            }
        } else {
            synchronized (lock) {
                tmpFrees = frees.clone();
            }
        }

        long max = 0;
        int tmpMaxDir = 0;
        for (int i = 0; i < this.dirs.length; i++) {
            if (tmpFrees[i] > max) {
                max = tmpFrees[i];
                tmpMaxDir = i;
            }
        }
        maxDir = tmpMaxDir;
    }
    USDecentralization(String[] dirs, long[] frees) {
        this.log = LoggerFactory.getLogger(UnitedStorage.class);
        mode = UnitedStorage.SelectMode.CAPACITY;
        lock = new Object();
        this.dirs = dirs;
        this.frees = frees;
    }
    public void take(int idx, long size) {
        if (mode != UnitedStorage.SelectMode.CONFIG_FREE && mode != UnitedStorage.SelectMode.MAX_FREE) {
            log.error("Use take() in mode " + mode);
        }

        log.debug("Before take: " + dirs[idx] + ": " + frees[idx] + "; " + free);
        synchronized (lock) {
            frees[idx] -= size;
            free -= size;
        }
        log.debug("After take: " + dirs[idx] + ": " + frees[idx] + "; " + free);

        if (idx == maxDir) {
            updateStat();
        }
    }
    public void createIfNotExists(String[] paths) {
        for (String path : paths) {
            File file = new File(path);
            if (!file.exists()) {
                if (!file.mkdirs()) {
                    log.error("Create directory " + path + " failed");
                }
            }
        }
    }
    void setMode(UnitedStorage.SelectMode mode) {
        this.mode = mode;
        updateStat();
    }
    public void release(int idx, long size) {
        if (mode != UnitedStorage.SelectMode.CONFIG_FREE && mode != UnitedStorage.SelectMode.MAX_FREE) {
            log.error("Use release() in mode " + mode);
        }

        synchronized (lock) {
            frees[idx] += size;
            free += size;
        }

        if (idx != maxDir) {
            updateStat();
        }
    }

}
