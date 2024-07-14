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

import java.io.File;

public class UnitedStorageExtension extends UnitedStorage {
    public UnitedStorageExtension(String[] dirs) {
        super(dirs);
    }

    public UnitedStorageExtension(String[] dirs, SelectMode mode) {
        super(dirs, mode);
    }

    public UnitedStorageExtension(String dirs) {
        super(dirs);
    }

    public UnitedStorageExtension(String dirs, SelectMode mode) {
        super(dirs, mode);
    }

    public UnitedStorageExtension(String dirs, String caps) {
        super(dirs, caps);
    }

    public UnitedStorageExtension(String dirs, String caps, SelectMode mode) {
        super(dirs, caps, mode);
    }

    public UnitedStorageExtension(String[] dirs, long[] capacities) {
        super(dirs, capacities);
    }

    public UnitedStorageExtension(String[] dirs, long[] capacities, SelectMode mode) {
        super(dirs, capacities, mode);
    }


    public boolean containsRelative(String file) {
        return containsRelativeInternal(file) >= 0;
    }
    public String randomDir() {
        return randomDir(true, false);
    }
    public String maxDir() {
        return this.dirs[maxDir];
    }
    public String randomDir(boolean balanced, boolean update) {
        return this.dirs[randomDirInternal(balanced, update)];
    }
    public void take(int idx, long size) {
        if (mode != SelectMode.CONFIG_FREE && mode != SelectMode.MAX_FREE) {
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
    void setMode(SelectMode mode) {
        this.mode = mode;
        updateStat();
    }
    public void release(int idx, long size) {
        if (mode != SelectMode.CONFIG_FREE && mode != SelectMode.MAX_FREE) {
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
