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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class UnitedStorageExtension {
    public boolean containsRelative(String file, String[] dirs) {
        return containsRelativeInternal(file, dirs) >= 0;
    }

    public String maxDir(String[] dirs, int maxDir) {
        return dirs[maxDir];
    }

    public static void createIfNotExists(String[] paths, Logger log) {
        for (String path : paths) {
            File file = new File(path);
            if (!file.exists()) {
                if (!file.mkdirs()) {
                    log.error("Create directory " + path + " failed");
                }
            }
        }
    }
    public static int containsAbsoluteInternal(String file, String[] dirs) {
        for (int i = 0; i < dirs.length; i++) {
            if (file.startsWith(dirs[i])) {
                return i;
            }
        }

        return -1;
    }
    public static int containsRelativeInternal(String file, String[] dirs) {
        for (int i = 0; i < dirs.length; i++) {
            Path absPath = Paths.get(dirs[i], file);
            if (absPath.toFile().exists()) {
                return i;
            }
        }

        return -1;
    }
    public static long free(long free) {
        return free;
    }
    public static String at(int i, String[] dirs) {
        return dirs[i];
    }
    public static long[] updateStat(UnitedStorage.SelectMode mode, DecentralizationObj deo, long free, Object lock) {
        long[] tmpFrees = null;

        if (mode == UnitedStorage.SelectMode.SYS_FREE) {
            tmpFrees = new long[deo.frees.length];
            for (int i = 0; i < deo.dirs.length; i++) {
                File file = new File(deo.dirs[i]);
                tmpFrees[i] = file.getFreeSpace();
            }

            synchronized (lock) {
                free = 0;
                for (int i = 0; i < deo.dirs.length; i++) {
                    deo.frees[i] = tmpFrees[i];
                    free += deo.frees[i];
                }
            }
        } else {
            synchronized (lock) {
                tmpFrees = deo.frees.clone();
            }
        }

        long max = 0;
        int tmpMaxDir = 0;
        for (int i = 0; i < deo.dirs.length; i++) {
            if (tmpFrees[i] > max) {
                max = tmpFrees[i];
                tmpMaxDir = i;
            }
        }
        deo.maxDir = tmpMaxDir;
        return new long[]{deo.maxDir, free};
    }

}
