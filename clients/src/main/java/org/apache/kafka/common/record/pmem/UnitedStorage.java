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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnitedStorage {
    public enum SelectMode {
        CAPACITY, SYS_FREE, CONFIG_FREE, MAX_FREE;
    };

    private static final Logger log = LoggerFactory.getLogger(UnitedStorage.class);
    static final SelectMode DEFAULT_MODE = SelectMode.CAPACITY;

    protected String[] dirs;
    private long[] frees;
    private long[] capacities;
    public long free = 0;
    private long capacity = 0;
    private Object lock = new Object();
    protected volatile int maxDir = 0;
    private SelectMode mode = DEFAULT_MODE;
    Random rand = new Random();


    public UnitedStorage(String[] dirs) {
        this(dirs, DEFAULT_MODE);
    }

    public UnitedStorage(String[] dirs, SelectMode mode) {
        init(dirs, null, mode);
    }

    public UnitedStorage(String dirs) {
        this(dirs, DEFAULT_MODE);
    }

    public UnitedStorage(String dirs, SelectMode mode) {
        String[] paths = dirs.split(",");
        init(paths, null, mode);
    }

    public UnitedStorage(String dirs, String caps) {
        this(dirs, caps, DEFAULT_MODE);
    }

    public UnitedStorage(String dirs, String caps, SelectMode mode) {
        String[] paths = dirs.split(",");
        USDecentralization use = new USDecentralization(new String[]{dirs}, frees);
        use.createIfNotExists(paths);
        String[] capsStr = caps.split(",");
        long[] capsLong = new long[capsStr.length];
        for (int i = 0; i < capsLong.length; i++) {
            capsLong[i] = Long.parseLong(capsStr[i]);
        }

        long[] calCaps = new long[paths.length];
        for (int i = 0; i < paths.length; i++) {
            String path = paths[i];
            long cap = 0;
            if (i < capsLong.length) {
                cap = capsLong[i];
            } else {
                cap = capsLong[capsLong.length - 1];
            }
            // use all the storage space if capacity is configured to -1
            if (cap == -1) {
                File file = new File(path);
                cap = file.getTotalSpace();
            }
            calCaps[i] = cap;
        }

        init(paths, calCaps, mode);
    }

    public UnitedStorage(String[] dirs, long[] capacities) {
        this(dirs, capacities, DEFAULT_MODE);
    }

    public UnitedStorage(String[] dirs, long[] capacities, SelectMode mode) {
        if (dirs.length != capacities.length) {
            log.error("Length of dirs not equal to length of capacities. Ignore capacities parameter");
            capacities = null;
        }
        init(dirs, capacities, mode);
    }



    public void init(String[] dirs, long[] caps, SelectMode mode) {
        USDecentralization use = new USDecentralization(dirs, frees);
        use.createIfNotExists(dirs);
        this.dirs = dirs;

        frees = new long[dirs.length];
        capacities = new long[dirs.length];

        for (int i = 0; i < dirs.length; i++) {
            if (caps == null) {
                File file = new File(dirs[i]);
                capacities[i] = file.getTotalSpace();
            } else {
                capacities[i] = caps[i];
            }
            capacity += capacities[i];
            frees[i] = capacities[i];
            free += frees[i];

            log.info(dirs[i] + " has capacity of " + capacities[i]);
        }
        use.setMode(mode);
    }

    public void take(String path, long size) {
        int idx = containsAbsoluteInternal(path);
        if (idx < 0) {
            log.error(path + " not in the storage: " + toString());
            return;
        }
        USDecentralization use = new USDecentralization(dirs, frees);
        use.take(idx, size);
    }


    public void release(String path, long size) {
        int idx = containsAbsoluteInternal(path);
        if (idx < 0) {
            log.error(path + " not in the storage: " + toString());
            return;
        }
        USDecentralization use = new USDecentralization(dirs, frees);
        use.release(idx, size);
    }





    public String at(int i) {
        return this.dirs[i];
    }

    public long capacity() {
        return this.capacity;
    }

    public long free() {
        return this.free;
    }

    public boolean containsAbsolute(String file) {
        return containsAbsoluteInternal(file) >= 0;
    }

    private int containsAbsoluteInternal(String file) {
        for (int i = 0; i < this.dirs.length; i++) {
            if (file.startsWith(this.dirs[i])) {
                return i;
            }
        }

        return -1;
    }

    protected int containsRelativeInternal(String file) {
        for (int i = 0; i < this.dirs.length; i++) {
            Path absPath = Paths.get(this.dirs[i], file);
            if (absPath.toFile().exists()) {
                return i;
            }
        }

        return -1;
    }
    protected int randomDirInternal(boolean balanced, boolean update) {
        if (!balanced) {
            return rand.nextInt(this.dirs.length);
        } else {
            if (update) {
                updateStat();
            }

            long[] cmpVals = null;
            long cmpTotal = 0;

            if (mode == SelectMode.CAPACITY) {
                cmpVals = capacities;
                cmpTotal = capacity;
            } else if (mode == SelectMode.MAX_FREE) {
                return maxDir;
            } else {
                synchronized (lock) {
                    cmpVals = frees.clone();
                    cmpTotal = free;
                }
            }

            long factor = 1024L * 1024 * 1024;
            int bound = Math.max((int) (cmpTotal / factor), 1);
            int r = rand.nextInt(bound);
            int cum = 0;
            for (int i = 0; i < cmpVals.length; i++) {
                long currVal = Math.max(cmpVals[i], 0L);
                cum += currVal / factor;

                if (cum > r) {
                    return i;
                }
            }

            log.error("Cannot get a reasonable root dir");
            return 0;
        }
    }

    public String toAbsolute(String relativePath) {
        return toAbsolute(relativePath, 0);
    }

    public String toAbsolute(String relativePath, long size) {
        int idx = containsRelativeInternal(relativePath);
        String dir = null;
        if (idx < 0) {
            idx = randomDirInternal(true, false);
        }
        dir = this.dirs[idx];
        if (size > 0) {
            USDecentralization use = new USDecentralization(dirs, frees);
            use.take(idx, size);
        }
        return Paths.get(dir, relativePath).toString();
    }

    @Override
    public String toString() {
        StringBuffer buf = new StringBuffer();
        long factor = 1024L * 1024 * 1024;
        for (int i = 0; i < this.dirs.length; i++) {
            buf.append(this.dirs[i] + ":" + (this.capacities[i] / factor) + " GB");
            if (i != this.dirs.length - 1) {
                buf.append(", ");
            }
        }

        return buf.toString();
    }

    public void updateStat() {
        long[] tmpFrees = null;
        if (mode == SelectMode.SYS_FREE) {
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


    
}
