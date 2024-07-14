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

public class DecentralizationObj {
    UnitedStorage.SelectMode mode;
    String[] dirs;
    long[] frees;
    public UnitedStorage.SelectMode getMode() {
        return mode;
    }

    public void setMode(UnitedStorage.SelectMode mode) {
        this.mode = mode;
    }

    public String[] getDirs() {
        return dirs;
    }

    public void setDirs(String[] dirs) {
        this.dirs = dirs;
    }

    public long[] getFrees() {
        return frees;
    }

    public void setFrees(long[] frees) {
        this.frees = frees;
    }

    public DecentralizationObj(UnitedStorage.SelectMode mode, String[] dirs, long[] frees) {
        this.mode = mode;
        this.dirs = dirs;
        this.frees = frees;
    }

}