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

}
