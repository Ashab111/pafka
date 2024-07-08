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
package org.apache.kafka.clients.admin;
public class QEEobj {
    boolean shouldRetryOnQuotaViolation;
    Throwable throwable;

    int throttleTimeDelta;
    public QEEobj() {
    }
    public QEEobj(boolean shouldRetryOnQuotaViolation, Throwable throwable, int throttleTimeDelta) {
        this.shouldRetryOnQuotaViolation = shouldRetryOnQuotaViolation;
        this.throwable = throwable;

        this.throttleTimeDelta = throttleTimeDelta;
    }



    public boolean getShouldRetryOnQuotaViolation() {
        return shouldRetryOnQuotaViolation;
    }

    public Throwable getThrowable() {
        return throwable;
    }


    public int getThrottleTimeDelta() {
        return throttleTimeDelta;
    }

    public void setShouldRetryOnQuotaViolation(boolean shouldRetryOnQuotaViolation) {
        this.shouldRetryOnQuotaViolation = shouldRetryOnQuotaViolation;
    }

    public void setThrowable(Throwable throwable) {
        this.throwable = throwable;
    }


    public void setThrottleTimeDelta(int throttleTimeDelta) {
        this.throttleTimeDelta = throttleTimeDelta;
    }
}
