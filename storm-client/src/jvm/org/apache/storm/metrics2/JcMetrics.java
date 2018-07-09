/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.metrics2;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.utils.JCQueue;

public class JcMetrics {
    private final SimpleGauge<Long> capacity;
    private final SimpleGauge<Long> population;
    private final Counter dropped;
    private final Counter arrivals;
    private final Meter dropMeter;
    private final Meter arrivalMeter;
    private static final Logger LOG = LoggerFactory.getLogger(JcMetrics.class);

    JcMetrics(SimpleGauge<Long> capacity, SimpleGauge<Long> population, 
              Counter arrivals, Counter dropped, Meter arrivalMeter, 
              Meter dropMeter) {
        this.capacity = capacity;
        this.population = population;
        this.arrivals = arrivals;
        this.dropped = dropped;
        this.dropMeter = dropMeter;
        this.arrivalMeter = arrivalMeter;
    }

    public long getArrival() {
        return this.arrivals.getCount();
    }

    public long getDropped() {
        return this.dropped.getCount();
    }

    public long getDropMeter() {
        return this.dropMeter.getCount();
    }

    public long getArrivalMeter() {
        return this.arrivalMeter.getCount();
    }

    public void setCapacity(Long capacity) {
        this.capacity.set(capacity);
    }

    public void setPopulation(Long population) {
        this.population.set(population);
    }

    public void markArrivals() {
        this.arrivalMeter.mark();
    }

    public void markDropped() {
        this.dropMeter.mark();
    }

    public void incrementArrivals(long count) {
        this.arrivals.inc(count);
        LOG.info("rkp : arrivals inc by " + count + " : " + this.arrivals.getCount());
    }

    public void incrementDropped(long count) {
        this.dropped.inc(count);
        LOG.info("rkp : dropped inc by " + count + " : " + this.dropped.getCount());
    }

    public void set(JCQueue.QueueMetrics metrics) {
        this.capacity.set(metrics.capacity());
        this.population.set(metrics.population());
        // this.arrivals.inc(metrics.getArrivalAndReset());
        // this.dropped.inc(metrics.getDroppedAndReset());
    }
}

