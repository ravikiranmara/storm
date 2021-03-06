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

package org.apache.storm.topology;

import java.util.Map;

public interface ComponentConfigurationDeclarer<T extends ComponentConfigurationDeclarer> extends ResourceDeclarer<T> {
    /**
     * add in several configs to the component.
     *
     * @param conf the configs to add
     * @return this for chaining.
     */
    T addConfigurations(Map<String, Object> conf);

    /**
     * return the current component configuration.
     *
     * @return the current configuration.
     */
    Map<String, Object> getComponentConfiguration();

    /**
     * Add in a single config.
     *
     * @param config the key for the config
     * @param value  the value of the config
     * @return this for chaining.
     */
    T addConfiguration(String config, Object value);

    /**
     * Turn on/off debug for this component.
     *
     * @param debug true for debug on false for debug off
     * @return this for chaining
     */
    T setDebug(boolean debug);

    /**
     * Set the max task parallelism for this component.
     *
     * @param val the maximum parallelism
     * @return this for chaining
     */
    T setMaxTaskParallelism(Number val);

    /**
     * Set the max spout pending config for this component.
     *
     * @param val the value of max spout pending.
     * @return this for chaining
     */
    T setMaxSpoutPending(Number val);

    /**
     * Set the number of tasks for this component.
     *
     * @param val the number of tasks
     * @return this for chaining.
     */
    T setNumTasks(Number val);

    /**
     * Add generic resources for this component.
     */
    T addResources(Map<String, Double> resources);

    /**
     * Add generic resource for this component.
     */
    T addResource(String resourceName, Number resourceValue);

    /**
     * set the number of workers to be assigned to this component.
     */
    T reserveWorkers(Number numWorkers);
}
