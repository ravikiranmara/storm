/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "Licensei"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.scheduler;

        import java.lang.Number;
        import java.util.HashMap;
        import java.util.HashSet;
        import java.util.List;
        import java.util.Map;
        import java.util.Map.Entry;
        import java.util.Set;
        import java.lang.String;
        import org.apache.storm.utils.Utils;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;
        import org.apache.storm.generated.ComponentCommon;
        import org.apache.storm.daemon.StormCommon;
        import org.apache.storm.generated.StormTopology;
        import org.apache.storm.Config;
        import org.json.simple.parser.JSONParser;
        import org.json.simple.JSONObject;
        import org.json.simple.parser.ParseException;
        import java.util.TreeMap;
        import java.util.ArrayList;
        import com.google.common.collect.Sets;
        import java.util.Collections;
        import java.util.Comparator;
        import org.apache.storm.utils.ServerUtils;


public class CGroupScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(CGroupScheduler.class);

    @Override
    public void prepare(Map<String, Object> conf) {
        LOG.info("CGroupScheduler prepare");
    }

    // @Override
    public static List<WorkerSlot> sortSlots(List<WorkerSlot> availableSlots) {
        if (availableSlots != null && availableSlots.size() > 0) {
            Map<String, List<WorkerSlot>> slotGroups = new TreeMap<>();
            for (WorkerSlot slot : availableSlots) {
                String node = slot.getNodeId();
                List<WorkerSlot> slots = new ArrayList<WorkerSlot>();
                if (slotGroups.containsKey(node)) {
                    slots = slotGroups.get(node);
                } else {
                    slots = new ArrayList<WorkerSlot>();
                    slotGroups.put(node, slots);
                }
                slots.add(slot);
            }

            for (List<WorkerSlot> slots : slotGroups.values()) {
                Collections.sort(slots, new Comparator<WorkerSlot>() {
                    @Override
                    public int compare(WorkerSlot o1, WorkerSlot o2) {
                        return o1.getPort() - o2.getPort();
                    }
                });
            }

            List<List<WorkerSlot>> list = new ArrayList<List<WorkerSlot>>(slotGroups.values());
            Collections.sort(list, new Comparator<List<WorkerSlot>>() {
                @Override
                public int compare(List<WorkerSlot> o1, List<WorkerSlot> o2) {
                    return o2.size() - o1.size();
                }
            });

            return ServerUtils.interleaveAll(list);
        }

        return null;
    }

    public static Map<WorkerSlot, List<ExecutorDetails>> getAliveAssignedWorkerSlotExecutors (Cluster cluster, String topologyId) {
        SchedulerAssignment existingAssignment = cluster.getAssignmentById(topologyId);
        Map<ExecutorDetails, WorkerSlot> executorToSlot = new HashMap<ExecutorDetails, WorkerSlot>();
        if (existingAssignment != null) {
            executorToSlot = existingAssignment.getExecutorToSlot();
        }

        if(executorToSlot == null) {
            LOG.info("Exec to slot is null");
            executorToSlot = new HashMap<ExecutorDetails, WorkerSlot>();
        }

        return Utils.reverseMap(executorToSlot);
    }

    public static Map<ExecutorDetails, WorkerSlot> getAliveAssignedExecutorWorkerSlot (Cluster cluster, String topologyId) {
        SchedulerAssignment existingAssignment = cluster.getAssignmentById(topologyId);
        Map<ExecutorDetails, WorkerSlot> executorToSlot = new HashMap<ExecutorDetails, WorkerSlot>();
        if (existingAssignment != null) {
            executorToSlot = existingAssignment.getExecutorToSlot();
        }

        if(executorToSlot == null) {
            LOG.info("Exec to slot is null");
            executorToSlot = new HashMap<ExecutorDetails, WorkerSlot>();
        }

        return executorToSlot;
    }

    public static Map<String, Number> getComponentToNumWorkerRequested (TopologyDetails topology) {
        Map<String, Number> ret = new HashMap<String, Number>();
        Map<String, Component> comps = topology.getComponents();
        StormTopology st = topology.getTopology();

        // get the number of workers requested by each component add it to get total sum
        try {
            Number totalComponentWorkers = 0;
            for (Map.Entry<String, Component> entry : comps.entrySet()) {
                String compId = entry.getKey();
                ComponentCommon ccom = Utils.getComponentCommon(st, compId);

                JSONParser parser = new JSONParser();
                JSONObject jsonCompCom = (JSONObject) parser.parse(ccom.get_json_conf());
                Number compWorkers = (Number)jsonCompCom.get(Config.TOPOLOGY_COMPONENT_WORKERS);

                LOG.info("rpk:workers requested for Component {}:{}", compId, compWorkers);
                totalComponentWorkers  = totalComponentWorkers.intValue() + compWorkers.intValue();
                ret.put(compId, compWorkers);
            }
        } catch (ParseException ex) {
            throw new RuntimeException("Failed to parse component resources with json");
        }

        return ret;
    }

    public static Map<ExecutorDetails, String> getExecutorToComponent(TopologyDetails topology) {
        Map<ExecutorDetails, String> ret = new HashMap<ExecutorDetails, String>();

        Map<String, Component> comps = topology.getComponents();
        for (Map.Entry<String, Component> entry : comps.entrySet()) {
            String compId = entry.getKey();
            List<ExecutorDetails> execs = entry.getValue().getExecs();

            for (ExecutorDetails exec : execs) {
                ret.put(exec, compId);
            }
        }

        return ret;
    }

    public static Map<ExecutorDetails, String> getAliveAssignedExecutorToSystem (TopologyDetails topology, Cluster cluster) {
        Map<ExecutorDetails, WorkerSlot> aliveExecutorToWorker =
                getAliveAssignedExecutorWorkerSlot(cluster, topology.getId());
        Map<ExecutorDetails, String> allExecutorToComponent = getExecutorToComponent(topology);
        Map<ExecutorDetails, String> ret = new HashMap<ExecutorDetails, String>();

        for (Map.Entry<ExecutorDetails, WorkerSlot> exec : aliveExecutorToWorker.entrySet()) {
            if (allExecutorToComponent.get(exec.getKey()) == null)
                ret.put(exec.getKey(), allExecutorToComponent.get(exec.getKey()));
        }

        return ret;
    }



    public static Map<ExecutorDetails, String> getAliveAssignedExecutorToComponent(TopologyDetails topology, Cluster cluster) {
        Map<ExecutorDetails, WorkerSlot> aliveExecutorToWorker =
                getAliveAssignedExecutorWorkerSlot(cluster, topology.getId());
        Map<ExecutorDetails, String> allExecutorToComponent = getExecutorToComponent(topology);
        Map<ExecutorDetails, String> ret = new HashMap<ExecutorDetails, String>();

        for (Map.Entry<ExecutorDetails, WorkerSlot> exec : aliveExecutorToWorker.entrySet()) {
            if (allExecutorToComponent.get(exec.getKey()) != null)
                ret.put(exec.getKey(), allExecutorToComponent.get(exec.getKey()));
        }

        return ret;
    }

    public static Map<String, Set<WorkerSlot>> getAliveAssignedComponentToWorkerSlot(TopologyDetails topology, Cluster cluster) {
        Map<String, Set<WorkerSlot>> ret = new HashMap<String, Set<WorkerSlot>>();
        Map<String, List<WorkerSlot>> tempret = new HashMap<String, List<WorkerSlot>>();
        Map<String, Component> comps = topology.getComponents();

        Map<ExecutorDetails, String> executorToComponent = getExecutorToComponent(topology);
        Map<ExecutorDetails, WorkerSlot> executorToWorker =
                getAliveAssignedExecutorWorkerSlot(cluster, topology.getId());

        for (Map.Entry entry : executorToWorker.entrySet()) {
            LOG.info("ex {} : wo {}", entry.getKey(), entry.getValue());
        }

        for(Map.Entry <ExecutorDetails, String> execomp : executorToComponent.entrySet()) {
            List<WorkerSlot> temp = new ArrayList<WorkerSlot>();
            if(tempret.get(execomp.getValue()) != null) {
                temp = tempret.get(execomp.getValue());
            }

            if (null != executorToWorker.get(execomp.getKey())) {
                temp.add(executorToWorker.get(execomp.getKey()));
            }

            tempret.put(execomp.getValue(), temp);
        }

        for (Map.Entry <String, List<WorkerSlot>> compslot : tempret.entrySet()) {
            ret.put(compslot.getKey(), new HashSet<WorkerSlot>(compslot.getValue()));
        }

        return ret;
    }

    public static Map<String, List<ExecutorDetails>> getComponentToExecutorList (Map<ExecutorDetails, String> execComp) {
        Set <String> compset = new HashSet<String> (execComp.values());
        List <String> comps = new ArrayList<String> (compset);

        Map <String, List<ExecutorDetails>> ret = new HashMap <String, List<ExecutorDetails>>();
        List<ExecutorDetails> execList = new ArrayList<ExecutorDetails>();

        for (String compId : comps) {
            execList = new ArrayList<ExecutorDetails>();

            for (Map.Entry <ExecutorDetails, String> entry : execComp.entrySet()) {
                if(entry.getValue() == null)
                    LOG.info("Missing Comp : {}", entry.getKey());

                if (entry.getValue().equals(compId)) {
                    execList.add(entry.getKey());
                }
            }

            ret.put(compId, execList);
        }

        return ret;
    }

    public static List<ExecutorDetails> getUnassignedSystemExecutor (TopologyDetails topology, Cluster cluster) {
        Set<ExecutorDetails> allExecutors = topology.getExecutors();
        Map<ExecutorDetails, WorkerSlot> aliveAssignedExecutors = getAliveAssignedExecutorWorkerSlot (cluster, topology.getId());
        Map<ExecutorDetails, String> allComponentExecutors = getExecutorToComponent(topology);

        // get all alive executors
        Set<ExecutorDetails> unassignedExecutorsSet = Sets.difference(allExecutors, aliveAssignedExecutors.keySet());

        // remove component executors
        unassignedExecutorsSet = Sets.difference(unassignedExecutorsSet, allComponentExecutors.keySet());

        return new ArrayList<ExecutorDetails>(unassignedExecutorsSet);
    }



    // get all executors, and alive executors by component
    // get alive workers, assign to component
    // get required workers for each component
    // get worker to executor map for reassignment
    public static Map<WorkerSlot, List<ExecutorDetails>> getReassignExecutorsMap (TopologyDetails topology, Cluster cluster) {
        Map<WorkerSlot, List<ExecutorDetails>> reassignment = new HashMap<WorkerSlot, List<ExecutorDetails>> ();

        // list component executors that need assignment
        Map<ExecutorDetails, String> allExecToComponent = getExecutorToComponent(topology);
        Map<ExecutorDetails, String> aliveExecToComponent = getAliveAssignedExecutorToComponent(topology, cluster);
        Map<ExecutorDetails, String> aliveExecToSystem = getAliveAssignedExecutorToSystem(topology, cluster);
        Map<ExecutorDetails, String> needsAllocExec = new HashMap<ExecutorDetails, String>();

        List<ExecutorDetails> allExecs = new ArrayList<ExecutorDetails>(topology.getExecutors());
        List<ExecutorDetails> aliveExecs = new ArrayList<ExecutorDetails>(aliveExecToComponent.keySet());
        List<ExecutorDetails> aliveSystems = new ArrayList<ExecutorDetails>(aliveExecToSystem.keySet());

        List<ExecutorDetails> needExecs = allExecs;
        needExecs.removeAll(aliveExecs);
        needExecs.removeAll(aliveSystems);

        LOG.info("Executors that need assignment");
        for(ExecutorDetails exec : needExecs) {
            LOG.info("{}", exec);
        }

        // list of workers needed
        Map<String, Set<WorkerSlot>> aliveAssignedWorker = getAliveAssignedComponentToWorkerSlot(topology, cluster);
        Map<String, Number> getAllWorkers = getComponentToNumWorkerRequested (topology);

        // calculate the number of workers required
        Number totalSpecifiedWorker = topology.getNumWorkers();
        List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
        Number totalAvailableWorkers = availableSlots.size();

        Map<WorkerSlot, List<ExecutorDetails>> aliveAssignedWorkerExecutor =
                getAliveAssignedWorkerSlotExecutors (cluster, topology.getId());

        // get requested - assigned map
        Map<String, Number> getNeededWorkers = new HashMap<String, Number> ();
        Number totalNeeded = 0;
        for (Map.Entry <String, Number> compWork : getAllWorkers.entrySet()) {
            String compId = compWork.getKey();
            Number requested = compWork.getValue();
            Number assigned = aliveAssignedWorker.get(compId).size();

            if (requested.intValue() - assigned.intValue() > 0) {
                int diff = (requested.intValue() - assigned.intValue());
                totalNeeded = totalNeeded.intValue() + diff;
                getNeededWorkers.put(compId, diff);
                LOG.info("needed worker comp : {}, diff : {}", compId, diff);
            }
        }

        LOG.info("needed workers");
        for (Map.Entry <String, Number> entry : getNeededWorkers.entrySet()) {
            LOG.info("{} : {}", entry.getKey(), entry.getValue());
        }

        LOG.info("Total workers needed : {}", totalNeeded.intValue());

        // get unassigned component to executor map
        Map<String, List<ExecutorDetails>> allCompToExecList = getComponentToExecutorList (allExecToComponent);
        Map<String, List<ExecutorDetails>> aliveCompToExecList = getComponentToExecutorList (aliveExecToComponent);
        Map<String, List<ExecutorDetails>> unassignedCompToExecList  = new HashMap<String, List<ExecutorDetails>>();

        dumpComponentExecutor(allCompToExecList);
        dumpComponentExecutor(aliveCompToExecList);

        for (Map.Entry<String, List<ExecutorDetails>> entry : allCompToExecList.entrySet()) {
            List<ExecutorDetails> allUnassExecs = entry.getValue();
            List<ExecutorDetails> aliveCompList = aliveCompToExecList.get(entry.getKey());
            if (aliveCompList != null)
                allUnassExecs.removeAll(aliveCompList);

            unassignedCompToExecList.put(entry.getKey(), allUnassExecs);
        }

        // get workers and start assigning to get final assignment
        if(availableSlots.size() <= 0 || totalNeeded.intValue() == 0 || availableSlots.size() < totalNeeded.intValue()) {
            LOG.info("Not enough available workers or not required: available {} - needed {}", availableSlots.size(), totalNeeded.intValue());
            return reassignment;
        }

        LOG.info("component to execs workers");
        for (Map.Entry <String, List<ExecutorDetails>> entry : unassignedCompToExecList.entrySet()) {
            LOG.info("{}", entry.getKey());
            for (ExecutorDetails ex : entry.getValue())
                LOG.info("{} ", ex);
        }

        List<WorkerSlot> sortedList = sortSlots(availableSlots);
        List<WorkerSlot> assignedSlots = new ArrayList<WorkerSlot>();
        int indexToSorted = 0;

        // reassign component execs
        Map<ExecutorDetails, WorkerSlot> reassignmentRev = new HashMap<ExecutorDetails, WorkerSlot>();
        for (Map.Entry<String, List<ExecutorDetails>> entry :  unassignedCompToExecList.entrySet()) {
            int workerNeededForComponent = 0;
            if (getNeededWorkers.get(entry.getKey()) != null) {
                LOG.info("Missing needed worker number : {}", entry.getKey());
                workerNeededForComponent = getNeededWorkers.get(entry.getKey()).intValue();
            }

            List<WorkerSlot> allocList = new ArrayList<WorkerSlot>();

            for (int i=0; i<workerNeededForComponent; i++) {
                allocList.add(sortedList.get(i+indexToSorted));
                assignedSlots.add(sortedList.get(i+indexToSorted));
            }
            indexToSorted += workerNeededForComponent;

            List<ExecutorDetails> unassignedExecs = entry.getValue();

            for (int i=0; i<unassignedExecs.size(); i++) {
                LOG.info("rpk: reassign {} : {}", unassignedExecs.get(i), allocList.get(i%allocList.size()));
                reassignmentRev.put(unassignedExecs.get(i), allocList.get(i % allocList.size()));
            }
        }

        // reassign system components
        List<ExecutorDetails> unassignedSystemExecs = getUnassignedSystemExecutor(topology, cluster);
        for (int i=0; i<unassignedSystemExecs.size(); i++) {
            LOG.info("rpk: reassign {} : {}", unassignedSystemExecs.get(i), assignedSlots.get(i%assignedSlots.size()));
            reassignmentRev.put(unassignedSystemExecs.get(i), assignedSlots.get(i % assignedSlots.size()));
        }

        reassignment = Utils.reverseMap(reassignmentRev);

        // reassignment rev
        return reassignment;
    }

    public static void dumpAssignment(Map<WorkerSlot, List<ExecutorDetails>> assignment) {
        LOG.info("Assignment map");
        for (Map.Entry <WorkerSlot, List<ExecutorDetails>> entry : assignment.entrySet()) {
            LOG.info("{}", entry.getKey());
            for (ExecutorDetails ex : entry.getValue())
                LOG.info("{}", ex);
        }

        return;
    }

    public static void dumpComponentExecutor(Map<String, List<ExecutorDetails>> compExec) {
        LOG.info("Component Executor map");
        for (Map.Entry <String, List<ExecutorDetails>> entry : compExec.entrySet()) {
            LOG.info("{}", entry.getKey());
            for (ExecutorDetails ex : entry.getValue())
                LOG.info("{}", ex);
        }

        return;
    }

    public static void dumpExecutorComponent(Map<ExecutorDetails, String> execComp) {
        LOG.info("Executor Component Map");
        for (Map.Entry <ExecutorDetails, String> entry : execComp.entrySet())
            LOG.info("{} : {}", entry.getKey(), entry.getValue());

        return;
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.info("rpk:Call Schedule for CGroupScheduler");

        for (TopologyDetails topology : topologies.getTopologies()) {
            LOG.info("rpk:Processing topology : {}", topology);
            Map<WorkerSlot, List<ExecutorDetails>> execmap = getReassignExecutorsMap(topology, cluster);

            dumpAssignment(execmap);

            for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : execmap.entrySet()) {
                WorkerSlot slot = entry.getKey();
                List<ExecutorDetails> execs = entry.getValue();
                cluster.assign(slot, topology.getId(), execs);
            }
        }
    }

    @Override
    public Map<String, Map<String, Double>> config() {
        return new HashMap<>();
    }
}
