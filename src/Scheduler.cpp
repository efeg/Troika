// Copyright (c) 2014, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//     * Redistributions of source code must retain the above copyright
//       notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of the <organization> nor the
//       names of its contributors may be used to endorse or promote products
//       derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
// DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include "Scheduler.h"
#include <iostream>

Scheduler::Scheduler(std::vector<double> queueCapacities,
		double maxAmResourcePercent, size_t minAllocMB, size_t maxAllocMB,
		int minAllocVcores, int maxAllocVcores, bool resourceCalculator) :
		queueCapacities_(queueCapacities),
		maxAmResourcePercent_(maxAmResourcePercent),
		minAllocMB_(minAllocMB),
		maxAllocMB_(maxAllocMB),
		minAllocVcores_(minAllocVcores),
		maxAllocVcores_(maxAllocVcores),
		resourceCalculator_(resourceCalculator),
		maxAlreadySet_(false){
}

Scheduler::~Scheduler() {
}

double Scheduler::getMaxAmResourcePercent() const {
	return maxAmResourcePercent_;
}

void Scheduler::setMaxAllocMb(size_t maxAllocMb) {
	maxAllocMB_ = maxAllocMb;
}

void Scheduler::setMaxAllocVcores(int maxAllocVcores) {
	maxAllocVcores_ = maxAllocVcores;
}

void Scheduler::setResourceCalculator(bool resourceCalculator){
	resourceCalculator_ = resourceCalculator;
}

void Scheduler::setMaxAmResourcePercent(double maxAmResourcePercent) {
	maxAmResourcePercent_ = maxAmResourcePercent;
}

void Scheduler::setMinAllocMb(size_t minAllocMb) {
	minAllocMB_ = minAllocMb;
}

void Scheduler::setMinAllocVcores(int minAllocVcores) {
	minAllocVcores_ = minAllocVcores;
}

void Scheduler::setQueueCapacities(const std::vector<double>& queueCapacities) {
	queueCapacities_ = queueCapacities;
	queueRemainingCoreCapacities_.resize(queueCapacities_.size());
	queueRemainingMemoryCapacities_.resize(queueCapacities_.size());
}

double Scheduler::getQueueCapacity(size_t queueIndex) const {
	if (queueIndex < queueCapacities_.size()) {
		return queueCapacities_.at(queueIndex);
	}
	std::cerr << "Index out of bound in Scheduler Queues - queue index must be in [(# of queues)-1]" << std::endl;
	return -1;
}

// nodemanagerEventType = schedulerSubmitNewApp(ev->getAppID());
// using app id, scheduler will determine the app. then using app it will determine which queue it will
// submit to and whether that queue has enough resource for AM of app.
// if there are available resources (amResourceMB, amCpuVcores) for it in some node, it will give that
// resource to it. it will scan the worker nodes in order to determine if space exists..

int Scheduler::schedulerSubmitNewApp(size_t appId, int outputEventType){

	// the queue that the application will be submitted to...
	int queueID = (applications.at(appId))->getQueueId();

	// initialize active map and reduce tasks
	activeMapTasks_[appId] = 0;
	activeReduceTasks_[appId] = 0;

	// required resources for AM
	size_t amResourcesMB = (applications.at(appId))->getAmResourceMb();
	amResourcesMB *= ONE_MB_IN_BYTES;

	int amCpuVcores;	// used in case resourceCalculator is true
	bool cpuSatisfactionCheck = true;
	if(isResourceCalculator()){
		amCpuVcores = (applications.at(appId))->getamCpuVcores();
		cpuSatisfactionCheck = (amCpuVcores <= (maxAmResourcePercent_ * queueRemainingCoreCapacities_.at(queueID)));
	}

	// check if there's enough resources in app's queue for the AM
	if (amResourcesMB <= (maxAmResourcePercent_ * queueRemainingMemoryCapacities_.at(queueID)) && cpuSatisfactionCheck) {

		// find a worker node (a node with a node manager) that contains these resources
		for (size_t i=0; i < workerNodeExpectedEventTypes_.size(); i++) {

			if(nodeExpEventT_nodeType[workerNodeExpectedEventTypes_.at(i)] == RESOURCEMANAGER){
				continue;
			}

			if(isResourceCalculator()){
				cpuSatisfactionCheck = nodeExpEventT_remainingCap[workerNodeExpectedEventTypes_.at(i)].remainingCpuCapacity * maxAmResourcePercent_ >= amCpuVcores;
			}

			if (nodeExpEventT_remainingCap[workerNodeExpectedEventTypes_.at(i)].remainingMemoryCapacity >= amResourcesMB && cpuSatisfactionCheck) {

				if(isResourceCalculator()){
					// seize and decrease seized queue resources (real CPU will cease it when the event is received by the corresponding worker node)
					nodeExpEventT_remainingCap[workerNodeExpectedEventTypes_.at(i)].remainingCpuCapacity -= amCpuVcores;
					queueRemainingCoreCapacities_.at(queueID) -= amCpuVcores;
				}

				// seize those resource
				nodeExpEventT_remainingCap[workerNodeExpectedEventTypes_.at(i)].remainingMemoryCapacity -= amResourcesMB;

				// decrease the seized queue resources
				queueRemainingMemoryCapacities_.at(queueID) -= amResourcesMB;
				#ifndef TERMINAL_LOG_DISABLED
				std::cout << "SCHE||INFO: queueRemainingMemoryCapacities_ amResourcesMB "<< queueRemainingMemoryCapacities_.at(queueID) << std::endl;
				#endif
				// return the eventType of the node that contain these resources
				return workerNodeExpectedEventTypes_.at(i);
			}
		}
		struct amScheduleData pendingApp;
		pendingApp.appId_ = appId;
		pendingApp.outputEventType_ = outputEventType;
		containerPendingAppQueue_[(applications.at(appId))->getQueueId()].push(pendingApp);
		#ifndef TERMINAL_LOG_DISABLED
		// if a worker node cannot be found, application cannot be submitted
		std::cerr << "SCHE||INFO: Insufficient RESOURCES AT WORKER NODES FOR APP MASTER! AppId: " << appId << std::endl;
		#endif
	}
	else {
		struct amScheduleData pendingApp;
		pendingApp.appId_ = appId;
		pendingApp.outputEventType_ = outputEventType;
		containerPendingAppQueue_[(applications.at(appId))->getQueueId()].push(pendingApp);
		#ifndef TERMINAL_LOG_DISABLED
		std::cout << "SCHE||INFO: Insufficient RESOURCES FOR APP MASTER! AppId: " << appId << std::endl;
		#endif
	}
	return -1;
}

void Scheduler::incnodeId_maxNumberOfMapTaskPerNode(int fsloc, int appID){

	if (!maxAlreadySet_) {	// (max is not set for each node)
		// increment map counter for nodeId
		if ( nodeId_maxNumberOfMapTaskPerNode.find(fsloc) == nodeId_maxNumberOfMapTaskPerNode.end() ) {	// not found
			nodeId_maxNumberOfMapTaskPerNode[fsloc] = 1;
		}
		else {	// found
			nodeId_maxNumberOfMapTaskPerNode[fsloc]++;
		}
	}
}

int Scheduler::getnodeId_maxNumberOfMapTaskPerNode(int nodeID){
	return nodeId_maxNumberOfMapTaskPerNode[nodeID];
}

void Scheduler::setWaitingTaskInfo(waitingTaskInfo &info, size_t appID, int taskLocation, int attr, int fsID, int outEvent, bool taskType){
	info.appId = appID;
	info.taskLocation = taskLocation;
	info.attr = attr;
	info.fsid = fsID;
	info.outEvent = outEvent;
	info.taskType = taskType;	// false: map true: reducer
}

struct waitingTaskInfo Scheduler::schedulerGetMapperLocation(size_t appId, int fileSplitLocationExpectedEventType, bool wakeUpSignal, int outEvent, int attr, int fsid){

	struct waitingTaskInfo mapInf;
	setWaitingTaskInfo(mapInf, appId, -1, attr, fsid, outEvent, false);

	size_t neededMemory = ONE_MB_IN_BYTES * (applications.at(appId))->getMapreduceMapMemory();
	int neededCpu = (applications.at(appId))->getMapCpuVcores();
	bool cpuRelatedCheck =true;

	if (neededMemory > (maxAllocMB_*ONE_MB_IN_BYTES) || (isResourceCalculator() && (neededCpu > maxAllocVcores_))) {
		std::cerr << "Maximum allocation allowance per container is exceeded in map task scheduler: " << appId << std::endl;
		maxAlreadySet_ = true; // cannot run any map task at any node.
		return mapInf;
	}

	size_t allocatedMemory;
	int allocatedCpu = neededCpu;

	if (neededMemory % (minAllocMB_*ONE_MB_IN_BYTES)) {
		allocatedMemory = ((neededMemory / (minAllocMB_*ONE_MB_IN_BYTES)) + 1) * (minAllocMB_*ONE_MB_IN_BYTES);
	}
	else {
		allocatedMemory = neededMemory;
	}

	if(isResourceCalculator()){
		if (neededCpu % minAllocVcores_) {
			allocatedCpu = ((neededCpu / minAllocVcores_) + 1) * minAllocVcores_;
		}
		else {
			allocatedCpu = neededCpu;
		}
	}

	// initial submission for this filesplit
	if(!wakeUpSignal){

		if (allocatedMemory > queueRemainingMemoryCapacities_.at((applications.at(appId))->getQueueId())
				|| (isResourceCalculator() && (allocatedCpu > queueRemainingCoreCapacities_.at((applications.at(appId))->getQueueId())))){
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "SCHE||INFO: Insufficient RESOURCES FOR MAPPER! AppId: " << appId << std::endl;
			#endif

			//Put them all to a wait list (suspend)! set the waiting task properties...
			struct waitingTaskInfo info;
			setWaitingTaskInfo(info, appId, fileSplitLocationExpectedEventType, attr, fsid, outEvent, false);
			waitingMapperQueue_[(applications.at(appId))->getQueueId()].push(info);

			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "SCHE||INFO: Map task waiting list size: " << waitingMapperQueue_[(applications.at(appId))->getQueueId()].size() << std::endl;
			#endif
			maxAlreadySet_ = true; // cannot run any more map task at any node.
			return mapInf;
		}

		// search for a node that has available resources at least as much as allocatedMemory and allocatedCpu
		// prefer the local node if possible, else check another node in the same rack else get any available node

		if(isResourceCalculator()){
			cpuRelatedCheck = nodeExpEventT_remainingCap[fileSplitLocationExpectedEventType].remainingCpuCapacity >= allocatedCpu;
		}

		// first check if local node has the needed resources
		if (nodeExpEventT_nodeType[fileSplitLocationExpectedEventType] != RESOURCEMANAGER && cpuRelatedCheck
			&& nodeExpEventT_remainingCap[fileSplitLocationExpectedEventType].remainingMemoryCapacity >= allocatedMemory) {

			if(isResourceCalculator()){
				nodeExpEventT_remainingCap[fileSplitLocationExpectedEventType].remainingCpuCapacity -= allocatedCpu;
				queueRemainingCoreCapacities_.at((applications.at(appId))->getQueueId()) -= allocatedCpu;
			}

			// seize those resource (real CPU will cease it when the event is received by the corresponding worker node)
			nodeExpEventT_remainingCap[fileSplitLocationExpectedEventType].remainingMemoryCapacity -= allocatedMemory;

			// set seized resource per map task for application
			// note that setSeizedMapCpuVcores in any case. If default resource scheduler is used it will always be the default requested amount
			(applications.at(appId))->setSeizedMapCpuVcores(allocatedCpu);
			(applications.at(appId))->setSeizedMapreduceMapMemory(allocatedMemory);

			// decrease the seized queue resources
			queueRemainingMemoryCapacities_.at((applications.at(appId))->getQueueId()) -= allocatedMemory;

			// return the eventType of the node that contain these resources
			mapInf.taskLocation = fileSplitLocationExpectedEventType;
			incnodeId_maxNumberOfMapTaskPerNode(fileSplitLocationExpectedEventType, appId);
			return mapInf;
		}
		// if local node was not available (or if it is the RESOURCEMANAGER) try finding a node in the same rack
		// if all the nodes are unavailable in the same rack, pick a random available node
		// rack id of fileSplit Location;
		int rackEventType = nodeExpEventT_switchExpEventT[fileSplitLocationExpectedEventType];
		int eventType, backupEvent;
		bool backupFound = false;

		for (size_t i=0; i < workerNodeExpectedEventTypes_.size(); i++) {

			eventType = workerNodeExpectedEventTypes_.at(i);

			if(isResourceCalculator()){
				cpuRelatedCheck = nodeExpEventT_remainingCap[eventType].remainingCpuCapacity >= allocatedCpu;
			}
			if (rackEventType == nodeExpEventT_switchExpEventT[eventType]
	 		     && nodeExpEventT_nodeType[eventType] != RESOURCEMANAGER && cpuRelatedCheck
				 && nodeExpEventT_remainingCap[eventType].remainingMemoryCapacity >= allocatedMemory) {

				if(isResourceCalculator()){
					nodeExpEventT_remainingCap[eventType].remainingCpuCapacity -= allocatedCpu;
					queueRemainingCoreCapacities_.at((applications.at(appId))->getQueueId()) -= allocatedCpu;
				}

				// seize those resource (real CPU will cease it when the event is received by the corresponding worker node)
				nodeExpEventT_remainingCap[eventType].remainingMemoryCapacity -= allocatedMemory;

				// set seized resource per mapper for application
				// note that setSeizedMapCpuVcores in any case. If default resource scheduler is used it will always be the default requested amount
				(applications.at(appId))->setSeizedMapCpuVcores(allocatedCpu);
				(applications.at(appId))->setSeizedMapreduceMapMemory(allocatedMemory);

				// decrease the seized queue resources
				queueRemainingMemoryCapacities_.at((applications.at(appId))->getQueueId()) -= allocatedMemory;

				mapInf.taskLocation = eventType;
				incnodeId_maxNumberOfMapTaskPerNode(eventType, appId);
				return mapInf;
			}

			else if (!backupFound && nodeExpEventT_nodeType[eventType] != RESOURCEMANAGER && cpuRelatedCheck
					&& nodeExpEventT_remainingCap[eventType].remainingMemoryCapacity >= allocatedMemory) {
				backupFound = true;
				backupEvent = eventType;
			}
		}

		if (backupFound) {
			if(isResourceCalculator()){
				nodeExpEventT_remainingCap[backupEvent].remainingCpuCapacity -= allocatedCpu;
				queueRemainingCoreCapacities_.at((applications.at(appId))->getQueueId()) -= allocatedCpu;
			}
			// seize those resource (real CPU will cease it when the event is received by the corresponding worker node)
			nodeExpEventT_remainingCap[backupEvent].remainingMemoryCapacity -= allocatedMemory;

			// set seized resource per mapper for application
			(applications.at(appId))->setSeizedMapCpuVcores(allocatedCpu);
			(applications.at(appId))->setSeizedMapreduceMapMemory(allocatedMemory);

			// decrease the seized queue resources
			queueRemainingMemoryCapacities_.at((applications.at(appId))->getQueueId()) -= allocatedMemory;

			mapInf.taskLocation = backupEvent;
			incnodeId_maxNumberOfMapTaskPerNode(backupEvent, appId);
			return mapInf;
		}
	}
	else{

		// search for a node that has available resources at least as much as allocatedMemory and allocatedCpu
		// prefer the local node if possible, else check another node in the same rack else get any available node

		if(isResourceCalculator()){
			cpuRelatedCheck = nodeExpEventT_remainingCap[fileSplitLocationExpectedEventType].remainingCpuCapacity >= allocatedCpu;
		}
		// first check if local node has the needed resources
		if (nodeExpEventT_nodeType[fileSplitLocationExpectedEventType] != RESOURCEMANAGER && cpuRelatedCheck
			&& nodeExpEventT_remainingCap[fileSplitLocationExpectedEventType].remainingMemoryCapacity >= allocatedMemory) {

			if(isResourceCalculator()){
				nodeExpEventT_remainingCap[fileSplitLocationExpectedEventType].remainingCpuCapacity -= allocatedCpu;
				queueRemainingCoreCapacities_.at((applications.at(appId))->getQueueId()) -= allocatedCpu;
			}

			// seize those resource (real CPU will cease it when the event is received by the corresponding worker node)
			nodeExpEventT_remainingCap[fileSplitLocationExpectedEventType].remainingMemoryCapacity -= allocatedMemory;

			// set seized resource per mapper for application
			(applications.at(appId))->setSeizedMapCpuVcores(allocatedCpu);
			(applications.at(appId))->setSeizedMapreduceMapMemory(allocatedMemory);

			// decrease the seized queue resources
			queueRemainingMemoryCapacities_.at((applications.at(appId))->getQueueId()) -= allocatedMemory;

			// return the eventType of the node that contain these resources
			mapInf.taskLocation = fileSplitLocationExpectedEventType;
			return mapInf;
		}

		// if local node was not available (or if it is the RESOURCEMANAGER) try finding a node in the same rack
		// if all the nodes are unavailable in the same rack, pick a random available node

		// rack id of fileSplit Location;
		int rackEventType = nodeExpEventT_switchExpEventT[fileSplitLocationExpectedEventType];
		int eventType, backupEvent;
		bool backupFound = false;

		for (size_t i=0; i < workerNodeExpectedEventTypes_.size(); i++) {

			eventType = workerNodeExpectedEventTypes_.at(i);
			if(isResourceCalculator()){
				cpuRelatedCheck = nodeExpEventT_remainingCap[eventType].remainingCpuCapacity >= allocatedCpu;
			}
			if (rackEventType == nodeExpEventT_switchExpEventT[eventType]
	 		     && nodeExpEventT_nodeType[eventType] != RESOURCEMANAGER
				 && cpuRelatedCheck
				 && nodeExpEventT_remainingCap[eventType].remainingMemoryCapacity >= allocatedMemory) {

				if(isResourceCalculator()){
					nodeExpEventT_remainingCap[eventType].remainingCpuCapacity -= allocatedCpu;
					queueRemainingCoreCapacities_.at((applications.at(appId))->getQueueId()) -= allocatedCpu;
				}

				// seize those resource (real CPU will cease it when the event is received by the corresponding worker node)
				nodeExpEventT_remainingCap[eventType].remainingMemoryCapacity -= allocatedMemory;

				// set seized resource per mapper for application
				(applications.at(appId))->setSeizedMapCpuVcores(allocatedCpu);
				(applications.at(appId))->setSeizedMapreduceMapMemory(allocatedMemory);

				// decrease the seized queue resources
				queueRemainingMemoryCapacities_.at((applications.at(appId))->getQueueId()) -= allocatedMemory;

				mapInf.taskLocation = eventType;
				return mapInf;
			}

			else if (!backupFound && nodeExpEventT_nodeType[eventType] != RESOURCEMANAGER && cpuRelatedCheck
					&& nodeExpEventT_remainingCap[eventType].remainingMemoryCapacity >= allocatedMemory) {

				backupFound = true;
				backupEvent = eventType;
			}
		}

		if (backupFound) {

			if(isResourceCalculator()){
				nodeExpEventT_remainingCap[backupEvent].remainingCpuCapacity -= allocatedCpu;
				queueRemainingCoreCapacities_.at((applications.at(appId))->getQueueId()) -= allocatedCpu;
			}

			// seize those resource (real CPU will cease it when the event is received by the corresponding worker node)
			nodeExpEventT_remainingCap[backupEvent].remainingMemoryCapacity -= allocatedMemory;

			// set seized resource per mapper for application
			(applications.at(appId))->setSeizedMapCpuVcores(allocatedCpu);
			(applications.at(appId))->setSeizedMapreduceMapMemory(allocatedMemory);

			// decrease the seized queue resources
			queueRemainingMemoryCapacities_.at((applications.at(appId))->getQueueId()) -= allocatedMemory;

			mapInf.taskLocation = backupEvent;
			return mapInf;
		}
	}

	#ifndef TERMINAL_LOG_DISABLED
	// no node has enough resources for the mapper
	std::cerr << "SCHE||INFO: Not enough resources for the mapper: " << appId << std::endl;
	#endif

	struct waitingTaskInfo info;
	setWaitingTaskInfo(info, appId, fileSplitLocationExpectedEventType, attr, fsid, outEvent, false);
	waitingMapperQueue_[(applications.at(appId))->getQueueId()].push(info);

	#ifndef TERMINAL_LOG_DISABLED
	std::cout << "SCHE||INFO: Map task waiting list size: " << waitingMapperQueue_[(applications.at(appId))->getQueueId()].size() << std::endl;
	#endif

	mapInf.taskLocation = -1;
	return mapInf;
}

struct waitingTaskInfo Scheduler::schedulerGetReducerLocation(size_t appId, bool wakeUpSignal, int outEvent){

	struct waitingTaskInfo redInf;
	setWaitingTaskInfo(redInf, appId, -1, -1, -1, outEvent, true);

	size_t neededMemory = (applications.at(appId))->getMapreduceReduceMemory();
	int neededCpu = (applications.at(appId))->getReduceCpuVcores();
	neededMemory *= ONE_MB_IN_BYTES;
	bool cpuRelatedCheck =true;

	if (neededMemory > (maxAllocMB_*ONE_MB_IN_BYTES) || (isResourceCalculator() && (neededCpu > maxAllocVcores_))) {
		std::cerr << "ERROR: Maximum allocation allowance per container is exceeded in reducer scheduler || AppID: " << appId << std::endl;
		return redInf;
	}
	size_t allocatedMemory;
	int allocatedCpu = neededCpu;

	if (neededMemory % (minAllocMB_*ONE_MB_IN_BYTES)) {
		allocatedMemory = ((neededMemory / (minAllocMB_*ONE_MB_IN_BYTES)) + 1) * (minAllocMB_*ONE_MB_IN_BYTES);
	}
	else {
		allocatedMemory = neededMemory;
	}

	if(isResourceCalculator()){
		if (neededCpu % minAllocVcores_) {
			allocatedCpu = ((neededCpu / minAllocVcores_) + 1) * minAllocVcores_;
		}
		else {
			allocatedCpu = neededCpu;
		}
	}

	if(!wakeUpSignal){

		if (allocatedMemory > queueRemainingMemoryCapacities_.at((applications.at(appId))->getQueueId())
				|| (isResourceCalculator() && (allocatedCpu > queueRemainingCoreCapacities_.at((applications.at(appId))->getQueueId())))){
			#ifndef TERMINAL_LOG_DISABLED
			std::cerr << "SCHE||WARNING: Insufficient RESOURCES FOR REDUCER || AppId: " << appId << " allocatedMemory: " << allocatedMemory << " remaining capacity " << queueRemainingMemoryCapacities_.at((applications.at(appId))->getQueueId()) << std::endl;
			#endif

			// Put them all to a wait list (suspend)!
			struct waitingTaskInfo info;
			setWaitingTaskInfo(info, appId, -1, -1, -1, outEvent, true);
			waitingReducerQueue_[(applications.at(appId))->getQueueId()].push(info);

			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "SCHE||INFO: Reducer waiting list size: " << waitingReducerQueue_[(applications.at(appId))->getQueueId()].size() << std::endl;
			#endif
			return redInf;
		}

		int eventType;
		// search for a node that has available resources at least as much as allocatedMemory and allocatedCpu
		// no need to prefer a local node
		for (size_t i=0; i < workerNodeExpectedEventTypes_.size(); i++) {

			eventType = workerNodeExpectedEventTypes_.at(i);
			if(isResourceCalculator()){
				cpuRelatedCheck = nodeExpEventT_remainingCap[eventType].remainingCpuCapacity >= allocatedCpu;
			}
			if (cpuRelatedCheck && nodeExpEventT_nodeType[eventType] != RESOURCEMANAGER
					&& nodeExpEventT_remainingCap[eventType].remainingMemoryCapacity >= allocatedMemory) {

				int queueID = (applications.at(appId))->getQueueId();

				if(isResourceCalculator()){
					nodeExpEventT_remainingCap[eventType].remainingCpuCapacity -= allocatedCpu;
					queueRemainingCoreCapacities_.at(queueID) -= allocatedCpu;
				}
				// seize those resource (real CPU will cease it when the event is received by the corresponding worker node)
				nodeExpEventT_remainingCap[eventType].remainingMemoryCapacity -= allocatedMemory;

				// set seized resource per reducer for application
				(applications.at(appId))->setSeizedReduceCpuVcores(allocatedCpu);
				(applications.at(appId))->setSeizedMapreduceReduceMemory(allocatedMemory);

				// decrease the seized queue resources
				queueRemainingMemoryCapacities_.at(queueID) -= allocatedMemory;

				redInf.taskLocation = eventType;
				return redInf;
			}
		}
	}
	else{

		int eventType;
		// search for a node that has available resources at least as much as allocatedMemory and allocatedCpu
		// no need to prefer a local node
		for (size_t i=0; i < workerNodeExpectedEventTypes_.size(); i++) {

			eventType = workerNodeExpectedEventTypes_.at(i);

			if(isResourceCalculator()){
				cpuRelatedCheck = nodeExpEventT_remainingCap[eventType].remainingCpuCapacity >= allocatedCpu;
			}
			if (cpuRelatedCheck
					&& nodeExpEventT_nodeType[eventType] != RESOURCEMANAGER
					&& nodeExpEventT_remainingCap[eventType].remainingMemoryCapacity >= allocatedMemory) {

				int queueID = (applications.at(appId))->getQueueId();
				if(isResourceCalculator()){
					nodeExpEventT_remainingCap[eventType].remainingCpuCapacity -= allocatedCpu;
					queueRemainingCoreCapacities_.at(queueID) -= allocatedCpu;
				}
				// seize those resource (real CPU will cease it when the event is received by the corresponding worker node)
				nodeExpEventT_remainingCap[eventType].remainingMemoryCapacity -= allocatedMemory;

				// set seized resource per reducer for application
				(applications.at(appId))->setSeizedReduceCpuVcores(allocatedCpu);
				(applications.at(appId))->setSeizedMapreduceReduceMemory(allocatedMemory);

				// decrease the seized queue resources
				queueRemainingMemoryCapacities_.at(queueID) -= allocatedMemory;

				redInf.taskLocation = eventType;
				return redInf;
			}
		}
	}
	#ifndef TERMINAL_LOG_DISABLED
	// No node has enough resources for the reducer
	std::cerr << "SCHE||INFO: Not enough resources for the reduce task: " << appId << std::endl;
	#endif

	struct waitingTaskInfo info;
	setWaitingTaskInfo(info, appId, -1, -1, -1, outEvent, true);
	waitingReducerQueue_[(applications.at(appId))->getQueueId()].push(info);

	#ifndef TERMINAL_LOG_DISABLED
	std::cout << "SCHE||INFO: Reducer waiting list size: " << waitingReducerQueue_[(applications.at(appId))->getQueueId()].size() << std::endl;
	#endif
	return redInf;
}

void Scheduler::updateQueueRemainingCapacities(int expectedEventType, int remainingCpuCapacity, size_t remainingMemoryCapacity, bool isRM, size_t nodemanagerResManMB) {

	workerNodeExpectedEventTypes_.push_back(expectedEventType);

	if(!isRM){
		for (size_t i = 0; i < queueCapacities_.size(); i++){
			if(isResourceCalculator()){
				queueRemainingCoreCapacities_.at(i) += (remainingCpuCapacity*0.01*queueCapacities_.at(i));
			}
			if(remainingMemoryCapacity >= (nodemanagerResManMB*ONE_MB_IN_BYTES)){
				queueRemainingMemoryCapacities_.at(i) += (nodemanagerResManMB*ONE_MB_IN_BYTES*0.01*queueCapacities_.at(i));
			}
			else{
				std::cerr << "Inconsistent configuration for: yarn.nodemanager.resource.memory-mb or Insufficient physical memory for containers in nodemanager"<< std::endl;
			}
		}
	}
}

size_t Scheduler::getQueueSize() const {
	return queueCapacities_.size();
}

size_t Scheduler::getMinAllocMb() const {
	return minAllocMB_;
}
int Scheduler::getMinAllocVcores() const {
	return minAllocVcores_;
}

void Scheduler::schedulerReleaseAMresources(int appID, double outEventTime){
	if(isResourceCalculator()){
		nodeExpEventT_remainingCap[(applications.at(appID))->getAmEventType()].remainingCpuCapacity += (applications.at(appID))->getamCpuVcores();
		queueRemainingCoreCapacities_.at((applications.at(appID))->getQueueId()) += (applications.at(appID))->getamCpuVcores();
	}
	nodeExpEventT_remainingCap[(applications.at(appID))->getAmEventType()].remainingMemoryCapacity += ((applications.at(appID))->getAmResourceMb() * ONE_MB_IN_BYTES);

	// release seized queue resources
	queueRemainingMemoryCapacities_.at((applications.at(appID))->getQueueId()) += ((applications.at(appID))->getAmResourceMb() * ONE_MB_IN_BYTES);

	// now check if an application is pending..
	if(!(containerPendingAppQueue_[(applications.at(appID))->getQueueId()].empty())){
		struct amScheduleData suspendedAppID = containerPendingAppQueue_[(applications.at(appID))->getQueueId()].front();
		containerPendingAppQueue_[(applications.at(appID))->getQueueId()].pop();
		int nodemanagerEventType = schedulerSubmitNewApp(suspendedAppID.appId_, suspendedAppID.outputEventType_);

		if(nodemanagerEventType != -1){
			// create an event that has a scheduled AM
			(applications.at(suspendedAppID.appId_))->setAmEventType(nodemanagerEventType);
			// Event to start container (NODEMANAGER event behavior: START_AM_CONTAINER)
			Event newEvent(suspendedAppID.appId_, outEventTime, HEARTBEAT_SIZE, 0.0, suspendedAppID.outputEventType_, nodemanagerEventType, START_AM_CONTAINER, OTHER);
			eventsList.push(newEvent);
		}
	}
}

void Scheduler::incrementActiveTaskCount(int appID, bool isMapTask){
	if(isMapTask){
		activeMapTasks_[appID]++;
	}
	else{
		activeReduceTasks_[appID]++;
	}
}

void Scheduler::decrementActiveTaskCount(int appID, bool isMapTask){
	if(isMapTask){
		activeMapTasks_[appID]--;
	}
	else{
		activeReduceTasks_[appID]--;
	}
}

struct waitingTaskInfo Scheduler::schedulerReleaseReducerresources(int appID, int nodeEventType){

	if(isResourceCalculator()){
		nodeExpEventT_remainingCap[nodeEventType].remainingCpuCapacity += (applications.at(appID))->getSeizedReduceCpuVcores();
		queueRemainingCoreCapacities_.at((applications.at(appID))->getQueueId()) += (applications.at(appID))->getSeizedReduceCpuVcores();
	}
	nodeExpEventT_remainingCap[nodeEventType].remainingMemoryCapacity += (applications.at(appID))->getSeizedMapreduceReduceMemory();

	// release the seized queue resources
	queueRemainingMemoryCapacities_.at((applications.at(appID))->getQueueId()) += (applications.at(appID))->getSeizedMapreduceReduceMemory();

	// There are waiting reduce tasks AND
	// (all map tasks are completed for this application OR there are waiting map
	// tasks but currently the number of active map tasks are more than reduce tasks)
	if((waitingReducerQueue_[(applications.at(appID))->getQueueId()].size() > 0) &&
	((waitingMapperQueue_[(applications.at(appID))->getQueueId()].size() == 0)  ||  (activeMapTasks_[appID] > activeReduceTasks_[appID]))
	){
		// call schedulerGetReducerLocation() with wake up signal
		size_t appId = waitingReducerQueue_[(applications.at(appID))->getQueueId()].front().appId;
		int outEvent = waitingReducerQueue_[(applications.at(appID))->getQueueId()].front().outEvent;

		waitingReducerQueue_[(applications.at(appID))->getQueueId()].pop();
		return schedulerGetReducerLocation(appId, true, outEvent);
	}
	else if(waitingMapperQueue_[(applications.at(appID))->getQueueId()].size() > 0){
		// call schedulerGetMapperLocation() with wake up signal
		size_t appId = waitingMapperQueue_[(applications.at(appID))->getQueueId()].front().appId;
		int fileSplitLocationExpectedEventType = waitingMapperQueue_[(applications.at(appID))->getQueueId()].front().taskLocation;
		int attr = waitingMapperQueue_[(applications.at(appID))->getQueueId()].front().attr;
		int fsid = waitingMapperQueue_[(applications.at(appID))->getQueueId()].front().fsid;
		int outEvent = waitingMapperQueue_[(applications.at(appID))->getQueueId()].front().outEvent;
		waitingMapperQueue_[(applications.at(appID))->getQueueId()].pop();

		return schedulerGetMapperLocation(appId, fileSplitLocationExpectedEventType, true, outEvent, attr, fsid);
	}
	// no waiting mapper task exists
	struct waitingTaskInfo redInf;
	redInf.taskLocation = -2;
	redInf.taskType = true;

	return redInf;
}

bool Scheduler::isReleasedAlready(int appID, int fsID){
	if((applications.at(appID))->hasReleasedMapperResources(fsID)){
		return true;
	}
	(applications.at(appID))->addReleasedMapper(fsID);
	return false;
}

struct waitingTaskInfo Scheduler::schedulerReleaseMapperresources(int appID, int nodeEventType, int fsID){
	bool isReleased = isReleasedAlready(appID, fsID);

	if(!isReleased){
		if(isResourceCalculator()){
			nodeExpEventT_remainingCap[nodeEventType].remainingCpuCapacity += (applications.at(appID))->getSeizedMapCpuVcores();
			queueRemainingCoreCapacities_.at((applications.at(appID))->getQueueId()) += (applications.at(appID))->getSeizedMapCpuVcores();
		}
		nodeExpEventT_remainingCap[nodeEventType].remainingMemoryCapacity += (applications.at(appID))->getSeizedMapreduceMapMemory();

		// release the seized queue resources
		queueRemainingMemoryCapacities_.at((applications.at(appID))->getQueueId()) += (applications.at(appID))->getSeizedMapreduceMapMemory();

		// check if there are any waiting reducer tasks. If there are any, then let it wake up
		if((waitingReducerQueue_[(applications.at(appID))->getQueueId()].size() > 0) &&
		((waitingMapperQueue_[(applications.at(appID))->getQueueId()].size() == 0)  ||  (activeMapTasks_[appID] > activeReduceTasks_[appID]))
		){
			size_t appId = waitingReducerQueue_[(applications.at(appID))->getQueueId()].front().appId;
			int outEvent = waitingReducerQueue_[(applications.at(appID))->getQueueId()].front().outEvent;
			waitingReducerQueue_[(applications.at(appID))->getQueueId()].pop();

			return schedulerGetReducerLocation(appId, true, outEvent);
		}
		else if(waitingMapperQueue_[(applications.at(appID))->getQueueId()].size() > 0){
			// call schedulerGetMapperLocation() with wake up signal

			size_t appId = waitingMapperQueue_[(applications.at(appID))->getQueueId()].front().appId;
			int fileSplitLocationExpectedEventType = waitingMapperQueue_[(applications.at(appID))->getQueueId()].front().taskLocation;
			int attr = waitingMapperQueue_[(applications.at(appID))->getQueueId()].front().attr;
			int fsid = waitingMapperQueue_[(applications.at(appID))->getQueueId()].front().fsid;
			int outEvent = waitingMapperQueue_[(applications.at(appID))->getQueueId()].front().outEvent;
			waitingMapperQueue_[(applications.at(appID))->getQueueId()].pop();

			return schedulerGetMapperLocation(appId, fileSplitLocationExpectedEventType, true, outEvent, attr, fsid);
		}
	}
	// no waiting mapper task exists
	struct waitingTaskInfo mapInf;
	mapInf.taskLocation = -2;
	mapInf.taskType = false;
	return mapInf;
}

bool Scheduler::isResourceCalculator() const {
	return resourceCalculator_;
}

size_t Scheduler::getMaxNumberOfAppsAtQueue(size_t queueIndex){
	// required resources for AM
	size_t amResourcesMB = (applications.at(0))->getAmResourceMb();
	amResourcesMB *= ONE_MB_IN_BYTES;
	return ((maxAmResourcePercent_ * queueRemainingMemoryCapacities_.at(queueIndex))/amResourcesMB) + 1;
}
