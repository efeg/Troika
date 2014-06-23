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

// Memory and CPU are the the resource requirements supported
// in the original Capacity Scheduler

#include <stdlib.h>
#include <vector>
#include "Application.h"

// DEFAULT YARN CONFIGS
#define YARN_SCHEDULER_MIN_ALLOCATION_MB 1024								// default min. allocation for every container request at the RM, in MBs. Memory requests lower than this won't take effect, and the specified value will get allocated at minimum.
#define YARN_SCHEDULER_MAX_ALLOCATION_MB 8192 								// default max. allocation for every container request at the RM, in MBs. Memory requests higher than this won't take effect, and will get capped to this value
#define YARN_SCHEDULER_MIN_ALLOCATION_VCORES 1								// default min. allocation for every container request at the RM, in terms of virtual CPU cores. Requests lower than this won't take effect, and the specified value will get allocated the minimum.
#define YARN_SCHEDULER_MAX_ALLOCATION_VCORES 32 							// default max. allocation for every container request at the RM, in terms of virtual CPU cores. Requests higher than this won't take effect, and will get capped to this value.
#define YARN_SCHEDULER_CAPACITY_MAX_AM_RESOURCE_PERCENT 0.1				// default max. percent of resources in the cluster which can be used to run application masters (for each capacity queue)

#define DEFAULT_RESOURCE_CALCULATOR false									// yarn.scheduler.capacity.resource-calculator (false: only memory true: memory and cpu)

#ifndef SCHEDULER_H_
#define SCHEDULER_H_

struct waitingTaskInfo{
	size_t appId;
	int outEvent;
	int taskLocation;
	int attr;
	int fsid;
	bool taskType;
};

struct remainingCapacities{
	int remainingCpuCapacity;
	size_t remainingMemoryCapacity;
};

extern std::map<int, remainingCapacities> nodeExpEventT_remainingCap;	// up-to-date remaining capacities
extern std::map<int, int> nodeExpEventT_switchExpEventT;				// a mapping for expected event types of node and rack to be used for network simulation
extern std::vector<std::shared_ptr<Application>> applications;
extern std::map<int, enum NodeType> nodeExpEventT_nodeType;		// used to check if is it a RM node

class Scheduler{
public:
	// by default there is a single queue
	Scheduler(std::vector<double> queueCapacities={100}, double maxAmResourcePercent=YARN_SCHEDULER_CAPACITY_MAX_AM_RESOURCE_PERCENT, size_t minAllocMB=YARN_SCHEDULER_MIN_ALLOCATION_MB,
			size_t maxAllocMB=YARN_SCHEDULER_MAX_ALLOCATION_MB, int minAllocVcores=YARN_SCHEDULER_MIN_ALLOCATION_VCORES, int maxAllocVcores=YARN_SCHEDULER_MAX_ALLOCATION_VCORES,
			bool resourceCalculator=DEFAULT_RESOURCE_CALCULATOR);
	virtual ~Scheduler();

	double getMaxAmResourcePercent() const;

	void setMaxAllocMb(size_t maxAllocMb);

	void setMaxAllocVcores(int maxAllocVcores);

	void setMaxAmResourcePercent(double maxAmResourcePercent);

	void setMinAllocMb(size_t minAllocMb);

	void setMinAllocVcores(int minAllocVcores);

	void setQueueCapacities(const std::vector<double>& queueCapacities);

	double getQueueCapacity(size_t queueIndex) const;

	int schedulerSubmitNewApp(size_t appId);

	struct waitingTaskInfo schedulerGetMapperLocation(size_t appId, int fileSplitLocationExpectedEventType, bool wakeUpSignal, int outEvent=-1, int attr=-1, int fsid=-1);

	struct waitingTaskInfo schedulerGetReducerLocation(size_t appId, bool wakeUpSignal, int outEvent);

	void updateQueueRemainingCapacities(int expectedEventType, int remainingCpuCapacity, size_t remainingMemoryCapacity, bool isRM, size_t nodemanagerResManMB);

	void schedulerReleaseAMresources(int appID);

	struct waitingTaskInfo schedulerReleaseReducerresources(int appID, int nodeEventType);

	struct waitingTaskInfo schedulerReleaseMapperresources(int appID, int nodeEventType, int fsID);

	size_t getQueueSize() const;

	size_t getMinAllocMb() const;

	int getMinAllocVcores() const;

	void setResourceCalculator(bool resourceCalculator);

	bool isResourceCalculator() const;

	int getnodeId_maxNumberOfMapTaskPerNode(int nodeID);

private:
	std::vector<double> queueCapacities_;			// queueId from Application will match with the order the elements are pushed to vector
	double maxAmResourcePercent_;
	size_t minAllocMB_, maxAllocMB_;
	int minAllocVcores_, maxAllocVcores_;
	bool resourceCalculator_, maxAlreadySet_;
	bool isReleasedAlready(int appID, int fsID);
	// queueId from Application will match with the order the elements are pushed to vector
	std::vector<double> queueRemainingCoreCapacities_, queueRemainingMemoryCapacities_;
	std::vector<int> workerNodeExpectedEventTypes_;			// used to determine which nodes are workers
	std::vector<size_t> remainingMemResource_AM_, remainingMemResource_Container_, remainingCpuResource_AM_, remainingCpuResource_Container_;
	std::queue<waitingTaskInfo> waitingMapperQueue_, waitingReducerQueue_;
	std::map<int, int> nodeId_maxNumberOfMapTaskPerNode;
	void incnodeId_maxNumberOfMapTaskPerNode(int fsloc);
};

#endif /* SCHEDULER_H_ */
