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

#ifndef CPU_H_
#define CPU_H_

#include <memory>
#include <map>
#include "EventTimeCompare.h"

#define NON_SPILL -1
#define SPILL 0
#define EXP_CPU_DELAY_CONSTANT 3	// the delay constant: increase to lower the exponential delay (if selected delay type is exponential)

extern std::priority_queue<Event, std::vector<Event>, EventTimeCompare > eventsList;

struct mapRecordState{
	size_t applicationId_;
	double neededResourceQuantity_;
	size_t entityId_;
	int attribute_;
	int fsLoc_;
	int fsId_;
	int redID_;
	int recordID_;
};

struct mapState{
	size_t appID_;
	int fsID_;
	double startTime_;
	double finishTime_;
	size_t remainingData_;	// still needs to be processed in map function
	size_t toBeSentToSort_;	// processed in map function but waiting for spilling of previous part of filesplit to continue building a full spill for sort function
	int state_;
	Event suspendedEvent_;
	bool isSuspended_;
};

class Cpu {
public:
	Cpu(int, size_t, enum DistributionType delayType=EXPONENTIAL, enum TimeType unit=SECONDS, double delayratio=0.01);

	virtual ~Cpu();

	void work (int, size_t, int, Event*, size_t, size_t, double);

	void sortWork (Event*, double, bool hasCombiner=false); // returns completion time of sort (which is the starting time for spill)

	void combinerWork (size_t resourceQuantity, int seized_mapCpuVcores, Event* ev, double taskIntensity);

	void mergeWork (Event*, double);

	int getRemainingNumberOfCores() const;

	void setRemainingNumberOfCores(int remainingNumberOfCores);

	void remainingMapWork(size_t resourceQuantity, int seized_mapCpuVcores, Event* ev, size_t mapreduceTaskIO, size_t spillSize, double taskIntensity);

	void setMapFinish(size_t appID, int fsID, double finishTime);

	void reduceMergeWork (size_t resourceQuantity, int seized_mapCpuVcores, Event* ev, double taskIntensity);

	void reduceSort(int seized_mapCpuVcores, Event* ev, double taskIntensity);

	void reduceFunctionWork (size_t resourceQuantity, int seized_mapCpuVcores, Event* ev, double taskIntensity);

	void mapFunction(Event* ev, size_t spillLimitInBytes, size_t suspendLimitInBytes, int cpuVcores, double mapIntensity, int totalRecCount);

	bool mapSpillCompleted(Event*ev);

	void nonSpillComplete(int fsId, size_t completedSize, Event *ev);

	int incCpuUserCount(int fsId, int appID);

	void decCpuUserCount(int fsId, int appID);

	int increduceCpuUserCount(int redId, int appID);

	void decreduceCpuUserCount(int redId, int appID);

	void setEffectiveMapCoreCapacity(double capacityLimitor);

private:
	int remainingNumberOfCores_;
	const size_t capacityPerCore_;	// (speed) per core!! (bytes/sec)
	enum DistributionType delayType_;
	enum TimeType unit_;
	double delayratio_;
	size_t spillLimitInBytes_, suspendLimitInBytes_;
	int cpuVcores_;
	double effectiveMapCoreCapacity_;
	std::vector<mapState> mapState_;
	std::map<std::pair<int,int>, int> fsId_state_;		// Either [SUSPENDED | SPILL_INPROGRESS | NO_SPILL_INPROGRESS]
	std::map<std::pair<int,int>, size_t>  fsId_usedBufferBytes, fsId_processedBufferBytes;	// Initially all buffer is empty for each map task
	std::map<std::pair<int,int>, bool> isActiveSpill;										// Initially no active spill
	std::map<std::pair<int,int>, std::queue<mapRecordState>> waitingMapRecords_;
	std::map<std::pair<int,int>, int> fsId_processedRecordCount, fsId_totalRecordCount;
	std::map<std::pair<int,int>, int> fsId_cpuUserCount_, redId_cpuUserCount_;	// CPU user thread count in map phase (per map task)
	std::map<int, double> mapIntensity_;

	int opEventMap(int op);
	size_t gettoBeSentToSort_(size_t appID, int fsID) const;
	int getstate_(size_t appID, int fsID) const;
	void setstate_(size_t appID, int fsID, int state);
	double getProcessingSpeed(int cpuVcores, int fsId, double mapIntensity, int appID);
	double getreduceProcessingSpeed(int cpuVcores, int redId, double taskIntensity, int appID);
	int  getMapFunctionState(int fsId, int appID, int totalRecCount, size_t spillLimitInBytes, size_t suspendLimitInBytes, int cpuVcores, double mapIntensity);
	bool isLastRecord(int fsId, int appID);
	double delay (size_t, Event, double, int, int, size_t remainingMapIntermediateOutputSize, size_t mapreduceTaskIO=0, double taskIntensity=1.0);
	void delayHelper(double newEventTime, size_t resourceQuantity, Event ev, int op, int seized_mapCpuVcores,
	size_t remainingMapIntermediateOutputSize, size_t mapreduceTaskIO, double taskIntensity);
	void resetToBeSentToSort(size_t appID, int fsID);
	void setsuspendedEvent_(size_t appID, int fsID, Event ev);
	void addToBeSentToSort(size_t appID, int fsID, size_t toBeSentToSort);
};

#endif /* CPU_H_ */
