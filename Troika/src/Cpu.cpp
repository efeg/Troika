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

#include "Cpu.h"
#include <random>
#include <chrono>
#include <iostream>

Cpu::Cpu(int numberOfCores, size_t capacity, enum DistributionType delayType, enum TimeType unit,
		  double delayratio):
		  remainingNumberOfCores_(numberOfCores),
		  capacityPerCore_(capacity),
		  delayType_(delayType),
		  unit_(unit),
		  delayratio_(delayratio),
		  spillLimitInBytes_(0),
		  suspendLimitInBytes_(0),
		  cpuVcores_(0),
		  mapIntensity_(0),
		  effectiveMapCoreCapacity_(0){
}

Cpu::~Cpu() {
}

// Return map function state [SUSPENDED | SPILL_INPROGRESS | NO_SPILL_INPROGRESS]
// Initialize map function related data for fsId
int Cpu::getMapFunctionState(int fsId, int totalRecCount, size_t spillLimitInBytes, size_t suspendLimitInBytes, int cpuVcores, double mapIntensity){

	if ( fsId_state_.find(fsId) == fsId_state_.end() ) {	// Not found...
		fsId_state_[fsId] = NO_SPILL_INPROGRESS;			// Initially no spill in progress
		fsId_usedBufferBytes[fsId] = 0;						// Initially all buffer is empty (contains processed and processing bytes)
		fsId_processedBufferBytes[fsId] =0;
		isActiveSpill[fsId] = false;						// Keep track of active spills in suspended state
		fsId_processedRecordCount[fsId] = 0;
		fsId_totalRecordCount[fsId] = totalRecCount;
		spillLimitInBytes_ = spillLimitInBytes;
		suspendLimitInBytes_ = suspendLimitInBytes;
		cpuVcores_ = cpuVcores;
		mapIntensity_ = mapIntensity;
	}
	return fsId_state_[fsId];
}

// Returns true if completed spill was the last one...
bool Cpu::mapSpillCompleted(int fsId, size_t completedSize, Event*ev){
	// At this point state is either SPILL_INPROGRESS or SUSPENDED
	// isActiveSpill[fsId] must be true

	// release seized local resources (fsId_usedBufferBytes[fsId] and fsId_processedBufferBytes[fsId])
	fsId_usedBufferBytes[fsId] -= completedSize;
	fsId_processedBufferBytes[fsId] -= completedSize;

	// active spill completed
	isActiveSpill[fsId] = false;

	if(fsId_state_[fsId] == SUSPENDED){

		if ( fsId_usedBufferBytes[fsId] > suspendLimitInBytes_ ){
			// state will still be SUSPENDED and processed part will be spilled

			if(fsId_processedBufferBytes[fsId] > 0 ){
				// spill
				isActiveSpill[fsId] = true;

				// In order to differentiate between spills and processing a non-spill, set redId to -1 (NON_SPILL) for non-spills and 0 (SPILL) for spills
				ev->setRedId(SPILL);

				// spill processed bytes (when spill complete, update processed and processing bytes)
				// note that before spill there is a sort phase!
				// generate sort event  (Note: seized_mapCpuVcores stored as attribute)  - spills will create a map sort event (Event: RUN_MAP_SORT)
				// note that there is no need to keep track of record id (not meaningful) for spilled data
				Event newEvent(ev->getApplicationId(), ev->getEventTime(), fsId_processedBufferBytes[ev->getFsId()], ev->getSeizedResQuantity(), ev->getDestinationEventType(), ev->getDestinationEventType(),
						RUN_MAP_SORT, MAPTASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());
				eventsList.push(newEvent);
			}
		}
		else if(fsId_usedBufferBytes[fsId] > spillLimitInBytes_){
			if(fsId_processedBufferBytes[fsId] > 0 ){
				// spill
				isActiveSpill[fsId] = true;

				// In order to differentiate between spills and processing a non-spill, set redId to -1 (NON_SPILL) for non-spills and 0 (SPILL) for spills
				ev->setRedId(SPILL);

				// spill processed bytes (when spill complete, update processed and processing bytes)
				// note that before spill there is a sort phase!
				// generate sort event  (Note: seized_mapCpuVcores stored as attribute)  - spills will create a map sort event (Event RUN_MAP_SORT)
				// note that there is no need to keep track of record id (not meaningful) for spilled data
				Event newEvent(ev->getApplicationId(), ev->getEventTime(), fsId_processedBufferBytes[ev->getFsId()], ev->getSeizedResQuantity(), ev->getDestinationEventType(), ev->getDestinationEventType(),
						RUN_MAP_SORT, MAPTASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());
				eventsList.push(newEvent);
			}
			// update state
			fsId_state_[fsId] = SPILL_INPROGRESS;

			// processing speed
			double processingSpeed;

			while(!(waitingMapRecords_[ev->getFsId()].empty())){	// get a record from queue (if exists)
				processingSpeed = getProcessingSpeed(cpuVcores_, fsId, mapIntensity_);

				Event newEvent(waitingMapRecords_[ev->getFsId()].front().applicationId_, ev->getEventTime(), waitingMapRecords_[ev->getFsId()].front().neededResourceQuantity_,
						0, ev->getDestinationEventType(), ev->getDestinationEventType(), RUN_MAP_SORT, MAPTASK, SEIZETOMASTER, waitingMapRecords_[ev->getFsId()].front().attribute_,
						waitingMapRecords_[ev->getFsId()].front().fsLoc_, waitingMapRecords_[ev->getFsId()].front().fsId_);


				fsId_usedBufferBytes[ev->getFsId()] += waitingMapRecords_[ev->getFsId()].front().neededResourceQuantity_;

				// run map function delay
				// In order to differentiate between spills and processing a non-spill, set redId to -1 (NON_SPILL) for non-spills and 0 (SPILL) for spills
				newEvent.setRedId(NON_SPILL);

				delay (newEvent.getNeededResQuantity(), newEvent, processingSpeed, CPU_MAP_DELAY_OP, cpuVcores_, (size_t)processingSpeed, 0, mapIntensity_);

				waitingMapRecords_[ev->getFsId()].pop();

				if(fsId_usedBufferBytes[ev->getFsId()] > suspendLimitInBytes_){
					// update state
					fsId_state_[fsId] = SUSPENDED;
					break;
				}
			}
		}
		else{	// remaining is less than spill limit
			// update state
			fsId_state_[fsId] = NO_SPILL_INPROGRESS;

			// processing speed
			double processingSpeed;
			double newStartTime = 0;
			while(!(waitingMapRecords_[ev->getFsId()].empty())){	// get a record from queue (if exists)
				processingSpeed = getProcessingSpeed(cpuVcores_, fsId, mapIntensity_);

				Event newEvent(waitingMapRecords_[ev->getFsId()].front().applicationId_, ev->getEventTime(), waitingMapRecords_[ev->getFsId()].front().neededResourceQuantity_,
						0, ev->getDestinationEventType(), ev->getDestinationEventType(), RUN_MAP_SORT, MAPTASK, SEIZETOMASTER, waitingMapRecords_[ev->getFsId()].front().attribute_,
						waitingMapRecords_[ev->getFsId()].front().fsLoc_, waitingMapRecords_[ev->getFsId()].front().fsId_);

				if(newStartTime){
					newEvent.setEventTime(newStartTime);
				}

				fsId_usedBufferBytes[ev->getFsId()] += waitingMapRecords_[ev->getFsId()].front().neededResourceQuantity_;
				// run map function delay
				// In order to differentiate between spills and processing a non-spill, set redId to -1 (NON_SPILL) for non-spills and 0 (SPILL) for spills
				newEvent.setRedId(NON_SPILL);
				newStartTime = delay (newEvent.getNeededResQuantity(), newEvent, processingSpeed, CPU_MAP_DELAY_OP, cpuVcores_, (size_t)processingSpeed, 0, mapIntensity_);
				waitingMapRecords_[ev->getFsId()].pop();

				if(fsId_usedBufferBytes[ev->getFsId()] > suspendLimitInBytes_){
					// update state
					fsId_state_[fsId] = SUSPENDED;
					break;
				}
				else if(fsId_usedBufferBytes[ev->getFsId()] > spillLimitInBytes_){
					// update state
					fsId_state_[fsId] = SPILL_INPROGRESS;
				}
			}

			// the last spill
			if((fsId_state_[fsId] == NO_SPILL_INPROGRESS) && isLastRecord(fsId) ){
				// spill will be initiated...
				isActiveSpill[fsId] = true;

				// In order to differentiate between spills and processing a non-spill, set redId to -1 (NON_SPILL) for non-spills and 0 (SPILL) for spills
				ev->setRedId(SPILL);

				// spill processed bytes (when spill complete, update processed and processing bytes)
				// note that before spill there is a sort phase!
				// generate sort event  (Note: seized_mapCpuVcores stored as attribute) - spills will create a map sort event (Event RUN_MAP_SORT)
				// note that there is no need to keep track of record id (not meaningful) for spilled data
				Event newEvent(ev->getApplicationId(), ev->getEventTime(), fsId_processedBufferBytes[ev->getFsId()], ev->getSeizedResQuantity(), ev->getDestinationEventType(), ev->getDestinationEventType(),
						RUN_MAP_SORT, MAPTASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());
				eventsList.push(newEvent);
			}
		}
	}
	else if(fsId_state_[fsId] == SPILL_INPROGRESS){
		// update state
		fsId_state_[fsId] = NO_SPILL_INPROGRESS;

		// the last spill
		if(isLastRecord(fsId)){
			// spill will be initiated...
			isActiveSpill[fsId] = true;

			// In order to differentiate between spills and processing a non-spill, set redId to -1 (NON_SPILL) for non-spills and 0 (SPILL) for spills
			ev->setRedId(SPILL);

			// spill processed bytes (when spill complete, update processed and processing bytes)
			// note that before spill there is a sort phase!
			// generate sort event  (Note: seized_mapCpuVcores stored as attribute) - spills will create a map sort event (Event RUN_MAP_SORT)
			// note that there is no need to keep track of record id (not meaningful) for spilled data
			Event newEvent(ev->getApplicationId(), ev->getEventTime(), fsId_processedBufferBytes[ev->getFsId()], ev->getSeizedResQuantity(), ev->getDestinationEventType(), ev->getDestinationEventType(),
					RUN_MAP_SORT, MAPTASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());
			eventsList.push(newEvent);
		}
	}
	else{
		if(!isLastRecord(fsId)){
			std::cerr<< "Error: CPU state cannot be NO_SPILL_INPROGRESS while there is spill in progress! @fsid " << ev->getFsId() << " time " << ev->getEventTime() <<std::endl;
		}
	}
	if(isLastRecord(fsId) && waitingMapRecords_[fsId].empty() && !isActiveSpill[fsId]){	// the last spill is completed
		return true;
	}
	// Not the last!
	return false;
}

bool Cpu::isLastRecord(int fsId){
	if(fsId_processedRecordCount[fsId] == fsId_totalRecordCount[fsId]){
		return true;
	}
	return false;
}

void Cpu::nonSpillComplete(int fsId, size_t completedSize, Event *ev){
	// to signal completion of processing map function increase processed buffer bytes
	fsId_processedBufferBytes[fsId] += completedSize;
	// increase processed record count
	fsId_processedRecordCount[fsId]++;
	// decrease active cpu user count for current map task
	decCpuUserCount(fsId);

	if(!isActiveSpill[fsId] &&
			(fsId_state_[fsId] == SUSPENDED || fsId_state_[fsId] == SPILL_INPROGRESS ||  ((fsId_state_[fsId] == NO_SPILL_INPROGRESS) && (isLastRecord(fsId))))){

		// spill will be initiated...
		isActiveSpill[fsId] = true;

		// In order to differentiate between spills and processing a non-spill, set redId to -1 (NON_SPILL) for non-spills and 0 (SPILL) for spills
		ev->setRedId(SPILL);

		// spill processed bytes (when spill complete, update processed and processing bytes)
		// note that before spill there is a sort phase!
		// generate sort event  (Note: seized_mapCpuVcores stored as attribute)  - spills will create a map sort event (Event RUN_MAP_SORT)
		// note that there is no need to keep track of record id (not meaningful) for spilled data
		Event newEvent(ev->getApplicationId(), ev->getEventTime(), fsId_processedBufferBytes[ev->getFsId()], ev->getSeizedResQuantity(), ev->getDestinationEventType(), ev->getDestinationEventType(),
				RUN_MAP_SORT, MAPTASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());
		eventsList.push(newEvent);
	}
	// if there is an active spill, then it cannot initiate another spill...
}

int Cpu::incCpuUserCount(int fsId){
	if(fsId_cpuUserCount_.find(fsId) == fsId_cpuUserCount_.end()){
		fsId_cpuUserCount_[fsId] = 1;
	}
	else{
		fsId_cpuUserCount_[fsId]++;
	}
	return fsId_cpuUserCount_[fsId];
}

void Cpu::decCpuUserCount(int fsId){
	fsId_cpuUserCount_[fsId]--;
}

int Cpu::getCpuUserCount(int fsId){
	return fsId_cpuUserCount_[fsId];
}

int Cpu::increduceCpuUserCount(int redId){
	if(redId_cpuUserCount_.find(redId) == redId_cpuUserCount_.end()){
		redId_cpuUserCount_[redId] = 1;
	}
	else{
		redId_cpuUserCount_[redId]++;
	}
	return redId_cpuUserCount_[redId];
}

void Cpu::decreduceCpuUserCount(int redId){
	redId_cpuUserCount_[redId]--;
}

int Cpu::getreduceCpuUserCount(int redId){
	return redId_cpuUserCount_[redId];
}

void Cpu::setEffectiveMapCoreCapacity(double capacityLimitor){
	effectiveMapCoreCapacity_ = capacityPerCore_*capacityLimitor;
}

double Cpu::getEffectiveMapCoreCapacity(){
	return effectiveMapCoreCapacity_;
}

double Cpu::getProcessingSpeed(int cpuVcores, int fsId, double taskIntensity){
	double processingSpeed;
	incCpuUserCount(fsId);

	if(fsId_cpuUserCount_[fsId] > 100){
		processingSpeed = (effectiveMapCoreCapacity_)/(100 * taskIntensity);
	}
	else{
		processingSpeed = (effectiveMapCoreCapacity_)/(fsId_cpuUserCount_[fsId] * taskIntensity);
	}
	return processingSpeed;
}

double Cpu::getreduceProcessingSpeed(int cpuVcores, int redId, double taskIntensity){
	size_t processingSpeed;

	processingSpeed = (cpuVcores * capacityPerCore_)/(increduceCpuUserCount(redId) * taskIntensity);
	return processingSpeed;
}

// spills will create a map sort event (Event RUN_MAP_SORT)
void Cpu::mapFunction(Event* ev, size_t spillLimitInBytes, size_t suspendLimitInBytes, int cpuVcores, double mapIntensity, int totalRecCount){

	int state = getMapFunctionState(ev->getFsId(), totalRecCount, spillLimitInBytes, suspendLimitInBytes, cpuVcores, mapIntensity);

	if(state == SUSPENDED){	// wait in waiting record queue until a spill notifies the waiting record to wake up
		struct mapRecordState waitingRecord;
		waitingRecord.applicationId_ = ev->getApplicationId();
		waitingRecord.neededResourceQuantity_ = ev->getNeededResQuantity();
		waitingRecord.entityId_ = ev->getEntityIns().getEntityId();
		waitingRecord.attribute_ = ev->getEntityIns().getAttribute();
		waitingRecord.fsLoc_ = ev->getFsLoc();
		waitingRecord.fsId_ = ev->getFsId();
		waitingRecord.redID_ = ev->getRedId();
		waitingRecord.recordID_ = ev->getRecordId();

		// push to waiting queue
		waitingMapRecords_[ev->getFsId()].push(waitingRecord);
	}
	else{	// state is either SPILL_INPROGRESS or NO_SPILL_INPROGRESS
		// to be processed
		fsId_usedBufferBytes[ev->getFsId()] += ev->getNeededResQuantity();

		// processing speed
		double processingSpeed = getProcessingSpeed(cpuVcores, ev->getFsId(), mapIntensity);

		if(fsId_usedBufferBytes[ev->getFsId()] > suspendLimitInBytes){
			if(state == SPILL_INPROGRESS){
				// set state to SUSPENDED
				fsId_state_[ev->getFsId()] = SUSPENDED;

				// upon completion of spill, if needed another spill will be initiated
				// run map function delay

				// In order to differentiate between spills and processing a non-spill, set redId to -1 (NON_SPILL) for non-spills and 0 (SPILL) for spills
				ev->setRedId(NON_SPILL);

				delay (ev->getNeededResQuantity(), *ev, processingSpeed, CPU_MAP_DELAY_OP, cpuVcores, (size_t)processingSpeed, 0, mapIntensity);
			}
			else{	// state was NO_SPILL_INPROGRESS
				// set state to SUSPENDED
				fsId_state_[ev->getFsId()] = SUSPENDED;

				// if there are processed bytes, start spilling them
				// if there are "processing" bytes but not yet "processed", once they process bytes they will check the state and start spilling
				if(fsId_processedBufferBytes[ev->getFsId()] > 0 ){	// there are processed bytes

					// a spill is initiating!
					isActiveSpill[ ev->getFsId()] = true;

					// In order to differentiate between spills and processing a non-spill, set redId to -1 (NON_SPILL) for non-spills and 0 (SPILL) for spills
					ev->setRedId(SPILL);

					// spill processed bytes (when spill complete, update processed and processing bytes)
					// note that before spill there is a sort phase!
					// generate sort event  (Note: seized_mapCpuVcores stored as attribute)  - spills will create a map sort event (Event RUN_MAP_SORT)
					// note that there is no need to keep track of record id (not meaningful) for spilled data
					Event newEvent(ev->getApplicationId(), ev->getEventTime(), fsId_processedBufferBytes[ev->getFsId()], ev->getSeizedResQuantity(), ev->getDestinationEventType(), ev->getDestinationEventType(),
							RUN_MAP_SORT, MAPTASK, SEIZETOMASTER, cpuVcores, ev->getFsLoc(), ev->getFsId(), ev->getRedId());
					eventsList.push(newEvent);
				}
				// run map function delay
				// In order to differentiate between spills and processing a non-spill, set redId to -1 (NON_SPILL) for non-spills and 0 (SPILL) for spills
				ev->setRedId(NON_SPILL);
				delay (ev->getNeededResQuantity(), *ev, processingSpeed, CPU_MAP_DELAY_OP, cpuVcores, (size_t)processingSpeed, 0, mapIntensity);
			}
		}

		else if(fsId_usedBufferBytes[ev->getFsId()] > spillLimitInBytes){
			if(state == SPILL_INPROGRESS){
				// no need to change state
				// start processing the record
				// In order to differentiate between spills and processing a non-spill, set redId to -1 (NON_SPILL) for non-spills and 0 (SPILL) for spills
				ev->setRedId(NON_SPILL);

				delay (ev->getNeededResQuantity(), *ev, processingSpeed, CPU_MAP_DELAY_OP, cpuVcores, (size_t)processingSpeed, 0, mapIntensity);
			}
			else{	// state was NO_SPILL_INPROGRESS
				// set state to SPILL_INPROGRESS
				fsId_state_[ev->getFsId()] = SPILL_INPROGRESS;

				// if there are processed bytes, start spilling them
				// if there are "processing" bytes but not yet "processed", once they process bytes they will check the state and start spilling
				if(fsId_processedBufferBytes[ev->getFsId()] > 0 ){	// there are processed bytes

					// a spill is initiating!
					isActiveSpill[ ev->getFsId()] = true;

					// In order to differentiate between spills and processing a non-spill, set redId to -1 (NON_SPILL) for non-spills and 0 (SPILL) for spills
					ev->setRedId(SPILL);

					// spill processed bytes (when spill complete, update processed and processing bytes)
					// note that before spill there is a sort phase!
					// generate sort event  (Note: seized_mapCpuVcores stored as attribute)  - spills will create a map sort event (Event RUN_MAP_SORT)
					// note that there is no need to keep track of record id (not meaningful) for spilled data
					Event newEvent(ev->getApplicationId(), ev->getEventTime(), fsId_processedBufferBytes[ev->getFsId()], ev->getSeizedResQuantity(), ev->getDestinationEventType(), ev->getDestinationEventType(),
							RUN_MAP_SORT, MAPTASK, SEIZETOMASTER, cpuVcores, ev->getFsLoc(), ev->getFsId(), ev->getRedId());
					eventsList.push(newEvent);
				}
				else{	// no processed bytes!

					// when there will be processed bytes, isActiveSpill[ ev->getFsId()] will be
					// checked and since there is no active spills it will start spilling
					// nothing else is needed to be done in here...
				}
				// run map function delay
				// In order to differentiate between spills and processing a non-spill, set redId to -1 (NON_SPILL) for non-spills and 0 (SPILL) for spills
				ev->setRedId(NON_SPILL);

				delay (ev->getNeededResQuantity(), *ev, processingSpeed, CPU_MAP_DELAY_OP, cpuVcores, (size_t)processingSpeed, 0, mapIntensity);
			}
		}
		else{	// no state change needed... NO_SPILL_INPROGRESS will remain true

			// run map function delay
			// In order to differentiate between spills and processing a non-spill, set redId to -1 (NON_SPILL) for non-spills and 0 (SPILL) for spills
			ev->setRedId(NON_SPILL);

			delay (ev->getNeededResQuantity(), *ev, processingSpeed, CPU_MAP_DELAY_OP, cpuVcores, (size_t)processingSpeed, 0, mapIntensity);
		}
	}
}

bool Cpu::getisSuspended_(size_t appID, int fsID) const{
	for(unsigned int i=0; i < mapState_.size();i++){
    	if ( (mapState_.at(i)).appID_== appID && (mapState_.at(i).fsID_ == fsID)){
    		return (mapState_.at(i)).isSuspended_;
    	}
    }
    return false;
}

Event Cpu::updateTime_getsuspendedEvent_(size_t appID, int fsID, double eventTime){
	for(unsigned int i=0; i < mapState_.size();i++){
    	if ( (mapState_.at(i)).appID_== appID && (mapState_.at(i).fsID_ == fsID)){
    		(mapState_.at(i)).suspendedEvent_.setEventTime(eventTime);
    		return (mapState_.at(i)).suspendedEvent_;
    	}
    }
	// should already be able to find the suspended event before coming here
	return (mapState_.at(0)).suspendedEvent_;
}

void Cpu::setsuspendedEvent_(size_t appID, int fsID, Event ev){
	for(unsigned int i=0; i < mapState_.size();i++){
    	if ( (mapState_.at(i)).appID_== appID && (mapState_.at(i).fsID_ == fsID)){
    		(mapState_.at(i)).suspendedEvent_ = ev;
    		(mapState_.at(i)).isSuspended_ = true;
    		break;
    	}
    }
}

// unless it is the last mapper, created spill sizes will be the same.
void Cpu::remainingMapWork(size_t resourceQuantity, int seized_mapCpuVcores, Event* ev, size_t mapreduceTaskIO, size_t spillSize, double taskIntensity){
	#ifndef TERMINAL_LOG_DISABLED
	std::cout << "resourceQuantity " << resourceQuantity<<  " CpuVcores " << seized_mapCpuVcores <<
		" mapreduceTaskIO " << mapreduceTaskIO << " FsId " << ev->getFsId() << " State " << getstate_(ev->getApplicationId(), ev->getFsId()) <<
		" toBeSentToSort " << gettoBeSentToSort_(ev->getApplicationId(), ev->getFsId()) << " spillSize " << spillSize<<
		" tot: " << spillSize +  gettoBeSentToSort_(ev->getApplicationId(), ev->getFsId())<< std::endl;
	#endif

	// if State is in SPILL_INPROGRESS,
	if(getstate_(ev->getApplicationId(), ev->getFsId())== SPILL_INPROGRESS){
		// wait for the signal that will change State to NO_SPILL_INPROGRESS
		// (when NO_SPILL_INPROGRESS is received, it will continue with event MAP_REMAINING_WORK)

		// store the suspended event in mapstate
		setsuspendedEvent_(ev->getApplicationId(), ev->getFsId(), *ev);
	}
	// else if State is not in SPILL_INPROGRESS,
	else{
		// currently there is (mapreduceTaskIO-toBeSentToSort) space available and
		// map task can process min(spillSize - toBeSentToSort, resourceQuantity)
		// amount of data before it needs to call sort (which will call spill).

		// if resourceQuantity > spillSize - toBeSentToSort, then State will be set to SPILL_INPROGRESS,
		// remaining resourceQuantity will be updated to (resourceQuantity- (spillSize - toBeSentToSort)) in event's needed resources
		// after delay event MAP_REMAINING_WORK will be created with remaining resorces and toBeSentToSort will be reset.

		if(resourceQuantity > spillSize - gettoBeSentToSort_(ev->getApplicationId(), ev->getFsId())){
			setstate_(ev->getApplicationId(), ev->getFsId(), SPILL_INPROGRESS);

			work (4, spillSize - gettoBeSentToSort_(ev->getApplicationId(), ev->getFsId()), seized_mapCpuVcores, ev,
					resourceQuantity - spillSize - gettoBeSentToSort_(ev->getApplicationId(), ev->getFsId()), mapreduceTaskIO, taskIntensity);
		}
		// else if resourceQuantity amount of data will be processed, then this is the last part to process! So work function with op0 will be called.
		else{
			work (0, resourceQuantity, seized_mapCpuVcores, ev, 0, mapreduceTaskIO, taskIntensity);
		}
	}
}

void Cpu::setstate_(size_t appID, int fsID, int state){
    for(unsigned int i=0; i < mapState_.size();i++){
    	if ( (mapState_.at(i)).appID_== appID && (mapState_.at(i).fsID_ == fsID)){
    		(mapState_.at(i)).state_ = state;	// waiting for a spill as big as spillbytelimitsize to be completed
    		break;
    	}
    }
}

int Cpu::getstate_(size_t appID, int fsID) const{
	for(unsigned int i=0; i < mapState_.size();i++){
    	if ( (mapState_.at(i)).appID_== appID && (mapState_.at(i).fsID_ == fsID)){
    		return (mapState_.at(i)).state_;
    	}
    }
    return -2;
}

size_t Cpu::gettoBeSentToSort_(size_t appID, int fsID) const{
    for(unsigned int i=0; i < mapState_.size();i++){
    	if ( (mapState_.at(i)).appID_== appID && (mapState_.at(i).fsID_ == fsID)){
    		return (mapState_.at(i)).toBeSentToSort_;
    	}
    }
    return 0;
}

// used during sorting in mapper
void Cpu::sortWork (size_t resourceQuantity, int seized_mapCpuVcores, Event* ev, double taskIntensity, bool hasCombiner){
	#ifndef TERMINAL_LOG_DISABLED
	std::cout << "FUNCTION resourceQuantity " << resourceQuantity << " seized_mapCpuVcores " << seized_mapCpuVcores << " taskIntensity " << taskIntensity<< std::endl;
	#endif
	double processingSpeed = getProcessingSpeed(seized_mapCpuVcores, ev->getFsId(), taskIntensity);

	// delay and create an event for sort function
	#ifndef TERMINAL_LOG_DISABLED
	std::cout << "CPU processingSpeed_______SORT______________: " << processingSpeed <<  " resourceQuantity: " << resourceQuantity << " fsID: " << ev->getFsId() << std::endl;
	#endif
	// delay and enqueue next event
	if(hasCombiner){	// event behavior: RUN_COMBINER
		delay(resourceQuantity, *ev, processingSpeed, CPU_COMBINER_OP, seized_mapCpuVcores, (size_t)processingSpeed, 0, taskIntensity);
	}
	else{	// event behavior: RUN_MAP_SPILL
		delay(resourceQuantity, *ev, processingSpeed, CPU_SPILL_GENERATE_OP, seized_mapCpuVcores, (size_t)processingSpeed, 0, taskIntensity);
	}
}

// used during combiner after (as part of) map phase
void Cpu::combinerWork (size_t resourceQuantity, int seized_mapCpuVcores, Event* ev, double taskIntensity){
	#ifndef TERMINAL_LOG_DISABLED
	std::cout << "FUNCTION resourceQuantity " << resourceQuantity << " seized_mapCpuVcores " << seized_mapCpuVcores << " taskIntensity " << taskIntensity<< std::endl;
	#endif
	double processingSpeed = getProcessingSpeed(seized_mapCpuVcores, ev->getFsId(), taskIntensity);

	// delay and create an event for sort function
	#ifndef TERMINAL_LOG_DISABLED
	std::cout << "CPU processingSpeed_______SORT______________: " << processingSpeed <<  " resourceQuantity: " << resourceQuantity << " fsID: " << ev->getFsId() << std::endl;
	#endif
	// delay and enqueue event (event behavior: RUN_MAP_SPILL)
	delay(resourceQuantity, *ev, processingSpeed, CPU_SPILL_GENERATE_OP, seized_mapCpuVcores, (size_t)processingSpeed, 0, taskIntensity);
}

// used during merging - in mapper
void Cpu::mergeWork (size_t resourceQuantity, int seized_mapCpuVcores, Event* ev, double taskIntensity){
	double processingSpeed = getProcessingSpeed(seized_mapCpuVcores, ev->getFsId(), taskIntensity);

	// delay and create an event for sort function
	#ifndef TERMINAL_LOG_DISABLED
	std::cout << "CPU processingSpeed_______MERGE______________: " << processingSpeed <<  " resourceQuantity: " << resourceQuantity << " fsID: " << ev->getFsId() << std::endl;
	#endif
	// delay and enqueue event (event behavior: MAP_MERGE_WB)

	delay(resourceQuantity, *ev, processingSpeed, CPU_MAP_MERGE_OP, seized_mapCpuVcores, (size_t)processingSpeed, 0, taskIntensity);
}

// used during reduce function in REDUCE
void Cpu::reduceFunctionWork (size_t resourceQuantity, int seized_reduceCpuVcores, Event* ev, double taskIntensity){
	double processingSpeed = getreduceProcessingSpeed(seized_reduceCpuVcores, ev->getRedId(), taskIntensity);

	// delay and create an event for sort function
	#ifndef TERMINAL_LOG_DISABLED
	std::cout << "CPU processingSpeed_______REDUCE FUNCTION_: " << processingSpeed <<
			" seized_reduceCpuVcores " << seized_reduceCpuVcores << " resourceQuantity: " << resourceQuantity << " taskIntensity " << taskIntensity << " fsID: " << ev->getFsId() << std::endl;
	#endif

	// delay and enqueue event (event behavior: FINISH_REDUCE_FUNC)
	delay(resourceQuantity, *ev, processingSpeed, CPU_REDUCE_FUNC_OP, seized_reduceCpuVcores, (size_t)processingSpeed, 0, taskIntensity);
}

// used during merging in in-memory - in reduce
void Cpu::reduceMergeWork (size_t resourceQuantity, int seized_reduceCpuVcores, Event* ev, double taskIntensity){
	double processingSpeed = getreduceProcessingSpeed(seized_reduceCpuVcores, ev->getRedId(), taskIntensity);

	// delay and create an event for sort function
	#ifndef TERMINAL_LOG_DISABLED
	std::cout << "CPU processingSpeed_______MERGE__IN_REDUCE__: " << processingSpeed <<  " resourceQuantity: " << resourceQuantity << " fsID: " << ev->getFsId() << std::endl;
	#endif

	// delay and enqueue event (event behavior: REDUCER_IN_MEM_INTER_MERGE)
	delay(resourceQuantity, *ev, processingSpeed, CPU_REDUCE_IN_MEMORY_MERGE, seized_reduceCpuVcores, (size_t)processingSpeed, 0, taskIntensity);
}

// used during reduce sort
void  Cpu::reduceSort(size_t resourceQuantity, int seized_reduceCpuVcores, Event* ev, double taskIntensity){
	double processingSpeed = getreduceProcessingSpeed(seized_reduceCpuVcores, ev->getRedId(), taskIntensity);
	// delay and create an event for sort function
	#ifndef TERMINAL_LOG_DISABLED
	std::cout << "CPU processingSpeed_______SORT_PHASE__IN_REDUCE__: " << processingSpeed <<  " resourceQuantity: " << resourceQuantity << " redID: " << ev->getRedId() << std::endl;
	#endif

	// delay and enqueue event (event behavior: RELEASE_AND_FINISH_REDUCE_SORT)
	delay(resourceQuantity, *ev, processingSpeed, CPU_REDUCE_SORT, seized_reduceCpuVcores, (size_t)processingSpeed, 0, taskIntensity);
}

void Cpu::setMapFinish(size_t appID, int fsID, double finishTime){
    for(unsigned int i=0; i < mapState_.size();i++){
    	if ( (mapState_.at(i)).appID_== appID && (mapState_.at(i).fsID_ == fsID)){
    		(mapState_.at(i)).finishTime_ = finishTime;
    		(mapState_.at(i)).remainingData_ = 0;
    		(mapState_.at(i)).state_ = -1;	// finished!
    		break;
    	}
    }
}

void Cpu::addToBeSentToSort(size_t appID, int fsID, size_t toBeSentToSort){
    for(unsigned int i=0; i < mapState_.size();i++){
    	if ( (mapState_.at(i)).appID_== appID && (mapState_.at(i).fsID_ == fsID)){
    		(mapState_.at(i)).toBeSentToSort_ += toBeSentToSort;
    		break;
    	}
    }
}

void Cpu::resetToBeSentToSort(size_t appID, int fsID){
    for(unsigned int i=0; i < mapState_.size();i++){
    	if ( (mapState_.at(i)).appID_== appID && (mapState_.at(i).fsID_ == fsID)){
    		(mapState_.at(i)).toBeSentToSort_ = 0;
    		break;
    	}
    }
}

// used during map function, merging, reducing,...
void Cpu::work (int op, size_t resourceQuantity, int seized_mapCpuVcores, Event* ev, size_t remainingMapIntermediateOutputSize, size_t mapreduceTaskIO, double taskIntensity){

	double processingSpeed = (effectiveMapCoreCapacity_ * capacityPerCore_)/taskIntensity;
	// delay and create an event for sort function
	#ifndef TERMINAL_LOG_DISABLED
	std::cout << "work taskIntensity " << taskIntensity << " processingSpeed " << processingSpeed <<  std::endl;
	std::cout << "CPU processingSpeed_____op:" << op <<  "______________: " << processingSpeed <<  " resourceQuantity: " << resourceQuantity <<" fsID: " << ev->getFsId() << std::endl;
	#endif

	// delay and enqueue event
	delay(resourceQuantity, *ev, processingSpeed, op, seized_mapCpuVcores, remainingMapIntermediateOutputSize, mapreduceTaskIO, taskIntensity);
}

int Cpu::opEventMap(int op){

	if(op == 0 || op == 1 || op == CPU_MAP_DELAY_OP){
		return RUN_MAP_SORT;
	}
	else if(op == CPU_SPILL_GENERATE_OP){
		return RUN_MAP_SPILL;
	}
	else if(op == 3 || op == 4){
		return MAP_REMAINING_WORK;
	}
	else if(op == CPU_MAP_MERGE_OP){
		return MAP_MERGE_WB;
	}
	else if(op == CPU_REDUCE_IN_MEMORY_MERGE){
		return REDUCER_IN_MEM_INTER_MERGE;
	}
	else if(op == CPU_REDUCE_FUNC_OP){
		return FINISH_REDUCE_FUNC;
	}
	else if(op == CPU_REDUCE_SORT){
		return RELEASE_AND_FINISH_REDUCE_SORT;
	}
	else if(op == CPU_COMBINER_OP){
		return RUN_COMBINER;
	}
	else{
		std::cerr<< "Undefined OP Type in CPU! " << std::endl;
		return -1;
	}
}

void Cpu::delayHelper(double newEventTime, size_t resourceQuantity, Event ev, int op, int seized_mapCpuVcores,
		size_t remainingMapIntermediateOutputSize, size_t mapreduceTaskIO, double taskIntensity){

	// generate new output event, enqueue to eventsList
	if(op == 0){	// last map function...
		// generate sort event  (Note: seized_mapCpuVcores stored as attribute)
		Event newEvent(ev.getApplicationId(), newEventTime, (resourceQuantity + gettoBeSentToSort_(ev.getApplicationId(), ev.getFsId())), ev.getSeizedResQuantity(), ev.getDestinationEventType(), ev.getDestinationEventType(),
				opEventMap(op), ev.getEntityIns().getEntityType(), SEIZETOMASTER, seized_mapCpuVcores, ev.getFsLoc(), ev.getFsId(), ev.getRedId());
		eventsList.push(newEvent);

		setMapFinish(ev.getApplicationId(), ev.getFsId(), ev.getEventTime());
		#ifndef TERMINAL_LOG_DISABLED
		std::cout << "mapFunction completion time for filesplitID: " << ev.getFsId() <<  " is: " << newEventTime << std::endl;
		#endif
	}
	else if(op == 1){
		// note that it generates event behavior RUN_MAP_SORT!!!
		#ifndef TERMINAL_LOG_DISABLED
		std::cout << "newEventTime: " << newEventTime << " resourceQuantity " << resourceQuantity<< std::endl;
		#endif
		Event newEvent(ev.getApplicationId(), newEventTime, resourceQuantity, ev.getSeizedResQuantity(), ev.getDestinationEventType(), ev.getDestinationEventType(),
				opEventMap(op), ev.getEntityIns().getEntityType(), SEIZETOMASTER, seized_mapCpuVcores, ev.getFsLoc(), ev.getFsId(), ev.getRedId());
		eventsList.push(newEvent);
		setstate_(ev.getApplicationId(), ev.getFsId(), SPILL_INPROGRESS);

		// check whether remainingMapIntermediateOutputSize < total - spillLimitInBytes (= mapreduceTaskIO-resourceQuantity)
		#ifndef TERMINAL_LOG_DISABLED
		std::cout << "remainingMapIntermediateOutputSize__: " << remainingMapIntermediateOutputSize <<  " (mapreduceTaskIO-resourceQuantity): "
				<< (mapreduceTaskIO-resourceQuantity) << " fsId: " << ev.getFsId() << std::endl;
		#endif

		if(remainingMapIntermediateOutputSize <=(mapreduceTaskIO-resourceQuantity)){	// delay and create an event for sort function
			work (0, remainingMapIntermediateOutputSize, seized_mapCpuVcores, &ev, 0, mapreduceTaskIO, taskIntensity);
		}
		else{
			remainingMapIntermediateOutputSize -= (mapreduceTaskIO-resourceQuantity);
			work (3, (mapreduceTaskIO-resourceQuantity), seized_mapCpuVcores, &ev, remainingMapIntermediateOutputSize, mapreduceTaskIO, taskIntensity);
		}
	}
	else if(op == 3){ // map part of filesplit (there is still more to process)
		addToBeSentToSort(ev.getApplicationId(), ev.getFsId(), resourceQuantity);

		// mapreduceTaskIO is passed in seized resource quantity
		Event newEvent(ev.getApplicationId(), newEventTime, remainingMapIntermediateOutputSize, mapreduceTaskIO, ev.getDestinationEventType(), ev.getDestinationEventType(),
				opEventMap(op), ev.getEntityIns().getEntityType(), SEIZETOMASTER, seized_mapCpuVcores, ev.getFsLoc(), ev.getFsId(), ev.getRedId());
		eventsList.push(newEvent);
	}
	else if(op == 4){
		resetToBeSentToSort(ev.getApplicationId(), ev.getFsId());
		// mapreduceTaskIO is passed in seized resource quantity
		Event newEvent(ev.getApplicationId(), newEventTime, remainingMapIntermediateOutputSize, mapreduceTaskIO, ev.getDestinationEventType(), ev.getDestinationEventType(),
				opEventMap(op), ev.getEntityIns().getEntityType(), SEIZETOMASTER, seized_mapCpuVcores, ev.getFsLoc(), ev.getFsId(), ev.getRedId());
		eventsList.push(newEvent);
	}

	// op CPU_REDUCE_IN_MEMORY_MERGE: delay for reduce in memory merge
	// op CPU_REDUCE_SORT: delay for reduce sort
	else if( op == CPU_REDUCE_IN_MEMORY_MERGE || op == CPU_REDUCE_SORT){
		Event newEvent(ev.getApplicationId(), newEventTime, resourceQuantity, 0, ev.getDestinationEventType(), ev.getDestinationEventType(),
				opEventMap(op), ev.getEntityIns().getEntityType(), SEIZETOMASTER, ev.getEntityIns().getAttribute(), ev.getFsLoc(), ev.getFsId(), ev.getRedId());
		eventsList.push(newEvent);
	}
	// op CPU_SPILL_GENERATE_OP: 	delay for sort generate spill event  (Note: seized_mapCpuVcores stored as attribute)
	// op CPU_MAP_MERGE_OP: 		delay for map merge // generate spill event  (Note: seized_mapCpuVcores stored as attribute)
	// op CPU_REDUCE_FUNC_OP: 		delay for reduce function
	// op CPU_MAP_DELAY_OP: 		delay for map function
	// op CPU_COMBINER_OP: 			delay for combiner function
	else if(op == CPU_SPILL_GENERATE_OP || op == CPU_MAP_MERGE_OP || op == CPU_REDUCE_FUNC_OP || op == CPU_COMBINER_OP || op == CPU_MAP_DELAY_OP){
		Event newEvent(ev.getApplicationId(), newEventTime, resourceQuantity, 0, ev.getDestinationEventType(), ev.getDestinationEventType(),
				opEventMap(op), ev.getEntityIns().getEntityType(), SEIZETOMASTER, seized_mapCpuVcores, ev.getFsLoc(), ev.getFsId(), ev.getRedId(), ev.getRecordId(), ev.getSpillTally());
		eventsList.push(newEvent);
	}
	else{
		std::cerr<< "Error: Undefined OP type in CPU!" << std::endl;
	}
}

double Cpu::delay (size_t resourceQuantity, Event ev, double processingSpeed, int op, int seized_mapCpuVcores,
		size_t remainingMapIntermediateOutputSize, size_t mapreduceTaskIO, double taskIntensity){
	// a random generator engine from a real time-based seed:
	unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
	std::default_random_engine generator (seed);
	double baseTransferTime = (double)resourceQuantity / processingSpeed;

	#ifndef TERMINAL_LOG_DISABLED
	std::cout<< "baseTransferTime: " << baseTransferTime << std::endl;
	#endif
	double newEventTime = ev.getEventTime() + baseTransferTime;
	double additionalDelay = baseTransferTime * delayratio_;

	switch (delayType_){
		case UNIFORM:{
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "in UNIFORM" << std::endl;
			#endif

			// Adjust time units to be stored as seconds in simulation time
			if (unit_ == MINUTES){
				additionalDelay *= MINUTE_IN_SEC;
			}
			else if(unit_ == HOURS){
				additionalDelay *= HOUR_IN_SEC;
			}

			std::uniform_real_distribution<double> distribution (0, additionalDelay);
			// time is updated here
			newEventTime += distribution(generator);
			delayHelper(newEventTime, resourceQuantity, ev, op, seized_mapCpuVcores, remainingMapIntermediateOutputSize, mapreduceTaskIO, taskIntensity);

			break;}
		case EXPONENTIAL:{
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "in EXPONENTIAL" << std::endl;
			#endif

			std::exponential_distribution<double> exponential(EXP_CPU_DELAY_CONSTANT/additionalDelay);

			if (unit_ == MINUTES){
				newEventTime += MINUTE_IN_SEC*exponential(generator);
			}
			else if(unit_ == HOURS){
				newEventTime += HOUR_IN_SEC*exponential(generator);
			}
			else{
				newEventTime += exponential(generator);
			}
			delayHelper(newEventTime, resourceQuantity, ev, op, seized_mapCpuVcores, remainingMapIntermediateOutputSize, mapreduceTaskIO, taskIntensity);

			break;}
		case CONSTANT:{
			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "in CONSTANT" << std::endl;
			#endif

			if (unit_ == MINUTES){
				newEventTime += MINUTE_IN_SEC*additionalDelay;
			}
			else if(unit_ == HOURS){
				newEventTime += HOUR_IN_SEC*additionalDelay;
			}
			else{
				newEventTime += additionalDelay;
			}
			delayHelper(newEventTime, resourceQuantity, ev, op, seized_mapCpuVcores, remainingMapIntermediateOutputSize, mapreduceTaskIO, taskIntensity);
			break;}
	}
	return newEventTime;
}

int Cpu::getRemainingNumberOfCores() const {
	return remainingNumberOfCores_;
}

void Cpu::setRemainingNumberOfCores(int remainingNumberOfCores) {
	remainingNumberOfCores_ = remainingNumberOfCores;
}

size_t Cpu::getRemainingCapacity() const {
	return capacityPerCore_;
}
