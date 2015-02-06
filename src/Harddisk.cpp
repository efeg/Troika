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

#include "Harddisk.h"
#include <random>
#include <iostream>

#ifdef CFQ_HD_SCHEDULER
#define CFQ_QUANTA 0.1		// CFQ with max. 1 second quanta

Harddisk::Harddisk(size_t maxReadSpeed, size_t maxWriteSpeed, size_t minReadSpeed, size_t minWriteSpeed, enum DistributionType delayType, enum TimeType unit, double delayratio):
		  delayType_(delayType), unit_(unit), delayratio_(delayratio), readCapInUsePercent_(0), writeCapInUsePercent_(0){

	// a random generator engine from a real time-based seed:
	unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
	std::default_random_engine generator (seed);
	std::uniform_real_distribution<double> 	readDist (0, (maxReadSpeed - minReadSpeed));
	std::uniform_real_distribution<double> writeDist (0, (maxWriteSpeed - minWriteSpeed));
	// time is updated here
	remainingReadCapacity_ = readDist(generator) + minReadSpeed;
	remainingWriteCapacity_ = writeDist(generator) + minWriteSpeed;
	totalReadCapacity_ = remainingReadCapacity_;
	totalWriteCapacity_ = remainingWriteCapacity_;

	debugTotalReduce_=0;
}

Harddisk::~Harddisk() {
}

void Harddisk::saveHDrequest(size_t entityID, Event* ev, int eventBehavior, double neededResourceQuantity){

	if (entityID_event_.find(entityID) == entityID_event_.end()) {	// not found
		entityID_event_[entityID].applicationId_ = ev->getAppID();
		entityID_event_[entityID].eventTime_ = ev->getEventTime();
		entityID_event_[entityID].neededResourceQuantity_ = neededResourceQuantity;
		entityID_event_[entityID].seizedResQuantity_ =ev->getSeizedResQuantity();
		entityID_event_[entityID].nextEvent_ =ev->getNextEventType();
		entityID_event_[entityID].destinationEventType_ = ev->getDestEventType();
		entityID_event_[entityID].eventBehavior_ =ev->getEventBehavior();
		entityID_event_[entityID].entityType_ =ev->getEntityIns().getEntityType();
		entityID_event_[entityID].attribute_ = ev->getEntityIns().getAttribute();
		entityID_event_[entityID].fsLoc_ = ev->getFsLoc();
		entityID_event_[entityID].fsId_ = ev->getFsId();
		entityID_event_[entityID].redID_ = ev->getRedId();
		entityID_event_[entityID].recordID_ = ev->getRecordId();
		entityID_event_[entityID].spillTally_ = ev->getSpillTally();
		entityID_event_[entityID].completionEvBeh = eventBehavior;
		entityID_event_[entityID].entityId_ = entityID;
	}
	else {	// found
		std::cerr << "Error: Attempted to save event with entity ID "<< entityID << " twice." << std::endl;
		exit(-1);
	}
}

hdCFQdata Harddisk::getHDrequest(size_t entityID){
	if ( entityID_event_.find(entityID) == entityID_event_.end() ) {	// not found
		std::cerr << "Error: No HD request with entity ID "<< entityID << " was found." << std::endl;
		exit(-1);
	}
	else {	// found
		return entityID_event_[entityID];
	}
}

void Harddisk::eraseHDrequest(size_t entityID){
	if ( entityID_event_.find(entityID) == entityID_event_.end() ) {	// not found
		std::cerr << "Error: No HD request with entity ID "<< entityID << " was found." << std::endl;
		exit(-1);
	}
	else {	// found
		entityID_event_.erase(entityID);
	}
}

bool Harddisk::incCheckForFsIdNextPost(int key, size_t limit){
	if ( key_limit_.find(key) == key_limit_.end() ) {	// not found
		key_limit_[key] = 1;
	}
	else{
		key_limit_[key]++;
	}

	if(key_limit_[key] == limit){
		key_limit_[key] = 0;
		return true;
	}
	return false;
}

int Harddisk::calculateIOSpeed(double *estimatedIOSpeed, int *capInUsePercent, double totIOcapacity){
	/*
	 * CFQ resource usage.
	 */
	if(!readCapInUsePercent_ && !writeCapInUsePercent_){
		*estimatedIOSpeed = totIOcapacity;
		*capInUsePercent = 10;
	}
	return 0;
}

double Harddisk::calculateReadSpeed(){
	double estimatedReadSpeed;
	calculateIOSpeed(&estimatedReadSpeed, &readCapInUsePercent_, totalReadCapacity_);
	return estimatedReadSpeed;
}

double Harddisk::calculateWriteSpeed(){
	double estimatedWriteSpeed;
	calculateIOSpeed(&estimatedWriteSpeed, &writeCapInUsePercent_, totalWriteCapacity_);
	return estimatedWriteSpeed;
}

void Harddisk::readInterWork(double neededResourceQuantity, Event* ev, int outputEventType, int nextEvent, int queueID){
	double transferSpeed;
	// delay and create a "read complete" event to be sent to the worker node
	if(readCapInUsePercent_ + writeCapInUsePercent_ < 10){
		// seize local resources
		transferSpeed = calculateReadSpeed();
		// seize local resources
		remainingReadCapacity_ -= transferSpeed;
		delay(neededResourceQuantity, ev->getEventTime(), transferSpeed, ev, outputEventType);
	}
	else{
		struct waitingQueueElement waitingEvent;

		setWaitingEvent(&waitingEvent, ev->getAppID(), ev->getEventTime(), neededResourceQuantity, ev->getDestEventType(), nextEvent, ev->getEntityIns().getEntityId(),
				ev->getEntityIns().getEntityType(), ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId(), ev->getRecordId(), ev->getSpillTally());

		waitingEvent.type_ = true;	// true: read

		if(queueID == HD_READ_FROM_REMOTE){
			// save output event type
			waitingEvent.outputEventType_ = outputEventType;
			waitingQueue_.push(waitingEvent);
		}
		else{
			// enqueue "wait for local read" event (when a resource is released queue will be checked)
			waitingEvent.outputEventType_ = ev->getDestEventType();

			if(queueID == HD_READ_FOR_MAP_TASK || queueID == HD_READ_MAP_OUTPUT || queueID == HD_READ_DATA_TO_SHUFFLE || queueID == HD_READ_SHUFFLED_DATA || queueID == HD_READ_TO_MERGE ||  queueID == HD_READ_TOBE_SORTED_REDUCE_DATA){
				waitingQueue_.push(waitingEvent);
			}
			else{
				std::cerr << "Error: Unexpected queueID type in HD! " << std::endl;
			}
		}
	}
}

void Harddisk::writeInterWork(double neededResourceQuantity, Event* ev, int outputEventType, int nextEvent, int queueID){
	double transferSpeed;
	// delay and create a "write complete" event which will read the written data

	if(readCapInUsePercent_ + writeCapInUsePercent_ < 10){
		// seize local resources
		transferSpeed = calculateWriteSpeed();
		// seize local resources
		remainingWriteCapacity_ -= transferSpeed;
		delay(neededResourceQuantity, ev->getEventTime(), transferSpeed, ev, outputEventType);
	}
	else{
		// enqueue "wait for local write" event (when a resource is released queue will be checked)
		struct waitingQueueElement waitingEvent;

		setWaitingEvent(&waitingEvent, ev->getAppID(), ev->getEventTime(), neededResourceQuantity, ev->getDestEventType(), nextEvent, ev->getEntityIns().getEntityId(),
				ev->getEntityIns().getEntityType(), ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId(), ev->getRecordId(), ev->getSpillTally());

		waitingEvent.type_ = false;	// false: write
		if(queueID == HD_WRITE_TO_LOCAL || queueID == HD_WRITE_TRANSFERRED_TO_LOCAL || queueID == HD_WRITE_MERGED_DATA || queueID == HD_WRITE_SHUFFLE_DATA_TO_DISK
				|| queueID == HD_WRITE_REDUCE_OUTPUT || queueID == HD_WRITE_SORTED_REDUCE_DATA){
			waitingQueue_.push(waitingEvent);
		}
		else{
			std::cerr << "Error: Unexpected queueID type in HD! " << std::endl;
		}
	}
}

void Harddisk::checkWaitingQueue(Event* ev, int outputEventType){
	double transferSpeed;
	// check whether there is any waiting event in waitingQueue_
	while (!waitingQueue_.empty()){
		// check resource availability

		if (readCapInUsePercent_ + writeCapInUsePercent_ < 10){
			if (waitingQueue_.front().type_){	// read req.
				transferSpeed = calculateReadSpeed();
				// seize local resources
				remainingReadCapacity_ -= transferSpeed;
			}
			else{	// write rq.
				transferSpeed = calculateWriteSpeed();
				// seize local resources
				remainingWriteCapacity_ -= transferSpeed;
			}

			Event newEvent(waitingQueue_.front().applicationId_, waitingQueue_.front().eventTime_, waitingQueue_.front().neededResourceQuantity_, 0, waitingQueue_.front().outputEventType_,
					waitingQueue_.front().destinationEventType_, waitingQueue_.front().nextEvent_, waitingQueue_.front().entityType_, SEIZETOMASTER, waitingQueue_.front().attribute_,
					waitingQueue_.front().fsLoc_, waitingQueue_.front().fsId_, waitingQueue_.front().redID_, waitingQueue_.front().recordID_, waitingQueue_.front().spillTally_);

			newEvent.setEntityId(waitingQueue_.front().entityId_);

			delay(waitingQueue_.front().neededResourceQuantity_, ev->getEventTime(), transferSpeed, &newEvent, waitingQueue_.front().outputEventType_);
			waitingQueue_.pop();
		}
		else{
			break;
		}
	}
}

void Harddisk::setWaitingEvent(struct waitingQueueElement *waitingEvent, size_t appId, double eventTime, double neededResourceQuantity, int destinationEventType, int nextEvent, size_t entityId,
		enum EntityType entityType, int attribute, int fsLoc, int fsId, int redID, int recordID, size_t spillTally){
	waitingEvent->applicationId_ = appId;
	waitingEvent->eventTime_ = eventTime;
	waitingEvent->neededResourceQuantity_ = neededResourceQuantity;
	waitingEvent->destinationEventType_ = destinationEventType;
	waitingEvent->nextEvent_ = nextEvent;
	waitingEvent->entityId_ = entityId;
	waitingEvent->entityType_ = entityType;
	waitingEvent->attribute_ = attribute;
	waitingEvent->fsLoc_ = fsLoc;
	waitingEvent->fsId_ = fsId;
	waitingEvent->redID_ = redID;
	waitingEvent->recordID_ = recordID;
	waitingEvent->spillTally_ = spillTally;
}

void Harddisk::releaseIOResources(double seizedSpeedCapacity, double* remainingIoCapacity, int* capInUsePercent, double totCapacity){
	// release seized local resource (which is the seized read or write capacity of hd)
	*remainingIoCapacity += seizedSpeedCapacity;
	*capInUsePercent = 0;
}

void Harddisk::releaseReadResources(double seizedReadSpeedCapacity, Event* ev, int outputEventType, int op){
	releaseIOResources(seizedReadSpeedCapacity, &remainingReadCapacity_, &readCapInUsePercent_, totalReadCapacity_);

	if(op == HD_CFQ_EVENT){
		// enqueue the remaining part to the end of queue

		struct waitingQueueElement waitingEvent;
		setWaitingEvent(&waitingEvent, ev->getAppID(), ev->getEventTime(), ev->getNeededResQuantity(), ev->getDestEventType(), getHDrequest(ev->getEntityIns().getEntityId()).completionEvBeh, ev->getEntityIns().getEntityId(),
				ev->getEntityIns().getEntityType(), ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId(), ev->getRecordId(), ev->getSpillTally());

		if(waitingEvent.nextEvent_ == FS_RECORD_READ_FINISH || waitingEvent.nextEvent_ == RECEIVE_FS_TRANSFER_REQUEST || waitingEvent.nextEvent_ == MERGE_PARTITIONS_FOR_REDUCER || waitingEvent.nextEvent_ == SHUFFLE_READ_DATA_COMPLETE ||
				waitingEvent.nextEvent_ == START_REDUCE_FUNC || waitingEvent.nextEvent_ == ON_DISK_MERGE_READ_COMPLETE || waitingEvent.nextEvent_ == RELEASE_AND_START_REDUCE_SORT){	// read request
			waitingEvent.type_ = true;	// true: read

			if(waitingEvent.nextEvent_ == RECEIVE_FS_TRANSFER_REQUEST){
				// save output event type
				waitingEvent.outputEventType_ = outputEventType;
			}
			else{
				// enqueue "wait for local read" event (when a resource is released queue will be checked)
				waitingEvent.outputEventType_ = ev->getDestEventType();
			}
			waitingQueue_.push(waitingEvent);
		}
		else{
			std::cerr << "Error: Unexpected request type in CFQ HD! " << std::endl;
		}
	}
	checkWaitingQueue(ev, outputEventType);
}

void Harddisk::releaseWriteResources(double seizedWriteSpeedCapacity, Event* ev, int outputEventType, int op){
	releaseIOResources(seizedWriteSpeedCapacity, &remainingWriteCapacity_, &writeCapInUsePercent_, totalWriteCapacity_);

	if(op == HD_CFQ_EVENT){
		// enqueue the remaining part to the end of queue

		struct waitingQueueElement waitingEvent;
		setWaitingEvent(&waitingEvent, ev->getAppID(), ev->getEventTime(), ev->getNeededResQuantity(), ev->getDestEventType(), getHDrequest(ev->getEntityIns().getEntityId()).completionEvBeh, ev->getEntityIns().getEntityId(),
				ev->getEntityIns().getEntityType(), ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId(), ev->getRecordId(), ev->getSpillTally());

		if(waitingEvent.nextEvent_ == RELEASE_FS_TRANSFERRED_WRITE_RES || waitingEvent.nextEvent_ == MAP_MERGE_READY || waitingEvent.nextEvent_ == MAP_MERGE_WB_COMPLETE ||
				waitingEvent.nextEvent_ == SHUFFLE_WRITE_DATA_PARTIALLY_COMPLETE || waitingEvent.nextEvent_ == WRITE_REDUCE_OUTPUT || waitingEvent.nextEvent_ == RELEASE_AND_DONE_REDUCE_SORT){	// write request
			waitingEvent.type_ = false;	// false: write
			waitingEvent.outputEventType_ = ev->getDestEventType();
			waitingQueue_.push(waitingEvent);
		}
		else{
			std::cerr << "Error: Unexpected request type in CFQ HD! " << std::endl;
		}
	}
	checkWaitingQueue(ev, outputEventType);
}

void Harddisk::work (int op, double neededResourceQuantity, Event* ev, int outputEventType){
	// read
	if(op == HD_READ_FOR_MAP_TASK || op == HD_READ_FROM_REMOTE || op == HD_READ_MAP_OUTPUT || op == HD_READ_DATA_TO_SHUFFLE || op == HD_READ_SHUFFLED_DATA
			|| op == HD_READ_TO_MERGE || op == HD_READ_TOBE_SORTED_REDUCE_DATA){
		int completionEvBeh;
		if(op == HD_READ_FOR_MAP_TASK){
			// read filesplit from local hard disk for mapper
			completionEvBeh = FS_RECORD_READ_FINISH;
			// it also produces an event to read another record from the same fsId if there exist any
		}
		else if(op == HD_READ_FROM_REMOTE){
			// read filesplit from remote hard disk for mapper
			completionEvBeh = RECEIVE_FS_TRANSFER_REQUEST;
		}
		else if(op == HD_READ_MAP_OUTPUT){
			// read map output from local hard disk for map merge (event behavior: MERGE_PARTITIONS_FOR_REDUCER)
			completionEvBeh = MERGE_PARTITIONS_FOR_REDUCER;
		}
		else if(op == HD_READ_DATA_TO_SHUFFLE){
			// read shuffle data from local hard disk (event behavior: SHUFFLE_READ_DATA_COMPLETE)
			completionEvBeh = SHUFFLE_READ_DATA_COMPLETE;
		}
		else if(op == HD_READ_SHUFFLED_DATA){
			// read shuffled data from reducer local disk (event behavior: START_REDUCE_FUNC)
			completionEvBeh = START_REDUCE_FUNC;

			// it also produces an event to read another record from the same redId if there exist any
		}
		else if(op == HD_READ_TO_MERGE){
			// read data for on disk merge (event behavior: ON_DISK_MERGE_READ_COMPLETE)
			completionEvBeh = ON_DISK_MERGE_READ_COMPLETE;
		}
		else if(op == HD_READ_TOBE_SORTED_REDUCE_DATA){
			// read data for on disk merge (event behavior: RELEASE_AND_START_REDUCE_SORT)
			completionEvBeh = RELEASE_AND_START_REDUCE_SORT;
		}
		// save hd request
		saveHDrequest(ev->getEntityIns().getEntityId(), ev, completionEvBeh, neededResourceQuantity);
		readInterWork(neededResourceQuantity, ev, outputEventType, completionEvBeh, op);
	}

	// write
	else if(op == HD_WRITE_TO_LOCAL || op == HD_WRITE_TRANSFERRED_TO_LOCAL || op == HD_WRITE_MERGED_DATA || op == HD_WRITE_SHUFFLE_DATA_TO_DISK
			|| op == HD_WRITE_REDUCE_OUTPUT || op == HD_WRITE_SORTED_REDUCE_DATA){
		int completionEvBeh;
		if(op == HD_WRITE_TO_LOCAL){
			// write transferred data to the local disk
			completionEvBeh = RELEASE_FS_TRANSFERRED_WRITE_RES;
		}
		else if(op == HD_WRITE_TRANSFERRED_TO_LOCAL){
			// write transferred data to the local disk - release spill resource and start merging (event behavior: MAP_MERGE_READY)
			completionEvBeh = MAP_MERGE_READY;
		}
		else if(op == HD_WRITE_MERGED_DATA){
			// write merged map data to the local disk - (event behavior: MAP_MERGE_WB_COMPLETE)
			completionEvBeh = MAP_MERGE_WB_COMPLETE;
		}
		else if(op == HD_WRITE_SHUFFLE_DATA_TO_DISK){
			// write shuffles data to the reducer disk (event behavior: SHUFFLE_WRITE_DATA_PARTIALLY_COMPLETE)
			completionEvBeh = SHUFFLE_WRITE_DATA_PARTIALLY_COMPLETE;
		}
		else if(op == HD_WRITE_REDUCE_OUTPUT){

			#ifndef TERMINAL_LOG_DISABLED
			std::cout << "neededResourceQuantity " << neededResourceQuantity << std::endl;
			debugTotalReduce_ += neededResourceQuantity;
			std::cout << "debugTotalReduce " << debugTotalReduce_ << std::endl;
			#endif
			// write final application output (event behavior: WRITE_REDUCE_OUTPUT)
			completionEvBeh = WRITE_REDUCE_OUTPUT;
		}
		else if(op == HD_WRITE_SORTED_REDUCE_DATA){
			// write reduce sort output to the local disk  (event behavior: RELEASE_AND_DONE_REDUCE_SORT)
			completionEvBeh = RELEASE_AND_DONE_REDUCE_SORT;
		}
		// save hd request
		saveHDrequest(ev->getEntityIns().getEntityId(), ev, completionEvBeh, neededResourceQuantity);
		writeInterWork(neededResourceQuantity, ev, outputEventType, completionEvBeh, op);
	}

	/*-----------------------RELEASE READ RESOURCES-------------------------*/
	// release local hard disk resource for corresponding filesplit read from local hd for mapper
	// release merge read resource
	// release shuffle read resource
	// release shuffled data read resource
	// release On Disk Merge Data resources from HD
	// release ToBeSorted Reduce Data resources from HD
	/*-----------------------RELEASE READ RESOURCES-------------------------*/

	// also check for waiting queues and activate waiting requests.
	else if(op == READ_RESOURCE_RELEASE){
		releaseReadResources(neededResourceQuantity, ev, outputEventType, op);
	}

	/*-----------------------RELEASE WRITE RESOURCES-------------------------*/
	// release local hard disk resource for corresponding filesplit write to local hd for mapper
	// release write data to the local disk (spill) resources
	// release local hard disk resource for corresponding merged map data
	// release shuffled write resource for reducer
	// release final application output write resource for reducer
	// release local hard disk resource for write identity map output to the local disk
	// release write resources for sorted Reduce Data
	/*-----------------------RELEASE WRITE RESOURCES-------------------------*/

	// also check for waiting queues and activate waiting requests.
	else if(op == WRITE_RESOURCE_RELEASE){
		releaseWriteResources(neededResourceQuantity, ev, outputEventType, op);
	}

	else if(op == HD_CFQ_EVENT){
		/*
		 * Release read or write resource and continue remaining part of the operation.
		 */
		int completionEvBeh = getHDrequest(ev->getEntityIns().getEntityId()).completionEvBeh;

		if(completionEvBeh == FS_RECORD_READ_FINISH || completionEvBeh == RECEIVE_FS_TRANSFER_REQUEST || completionEvBeh == MERGE_PARTITIONS_FOR_REDUCER || completionEvBeh == SHUFFLE_READ_DATA_COMPLETE || completionEvBeh == START_REDUCE_FUNC ||
				completionEvBeh == ON_DISK_MERGE_READ_COMPLETE || completionEvBeh == RELEASE_AND_START_REDUCE_SORT){	// read release
			releaseReadResources(neededResourceQuantity, ev, outputEventType, op);
		}
		else{	// write release
			releaseWriteResources(neededResourceQuantity, ev, outputEventType, op);
		}
	}
	else{
		std::cerr << "Error: Unexpected HD operation type op: " << op << std::endl;
	}
}

void Harddisk::delayHelper(double newEventTime, double neededResourceQuantity, double seizedResource, int evBehavior, Event* ev, int outputEventType){

	// generate new output event, enqueue to eventsList
	if(evBehavior == RECEIVE_FS_TRANSFER_REQUEST){ // remote (one event to release. one event to transfer)
		// release
		Event newEvent(ev->getAppID(), newEventTime, 0.0, seizedResource, ev->getDestEventType(), ev->getDestEventType(),
				RELEASE_REMOTE_FS_READ_RES, ev->getEntityIns().getEntityType(), SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId(), ev->getRecordId(), ev->getSpillTally());
		eventsList.push(newEvent);
		// start transfer
		Event newEvent2(ev->getAppID(), newEventTime, neededResourceQuantity, 0.0, outputEventType, ev->getEntityIns().getAttribute(),
				WRITE_TRANSFERRED_FS_TO_LOCAL, ev->getEntityIns().getEntityType(), SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId(), ev->getRecordId(), ev->getSpillTally());
		eventsList.push(newEvent2);
	}
	else if(evBehavior == FS_RECORD_READ_FINISH || evBehavior == RELEASE_FS_TRANSFERRED_WRITE_RES || evBehavior == MAP_MERGE_READY || evBehavior == MERGE_PARTITIONS_FOR_REDUCER || evBehavior == MAP_MERGE_WB_COMPLETE
			|| evBehavior == SHUFFLE_READ_DATA_COMPLETE || evBehavior == SHUFFLE_WRITE_DATA_PARTIALLY_COMPLETE || evBehavior == WRITE_REDUCE_OUTPUT || evBehavior == START_REDUCE_FUNC || evBehavior == ON_DISK_MERGE_READ_COMPLETE
			|| evBehavior == RELEASE_AND_START_REDUCE_SORT || evBehavior == RELEASE_AND_DONE_REDUCE_SORT){

		Event newEvent(ev->getAppID(), newEventTime, neededResourceQuantity, seizedResource, ev->getDestEventType(), ev->getDestEventType(),
				evBehavior, ev->getEntityIns().getEntityType(), SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId(), ev->getRecordId(), ev->getSpillTally());
		eventsList.push(newEvent);

		if(evBehavior == FS_RECORD_READ_FINISH && ev->getRecordId() && incCheckForFsIdNextPost(ev->getFsId(), ev->getSpillTally())){ // nonzero ev->getRecordId() and total count is a multiple of BUFFER_NUMBER_OF_PACKETS
			// it produces an event to read another record from the same fsId if there exist any (as an input to map function)
			Event newEvent(ev->getAppID(), newEventTime, -1, -1, ev->getDestEventType(), ev->getDestEventType(),
					READ_NEW_RECORD, MAPTASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId(), ev->getRecordId(), ev->getSpillTally());
			eventsList.push(newEvent);
		}
		else if(evBehavior == START_REDUCE_FUNC && ev->getRecordId() && incCheckForFsIdNextPost(ev->getRedId(), ev->getSpillTally())){
			// create a new event for the first record to be read at HD
			Event newEvent(ev->getAppID(), newEventTime, -1, -1, ev->getDestEventType(), ev->getDestEventType(),
					READ_REDUCE_RECORDS_TO_SORT, REDUCETASK, SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId(), ev->getRecordId(), ev->getSpillTally());
			eventsList.push(newEvent);
		}
	}
	else if(evBehavior == HD_CFQ_EVENT){		// evBehavior == 100: perform given i/o operation then enqueue remaining job to waitingQueue upon completion with the following event
		// neededResourceQuantity: remaining data to be read / write. seizedResource: transfer speed used to determine how much resource to release
		Event newEvent(ev->getAppID(), newEventTime, neededResourceQuantity, seizedResource, ev->getDestEventType(), ev->getDestEventType(),
				evBehavior, ev->getEntityIns().getEntityType(), SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId(), ev->getRecordId(), ev->getSpillTally());

		// this is required for detection of entry in entityID_event_
		newEvent.setEntityId( ev->getEntityIns().getEntityId());
		eventsList.push(newEvent);
	}
	else{
		std::cerr << "Error: Unexpected evBehavior in delay helper in HD: " << evBehavior << std::endl;
	}
}

void Harddisk::delay (double neededResourceQuantity, double currentTime, double transferSpeed, Event* ev, int outputEventType){
	// a random generator engine from a real time-based seed:
	unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
	std::default_random_engine generator (seed);
	double baseTransferTime = neededResourceQuantity / transferSpeed;
	double newEventTime = currentTime + baseTransferTime;
	double additionalDelay = baseTransferTime * delayratio_;

	switch (delayType_){
		case UNIFORM:{

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

			break;}
		case EXPONENTIAL:{

			std::exponential_distribution<double> exponential(EXP_HD_DELAY_CONSTANT/additionalDelay);

			if (unit_ == MINUTES){
				newEventTime += MINUTE_IN_SEC*exponential(generator);
			}
			else if(unit_ == HOURS){
				newEventTime += HOUR_IN_SEC*exponential(generator);
			}
			else{
				newEventTime += exponential(generator);
			}

			break;}
		case CONSTANT:{

			if (unit_ == MINUTES){
				newEventTime += MINUTE_IN_SEC*additionalDelay;
			}
			else if(unit_ == HOURS){
				newEventTime += HOUR_IN_SEC*additionalDelay;
			}
			else{
				newEventTime += additionalDelay;
			}
			break;}
		default:{
			std::cerr << "Error: Incorrect delay type!" << std::endl;
			exit(-1);
			break;}
	}

	if(baseTransferTime > CFQ_QUANTA){
		// run the read / write operation for CFQ_QUANTA seconds then upon completion of CFQ_QUANTA
		// seconds release the seized resources and enqueue back to waiting queue to complete remaining part.

		double remainingIoOperationSize = neededResourceQuantity - (transferSpeed*CFQ_QUANTA);
		newEventTime = currentTime + CFQ_QUANTA;

		// HD_CFQ_EVENT: HD CFQ event behavior. Upon completion of its time quanta, the request will be enqueued back to the queue.
		delayHelper(newEventTime, remainingIoOperationSize, transferSpeed, HD_CFQ_EVENT, ev, outputEventType);
	}
	else{	// the last i/o operation. no need to enqueue back to the waiting queue. proceed to the next intended node event (i.e. read / write complete)
		hdCFQdata recoveredCFQdata =  getHDrequest(ev->getEntityIns().getEntityId());
		Event newEvent(recoveredCFQdata.applicationId_, recoveredCFQdata.eventTime_, recoveredCFQdata.neededResourceQuantity_, recoveredCFQdata.seizedResQuantity_, recoveredCFQdata.nextEvent_,
				recoveredCFQdata.destinationEventType_, recoveredCFQdata.eventBehavior_, recoveredCFQdata.entityType_, SEIZETOMASTER, recoveredCFQdata.attribute_, recoveredCFQdata.fsLoc_,
				recoveredCFQdata.fsId_, recoveredCFQdata.redID_, recoveredCFQdata.recordID_, recoveredCFQdata.spillTally_);

		// copy original entityId
		newEvent.setEntityId(recoveredCFQdata.entityId_);
		delayHelper(newEventTime, recoveredCFQdata.neededResourceQuantity_, transferSpeed, recoveredCFQdata.completionEvBeh, &newEvent, outputEventType);
		eraseHDrequest(ev->getEntityIns().getEntityId());
	}
}

#endif
