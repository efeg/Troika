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

#ifndef HARDDISK_H_
#define HARDDISK_H_

#include <queue>
#include <memory>
#include <map>
#include <chrono>
#include "EventTimeCompare.h"
#define EXP_HD_DELAY_CONSTANT 3			// this is the delay constant, increase to lower the exponential delay (if selected delay type is exponential)

extern std::priority_queue<Event, std::vector<Event>, EventTimeCompare > eventsList;

// #define TWO_QUEUE_HD_SCHEDULER		// two separate queues for reduce spills and all other requests
#define CFQ_HD_SCHEDULER				// Linux CFQ scheduler
// #define FIFO_HD_SCHEDULER				// FIFO HD scheduler (noop)


#ifdef CFQ_HD_SCHEDULER

struct hdCFQdata{
	size_t applicationId_;
	double eventTime_;
	double neededResourceQuantity_;
	double seizedResQuantity_;
	int nextEvent_;
	int destinationEventType_;
	int eventBehavior_;
	enum EntityType entityType_;
	int attribute_;
	int fsLoc_;
	int fsId_;
	int redID_;
	int recordID_;
	size_t spillTally_;
	size_t entityId_;
	int completionEvBeh;	// next event behavior
};

struct waitingQueueElement{
	size_t applicationId_;
	double eventTime_;
	double neededResourceQuantity_;
	int outputEventType_;
	int destinationEventType_;
	int nextEvent_;
	size_t entityId_;
	enum EntityType entityType_;
	int attribute_;
	int fsLoc_;
	int fsId_;
	int redID_;
	int recordID_;
	size_t spillTally_;
	bool type_;	// true: read or false: write
	int ioStatus_;	// 0: first arrival, 1: not the last io operation, 2: last io read or write...
};

class Harddisk{
public:
	Harddisk(size_t, size_t, size_t, size_t, enum DistributionType delayType=EXPONENTIAL, enum TimeType unit=SECONDS, double delayratio=0.01);
	virtual ~Harddisk();
	double calculateWriteSpeed();
	double calculateReadSpeed();
	void work (int, double, Event*, int);
private:
	enum DistributionType delayType_;
	enum TimeType unit_;
	double delayratio_;
	int readCapInUsePercent_, writeCapInUsePercent_;
	double totalReadCapacity_, totalWriteCapacity_;
	double remainingReadCapacity_;				// read speed capacity
	double remainingWriteCapacity_;				// write speed capacity
	std::queue<waitingQueueElement> waitingQueue_;			// events wait in this FIFO queue in case of insufficient hd resources
	std::map<size_t, hdCFQdata> entityID_event_;
	std::map<int, size_t> key_limit_;
	void saveHDrequest(size_t entityID, Event* ev, int eventBehavior, double neededResourceQuantity);
	hdCFQdata getHDrequest(size_t entityID);
	void eraseHDrequest(size_t entityID);
	void readInterWork(double neededResourceQuantity, Event* ev, int outputEventType, int nextEvent, int queueID);
	void writeInterWork(double neededResourceQuantity, Event* ev, int outputEventType, int nextEvent, int queueID);
	bool incCheckForFsIdNextPost(int key, size_t limit);
	void checkWaitingQueue(Event* ev, int outputEventType);
	void delayHelper(double newEventTime, double neededResourceQuantity, double seizedResource, int eventBehaviorToBeGenerated, Event* ev, int outputEventType);
	void delay (double, double, double, Event*, int);
	void releaseReadResources(double seizedReadSpeedCapacity, Event* ev, int outputEventType, int op);
	void releaseWriteResources(double seizedWriteSpeedCapacity, Event* ev, int outputEventType, int op);
	void setWaitingEvent(struct waitingQueueElement *waitingEvent, size_t appId, double eventTime, double neededResourceQuantity, int destinationEventType, int nextEvent, size_t entityId,
	enum EntityType entityType, int attribute, int fsLoc, int fsId, int redID, int recordID, size_t spillTally);
	void releaseIOResources(double seizedSpeedCapacity, double* remainingIoCapacity, int* capInUsePercent, double totCapacity);
	int calculateIOSpeed(double *estimatedIOSpeed, int *capInUsePercent, double totIOcapacity);
};
#endif

#endif /* HARDDISK_H_ */
