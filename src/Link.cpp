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

#include "Link.h"
#include <random>
#include <chrono>
#include <iostream>

std::map<int, int> linkExpEventT_capacity;				// a mapping for expected event types of link and total capacity of link

/*
	A Link is a connection between two machines namely a master and a worker
	capacity: 			total upstream = downstream bandwidth in bytes
	delayType: 			type of distribution used for calculation of the approximate delay
	unit: 				time unit of the simulation (default is second)
	expectedEventType: 	module's event type
	masterEventType: 	event type of master end of the link
	workerEventType:	event type of worker end of the link
	mttr:				mean time to repair
	mtbf:				mean time between failures
	delayratio:			ratio of total delay (high in unreliable networks) a number between 0 - 1
*/

Link::Link(double capacity, int expectedEventType, int masterEventType, int workerEventType, double mttr,
		int mtbf, enum DistributionType delayType, enum TimeType unit, double delayratio)
		: Module(expectedEventType, LINK),
		totalCapacity_(capacity), delayType_(delayType), unit_(unit), masterEventType_(masterEventType),
		workerEventType_(workerEventType), mttr_(mttr), mtbf_(mtbf), delayratio_(delayratio),
		totalPacketProcessed_(0), totalSizeInWaitingBand0Queue_(0.0), totalSizeInWaitingBand1Queue_(0.0),
		activeTransfer_(false){
	linkExpEventT_capacity[expectedEventType] = totalCapacity_;
}

Link::~Link() {
}

/*
 * One transfer at a time...
 * the transferred data gets the whole bandwidth or if there
 * is an active transfer, it is added to the queue
 */
double Link::calculateTransferSpeed(Event* ev){
	if(activeTransfer_){
		// enqueue event (when a resource is released queue will be checked)
		Event newEvent(ev->getAppID(), ev->getEventTime(), ev->getNeededResQuantity(), 0, ev->getNextEventType(), ev->getDestEventType(),
				ev->getEventBehavior(), ev->getEntityIns().getEntityType(), ev->getLinkBehavior(), ev->getEntityIns().getAttribute(), ev->getFsLoc(),
				ev->getFsId(), ev->getRedId(), ev->getRecordId());

		if (ev->getNeededResQuantity() <= HEARTBEAT_SIZE){ // size of the transfered file is less than or equal to HEARTBEAT data size.
			/*add to band0queue */
			band0Queue_.push(newEvent);

			// add waiting size to totalSizeInWaitingBand0Queue_
			totalSizeInWaitingBand0Queue_ += ev->getNeededResQuantity();
		}
		else{
			/*add to band1queue */
			band1Queue_.push(newEvent);

			// add waiting size to totalSizeInWaitingBand0Queue_
			totalSizeInWaitingBand1Queue_ += ev->getNeededResQuantity();
		}
		return 0;
	}
	// the link is seized for transfer
	activeTransfer_ = true;
	return totalCapacity_;
}

void Link::activateWaitingEventInQueue(double currentTime){
	// check whether there is any waiting event in band0queue
	if (!band0Queue_.empty()){
		int transferSpeed = calculateTransferSpeed(&band0Queue_.front());
		delay(band0Queue_.front(), currentTime, transferSpeed);

		// sub waiting size to totalSizeInWaitingBand0Queue_
		totalSizeInWaitingBand0Queue_ -= band0Queue_.front().getNeededResQuantity();
		band0Queue_.pop();
	}
	// check whether there is any waiting event in band1queue
	else if (!band1Queue_.empty()){
		int transferSpeed = calculateTransferSpeed(&band1Queue_.front());
		delay(band1Queue_.front(), currentTime, transferSpeed);

		// sub waiting size to totalSizeInWaitingBand0Queue_
		totalSizeInWaitingBand1Queue_ -= band1Queue_.front().getNeededResQuantity();
		band1Queue_.pop();
	}
	else{
		// no waiting request in either bands! NOOP
	}
}

/*
 * eventtype is either master, worker or the current link itself
 * ev->getLinkBehavior() determines if it is going to master or worker and seize or release the bandwidth
 *
 */

void Link::work (Event* ev){
	if(ev->getLinkBehavior() == SEIZETOMASTER || ev->getLinkBehavior() == SEIZEFROMMASTER){
		/* If the returned value is 0, this means that currently
		 * there is an active transfer so the request is queued
		 * wait for the link to release the seized resource.*/
		int transferSpeed = calculateTransferSpeed(ev);
		if(transferSpeed){	// nonzero transfer speed means the link is acquired
			delay(*ev, ev->getEventTime(), transferSpeed);
		}
	}
	else if(ev->getLinkBehavior() == RELEASETOMASTER){
		// The link is released for completion of transfer
		activeTransfer_ = false;
		// generate POST-LINK EVENT, enqueue to eventsList
		Event newEvent(ev->getAppID(), ev->getEventTime(), ev->getNeededResQuantity(), 0.0, masterEventType_, ev->getDestEventType(),
				   ev->getEventBehavior(), ev->getEntityIns().getEntityType(), SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId(), ev->getRecordId());
		eventsList.push(newEvent);

		// activate if there is any waiting event in either band0 or band1
		activateWaitingEventInQueue(ev->getEventTime());
	}
	else if(ev->getLinkBehavior() == RELEASEFROMMASTER){
		// The link is released for completion of transfer
		activeTransfer_ = false;
		// generate POST-LINK EVENT, enqueue to eventsList
		Event newEvent(ev->getAppID(), ev->getEventTime(), ev->getNeededResQuantity(), 0.0, workerEventType_, ev->getDestEventType(),
				   ev->getEventBehavior(), ev->getEntityIns().getEntityType(), SEIZEFROMMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId(), ev->getRecordId());
		eventsList.push(newEvent);

		// activate if there is any waiting event in either band0 or band1
		activateWaitingEventInQueue(ev->getEventTime());
	}
	else{
		std::cerr << "Corrupted Link Behavior!" << std::endl;
		exit(-1);
	}
}

void Link::delay (Event ev, double currentTime, int transferSpeed){
	// a random generator engine from a real time-based seed:
	unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
	std::default_random_engine generator (seed);
	double baseTransferTime = ev.getNeededResQuantity() / transferSpeed;
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

			// generate RELEASE event, enqueue to eventsList
			Event newEvent(ev.getAppID(), newEventTime, ev.getNeededResQuantity(), ev.getSeizedResQuantity(), ev.getNextEventType(), ev.getDestEventType(),
					ev.getEventBehavior(), ev.getEntityIns().getEntityType(), getReleaseBehavior(ev.getLinkBehavior()), ev.getEntityIns().getAttribute(), ev.getFsLoc(),
					ev.getFsId(), ev.getRedId(), ev.getRecordId());

			eventsList.push(newEvent);

			break;}
		case EXPONENTIAL:{

			std::exponential_distribution<double> exponential(EXP_NW_DELAY_CONSTANT/additionalDelay);

			if (unit_ == MINUTES){
				newEventTime += MINUTE_IN_SEC*exponential(generator);
			}
			else if(unit_ == HOURS){
				newEventTime += HOUR_IN_SEC*exponential(generator);
			}
			else{
				newEventTime += exponential(generator);
			}
			// generate RELEASE event, enqueue to eventsList
			Event newEvent(ev.getAppID(), newEventTime, ev.getNeededResQuantity(), ev.getSeizedResQuantity(), ev.getNextEventType(), ev.getDestEventType(),
					ev.getEventBehavior(), ev.getEntityIns().getEntityType(), getReleaseBehavior(ev.getLinkBehavior()), ev.getEntityIns().getAttribute(), ev.getFsLoc(),
					ev.getFsId(), ev.getRedId(), ev.getRecordId());

			eventsList.push(newEvent);

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

			// generate RELEASE event, enqueue to eventsList
			Event newEvent(ev.getAppID(), newEventTime, ev.getNeededResQuantity(), ev.getSeizedResQuantity(), ev.getNextEventType(), ev.getDestEventType(),
					ev.getEventBehavior(), ev.getEntityIns().getEntityType(), getReleaseBehavior(ev.getLinkBehavior()), ev.getEntityIns().getAttribute(), ev.getFsLoc(),
					ev.getFsId(), ev.getRedId(), ev.getRecordId());

			eventsList.push(newEvent);

			break;}
		default:{
			std::cerr << "Incorrect delay type!" << std::endl;
			exit(-1);
			break;}
	}
}

int Link::getExpectedEventType() const {
	return expectedEventType_;
}

double Link::getTotalCapacity() const {
	return totalCapacity_;
}

enum LinkEventBehavior Link::getReleaseBehavior(enum LinkEventBehavior behavior){
	if(behavior == SEIZETOMASTER){
		return RELEASETOMASTER;
	}
	return RELEASEFROMMASTER;
}
