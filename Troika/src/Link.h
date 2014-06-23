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

#ifndef LINK_H_
#define LINK_H_

#include <queue>
#include <memory>
#include "Module.h"
#include "EventTimeCompare.h"
#define INITIAL_NOFAILURE_LINK 1000000
#define EXP_NW_DELAY_CONSTANT 3			// this is the delay constant, increase to lower the exponential delay (if selected delay type is exponential)

extern std::priority_queue<Event, std::vector<Event>, EventTimeCompare > eventsList;

// Link module is a specialized module for connection between machines
class Link: public Module {
public:

	Link(double, int, int, int, double mttr=0, int mtbf=0,
			enum DistributionType delayType=EXPONENTIAL, enum TimeType unit=SECONDS, double delayratio=0.01);
	virtual ~Link();
	void work (Event*);
	int getExpectedEventType() const;
	double getTotalCapacity() const;

private:
	double totalCapacity_;
	enum DistributionType delayType_;
	enum TimeType unit_;
	int masterEventType_, workerEventType_;
	double mttr_;
	int mtbf_;
	double delayratio_;
	int totalPacketProcessed_;	// count starting from
	double totalSizeInWaitingBand0Queue_;	// this is waiting in band0
	double totalSizeInWaitingBand1Queue_;	// this is waiting in band1
	double calculateTransferSpeed(Event* ev);
	void delay (Event, double, int);
	void activateWaitingEventInQueue(double currentTime);
	enum LinkEventBehavior getReleaseBehavior(enum LinkEventBehavior behavior);

	// band0 queue has priority over band1 queue.
	// the scheduling approach is similar to pfifo_fast with 2 bands.
	// band0 is used by heartbeat messages.
	// band1 is used by other regular messages.
	std::queue<Event> band0Queue_;	// events wait in this Heartbeat FIFO queue in case of insufficient resources
	std::queue<Event> band1Queue_;	// events wait in this FIFO queue in case of insufficient resources
	bool activeTransfer_;			// if there is an ongoing transfer on the link add the packet to the queue
};

#endif /* LINK_H_ */
