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

#include <iostream>
#include "Switch.h"

std::map<int, int> switchExpEventT_masterLinkExpEventT;		// a mapping for expected event types

Switch::Switch(int expectedEventType, int masterLinkType):
			Module (expectedEventType, SWITCH),
			masterLinkType_(masterLinkType){

	if(masterLinkType_!=-1){
		switchExpEventT_masterLinkExpEventT[expectedEventType]=masterLinkType;
	}
}

Switch::~Switch() {
}

// NOTE: no delay in switch. Consider adding a short processing delay here...
void Switch::work (Event* ev){

	if(ev->getLinkBehavior() == SEIZETOMASTER){	// to master

		if(masterLinkType_ == -1){	// AGGREGATION SWITCH

			int nextSwitchExpEvent = nodeExpEventT_switchExpEventT[ev->getDestinationEventType()];
			int nextEventType =  switchExpEventT_masterLinkExpEventT[nextSwitchExpEvent];

			// generate POST-LINK EVENT, enqueue to eventsList
			Event newEvent(ev->getApplicationId(), ev->getEventTime(), ev->getNeededResQuantity(), 0.0, nextEventType, ev->getDestinationEventType(),
					   ev->getEventBehavior(), ev->getEntityIns().getEntityType(), SEIZEFROMMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());
			eventsList.push(newEvent);
		}
		else{	// TOR SWITCH

			if(nodeExpEventT_switchExpEventT[ev->getDestinationEventType()] == expectedEventType_){	// Destination Node is within the rack of this TOR SWITCH
				int nextEventType = nodeExpEventT_linkExpEventT[ev->getDestinationEventType()];
				// generate POST-LINK EVENT, enqueue to eventsList
				Event newEvent(ev->getApplicationId(), ev->getEventTime(), ev->getNeededResQuantity(), 0.0, nextEventType, ev->getDestinationEventType(),
						   ev->getEventBehavior(), ev->getEntityIns().getEntityType(), SEIZEFROMMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());
				eventsList.push(newEvent);
			}
			else{

				// generate POST-LINK EVENT, enqueue to eventsList
				Event newEvent(ev->getApplicationId(), ev->getEventTime(), ev->getNeededResQuantity(), 0.0, masterLinkType_, ev->getDestinationEventType(),
						   ev->getEventBehavior(), ev->getEntityIns().getEntityType(), SEIZETOMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());
				eventsList.push(newEvent);
			}
		}
	}
	else{	// from master

		int nextEventType = nodeExpEventT_linkExpEventT[ev->getDestinationEventType()];
		// generate POST-LINK EVENT, enqueue to eventsList
		Event newEvent(ev->getApplicationId(), ev->getEventTime(), ev->getNeededResQuantity(), 0.0, nextEventType, ev->getDestinationEventType(),
				   ev->getEventBehavior(), ev->getEntityIns().getEntityType(), SEIZEFROMMASTER, ev->getEntityIns().getAttribute(), ev->getFsLoc(), ev->getFsId(), ev->getRedId());
		eventsList.push(newEvent);
	}
}

int Switch::getExpectedEventType() const {
	return expectedEventType_;
}
