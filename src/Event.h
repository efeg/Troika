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

#ifndef EVENT_H_
#define EVENT_H_

#include "Entity.h"

class Event {
public:

	Event(size_t, double, double, double, int, int, int eventBehavior = 0, enum EntityType entityType=OTHER,
			enum LinkEventBehavior linkBehavior = SEIZETOMASTER,  int attribute=0, int fsLoc=-1, int fsID=-1, int redID=-1, int recordID=-1, size_t spillTally=1);

	// copy constructor
	Event(const Event& other);

	// assignment
	Event& operator=( const Event& rhs );

	virtual ~Event();

	double getEventTime() const;

	const Entity& getEntityIns() const;

	void setEntityInsAttribute(const int);

	int getEventBehavior() const;

	void setEventBehavior(int eventBehavior);

	double getNeededResQuantity() const;

	double getSeizedResQuantity() const;

	void setSeizedResQuantity(double seizedResQuantity);

	size_t getApplicationId() const;

	int getDestinationEventType() const;

	int getNextEventType() const;

	enum LinkEventBehavior getLinkBehavior() const;

	int getFsLoc() const;

	int getFsId() const;

	void setNeededResQuantity(double neededResQuantity);

	void setEventTime(double eventTime);

	int getRedId() const;

	void setEntityId(size_t entityId);

	int getRecordId() const;

	void setRecordId(int recordId);

	void setRedId(int redId);

	void incGlobalEntityId();

	size_t getSpillTally() const;

	void setSpillTally(size_t spillTally);

	void setNextEventType(int nextEventType);

private:
	size_t applicationID_;
	double eventTime_;
	double neededResQuantity_;
	double seizedResQuantity_;
	int nextEventType_;  // is it an arrival? departure? etc? which module is it heading to?
	int destinationEventType_;
	int eventBehavior_;
	enum LinkEventBehavior linkBehavior_;
	Entity entityIns_;
	int fsLoc_;
	int fsID_;
	int redID_;
	int recordID_;
	size_t spillTally_;
};

#endif /* EVENT_H_ */
