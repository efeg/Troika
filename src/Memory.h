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

#ifndef MEMORY_H_
#define MEMORY_H_

#include <memory>
#include <map>
#include "EventTimeCompare.h"
#define EXP_MEM_DELAY_CONSTANT 3			// this is the delay constant, increase to lower the exponential delay (if selected delay type is exponential)

class Memory{
public:
	Memory(size_t, size_t, size_t, size_t, size_t, enum DistributionType delayType=EXPONENTIAL, enum TimeType unit=SECONDS, double delayratio=0.01);
	virtual ~Memory();
	void work (Event*);

	void setRemainingCapacity(size_t remainingCapacity);

	size_t getRemainingCapacity() const;

	bool addCheckMergeSpill(double dataSize, size_t shuffleMergeLimit, int redID, int appID);

	double getResetDataWaitingForSpill(int redID, int appID);

	int getnumberOfMapOutputsWaitingForSpill(int redID, int appID);

private:
	size_t remainingCapacity_;			// total amount of physical memory that can be used
	double remainingReadCapacity_, remainingWriteCapacity_;
	enum DistributionType delayType_;
	enum TimeType unit_;
	double delayratio_;
	std::map<std::pair<int,int>,double> dataWaitingForSpill_;
	std::map<std::pair<int,int>,int> numberOfMapOutputsWaitingForSpill_;	// redID vs numMapOutputs waiting for spill
	std::queue<Event> waitingQueue_;	// events wait in this FIFO queue in case of insufficient resources
	void delay (Event, double, int);
};

#endif /* MEMORY_H_ */
