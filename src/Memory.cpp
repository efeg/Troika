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

#include "Memory.h"
#include <random>
#include <chrono>
#include <iostream>

Memory::Memory(size_t maxReadSpeed, size_t maxWriteSpeed, size_t minReadSpeed, size_t minWriteSpeed, size_t remainingCapacity, enum DistributionType delayType, enum TimeType unit,
		  double delayratio):
		  remainingCapacity_(remainingCapacity),
		  delayType_(delayType),
		  unit_(unit),
		  delayratio_(delayratio){

	// a random generator engine from a real time-based seed:
	unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
	std::default_random_engine generator (seed);
	std::uniform_real_distribution<double> 	readDist (0, (maxReadSpeed - minReadSpeed));
	std::uniform_real_distribution<double> writeDist (0, (maxWriteSpeed - minWriteSpeed));
	// time is updated here
	remainingReadCapacity_ = readDist(generator);
	remainingWriteCapacity_ = writeDist(generator);
}

Memory::~Memory() {
}

void Memory::work (Event* ev){
}

void Memory::delay (Event ev, double currentTime, int transferSpeed){
	// a random generator engine from a real time-based seed:
	unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
	std::default_random_engine generator (seed);
	double baseTransferTime = ev.getNeededResQuantity() / transferSpeed;
	double newEventTime = currentTime + baseTransferTime;
	double high = baseTransferTime * delayratio_;

	switch (delayType_){

		case UNIFORM:{
			// Adjust time units to be stored as seconds in simulation time
			if (unit_ == MINUTES){
				high *= MINUTE_IN_SEC;
			}
			else if(unit_ == HOURS){
				high *= HOUR_IN_SEC;
			}

			std::uniform_real_distribution<double> distribution (0, high);
			// time is updated here
			newEventTime += distribution(generator);

			break;}
		case EXPONENTIAL:{
			std::exponential_distribution<double> exponential(high*EXP_MEM_DELAY_CONSTANT);

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
				newEventTime += MINUTE_IN_SEC*high;
			}
			else if(unit_ == HOURS){
				newEventTime += HOUR_IN_SEC*high;
			}
			else{
				newEventTime += high;
			}
			break;}
		default:{
			std::cout << "Incorrect delay type!" << std::endl;
			exit(-1);
			break;}
	}
}

void Memory::setRemainingCapacity(size_t remainingCapacity) {	// after subtracting used resources for daemons
	remainingCapacity_ = remainingCapacity;
}

size_t Memory::getRemainingCapacity() const {
	return remainingCapacity_;
}

double Memory::getResetDataWaitingForSpill(int redID, int appID) {
	double temp = dataWaitingForSpill_[std::make_pair(redID,appID)];
	dataWaitingForSpill_[std::make_pair(redID,appID)] = 0;
	numberOfMapOutputsWaitingForSpill_[std::make_pair(redID,appID)] = 0;
	return temp;
}

bool Memory::addCheckMergeSpill(double dataSize, size_t shuffleMergeLimit, int redID, int appID){

	if(dataWaitingForSpill_.find(std::make_pair(redID,appID)) == dataWaitingForSpill_.end()){	// not found
		dataWaitingForSpill_[std::make_pair(redID,appID)] = dataSize;
		numberOfMapOutputsWaitingForSpill_[std::make_pair(redID,appID)] = 1;
	}
	else{
		dataWaitingForSpill_[std::make_pair(redID,appID)] += dataSize;
		numberOfMapOutputsWaitingForSpill_[std::make_pair(redID,appID)]+=1;
	}
	if(dataWaitingForSpill_[std::make_pair(redID,appID)] > (double)shuffleMergeLimit){
		return true;
	}
	return false;
}

int Memory::getnumberOfMapOutputsWaitingForSpill(int redID, int appID){
	return numberOfMapOutputsWaitingForSpill_[std::make_pair(redID,appID)];
}
