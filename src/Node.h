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

#ifndef NODE_H_
#define NODE_H_

#include "Module.h"
#include "Cpu.h"
#include "Harddisk.h"
#include "Memory.h"
#include "Scheduler.h"

#define YARN_HEAPSIZE 1000	// (default) 1000 MB heapsize is allocated for each daemon (datanode and nodemanager) Note: no history server!

extern Scheduler g_scheduler;
extern bool terminal_output_disabled;

class Node: public Module {
public:
	Node(int, enum NodeType, int, const Cpu, const Harddisk, const Memory, int);

	virtual ~Node();

	void work (Event*);

	int getExpectedEventType() const;

	size_t getUniqueId();

private:

	bool isUberTask(Event*);
	static size_t ID_;
	static size_t uniqueID_;
	const size_t nodeID_;
	int rackExpectedEventType_;
	enum NodeType nodeType_;
	Cpu cpu_;
	Harddisk hd_;
	Memory memory_;
	int outputEventType_;    // the event that will be produced by this module
	void delay (Event ev);
	std::map<std::pair<int,int>, char> activeMappersInThisNode_;
	void testing(int numReduces, double eventTime, double avgMapTime, double avgRedTime, double shuffleTime, double mapTime);
	void recordSetter (size_t partSize, size_t recordSize, int taskID, bool isMapTask, Event* ev);
};

#endif /* NODE_H_ */
