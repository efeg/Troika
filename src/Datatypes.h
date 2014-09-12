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

#ifndef DATATYPES_H_
#define DATATYPES_H_

// Disable terminal log messages
#define TERMINAL_LOG_DISABLED

#define HOUR_IN_SEC 3600
#define SECOND_IN_MS 1000
#define MINUTE_IN_SEC 60
#define HEARTBEAT_SIZE 1000						// Default heartbeat size in bytes
#define YARN_NODEMANAGER_HEARTBEAT_INTERVALMS 1000	// Heartbeat interval to RM

enum ModuleType{
	NODE=1,
	LINK=2,
	SWITCH=3
};

enum NodeType{
	CLIENT=1,
	RESOURCEMANAGER=2,
	NODEMANAGER=3,
	OTHERNODE=4
};

enum LinkEventBehavior{
	SEIZETOMASTER=1,
	SEIZEFROMMASTER=2,
	RELEASETOMASTER=3,
	RELEASEFROMMASTER=4
};

enum ResourceScheduler{
	CAPACITY=1,
	FAIR=2,
	CUSTOM=3
};

enum DistributionType{
	UNIFORM=1,
	EXPONENTIAL=2,
	CONSTANT=3
};

enum TimeType{
	SECONDS=1,
	MINUTES=2,
	HOURS=3
};

enum EntityType{
	APPLICATION=1,
	MAPTASK=2,
	REDUCETASK=3,
	OTHER=4
};

#endif /* DATATYPES_H_ */
