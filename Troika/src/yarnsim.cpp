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

//============================================================================
// Name        : Troika Simulator
// Version     : v0.1 alpha
// Copyright   : Copyright (c) 2014, Cornell University
// Description : TROIKA - Simulator Component
//============================================================================

#include <iostream>
#include <cstdlib>
#include <string>
#include "LogMessages.h"
#include "Init.h"

using namespace std;
priority_queue<Event, vector<Event>, EventTimeCompare > eventsList;
vector<std::shared_ptr<Application>> applications;

double simulationTime = 0;	// simulation time

int main(int argc, char* argv[]) {

	double endingCondition;   // time limit of simulation
	vector<std::shared_ptr<Module>> eventExpectingModules;

	Init initializer;
	initializer.initDES(&endingCondition, argc, argv, eventExpectingModules, applications);

	// ------------------------------------------------------------------------------------------

	// loop until ending time is exceeded or events list is empty
	while(true){
		// If events list is empty terminate
		if(eventsList.empty()){

			if(!terminal_output_disabled){
				cout << "Events list is empty. Simulation completed successfully." << endl;
			}
			break;
		}
		// Get next event from events list
		Event currentEvent = eventsList.top();
		eventsList.pop();	// remove from the events list
		// Advance the simulation clock to the
        // time of the most imminent event
		simulationTime = currentEvent.getEventTime();

		// ending condition
        if (simulationTime > endingCondition){
			cout << "Ending condition is satisfied. Simulation completed successfully." << endl;
            break;
        }
        // check the eventType and find the correct module waiting for it.
        std::shared_ptr<Module> currentModule(nullptr);
        for(unsigned int i=0; i < eventExpectingModules.size();i++){
        	if ( (eventExpectingModules.at(i))->getExpectedEventType() ==  currentEvent.getNextEventType()){
        		currentModule = (eventExpectingModules.at(i));
        		break;
        	}
        }

        if (!currentModule){	// Module that expects the generated event does not exist!
        	cout << "Unexpected Event with no matched Module! Event type: " << currentEvent.getNextEventType()<< endl;
        	exit(-1);
        }

        printModuleName(currentModule->getModuleId());

        currentModule->work(&currentEvent);
	}
	return 0;
}
