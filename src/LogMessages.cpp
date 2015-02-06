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

#include "LogMessages.h"
#include <iostream>

const std::string PRINT_UNDER = "______________________________________\n|__________in ";

void printModuleName(enum ModuleType moduleID){

	#ifndef TERMINAL_LOG_DISABLED
	switch(moduleID){
	case NODE: std::cout <<  PRINT_UNDER <<       "[INFO] NODE module___________|" << std::endl; break;
	case LINK: std::cout <<  PRINT_UNDER <<       "[INFO] LINK module___________|" << std::endl; break;
	case SWITCH: std::cout <<  PRINT_UNDER <<     "[INFO] SWITCH module_________|" << std::endl; break;
	default: std::cout <<  PRINT_UNDER <<         "[INFO] UNRECOGNIZED module___|" << std::endl; break;
	}
	#endif
}

void printNodeMessages(enum NodeType nodeType, int eventBehavior, size_t applicationId){

	#ifndef TERMINAL_LOG_DISABLED
	if(nodeType == CLIENT){
		switch(eventBehavior){
		case 1: std::cout <<   "[INFO] Step1: Run Application (in CLIENT - AppID: " << applicationId << ")" << std::endl; break;
		case 2: std::cout <<   "[INFO] Step3: Copy App Resources (in CLIENT - AppID: " <<applicationId << ")" << std::endl; break;
		default: std::cout <<  "[INFO] in CLIENT - UNRECOGNIZED Event Behavior" << std::endl; break;
		}
	}
	else if(nodeType == RESOURCEMANAGER){
		switch(eventBehavior){
		case 1: std::cout <<   "[INFO] Step2: Get Application ID (in RESOURCEMANAGER - AppID: " <<applicationId << ")" << std::endl; break;
		case 2: std::cout <<   "[INFO] Step4: Submit Application (in RESOURCEMANAGER - AppID: " <<applicationId << ")" << std::endl; break;
		case 3: std::cout <<   "[INFO] Step8: Allocate Resources for a Mapper (AppID: " <<applicationId << ")" << std::endl; break;
		case 4: std::cout <<   "[INFO] Step8: Allocate Resources for a Reducer (AppID: " <<applicationId << ")" << std::endl; break;
		default: std::cout <<  "[INFO] in RESOURCEMANAGER - UNRECOGNIZED Event Behavior" << std::endl; break;
		}
	}
	else if(nodeType == NODEMANAGER){
		switch(eventBehavior){
		case 1: std::cout <<   "[INFO] Step5a: Start Container (AppID: " <<applicationId << ")" << std::endl; break;
		case 2: std::cout <<   "[INFO] Step5b: Launch Application Master's Process (AppID: " <<applicationId << ")" << std::endl; break;
		case 3: std::cout <<   "[INFO] Step6: Initialize Application (AppID: " <<applicationId << ")" << std::endl; break;
		case 4: std::cout <<   "[INFO] Step7: Retrieve Input Splits (AppID: " <<applicationId << ")" << std::endl; break;
		case 5: std::cout <<   "[INFO] UBER TASK Execution Initiated (AppID: " <<applicationId << ")" << std::endl; break;
		case 6: std::cout <<   "[INFO] Step9a: Start Container for mapper (AppID: " <<applicationId << ")" << std::endl; break;
		case 7: std::cout <<   "[INFO] Step9a: Start Container for reducer(AppID: " <<applicationId << ")" << std::endl; break;
		case 8: std::cout <<   "[INFO] Step9b: Launch (AppID: " <<applicationId << ")" << std::endl; break;
		case 9: std::cout <<   "[INFO] Step10: Retrieve Application Resources (AppID: " <<applicationId << ")" << std::endl; break;
		case 10: std::cout <<  "[INFO] Step11: Run (AppID: " <<applicationId << ")" << std::endl; break;
		case 13: std::cout <<  "[INFO] Transfer request is sent to file split location (AppID: " <<applicationId << ")" << std::endl; break;
		default: std::cout <<  "[INFO] in NODEMANAGER - UNRECOGNIZED Event Behavior" << std::endl; break;
		}
	}
	else{
		std::cout <<  "[INFO] UNRECOGNIZED node type" << std::endl;
	}
	#endif

}
