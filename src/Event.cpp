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

#include "Event.h"
#include <iostream>

/*
 * neededResQuantity: requested resource amount by the current event.
 * seizedResQuantity: shows the seized resource amount on the module that gets the event
 */

Event::Event(size_t applicationID, double eventTime, double neededResQuantity, double seizedResQuantity, int nextEventType,  int destinationEventType,
		 int eventBehavior, enum EntityType entityType, enum LinkEventBehavior linkBehavior, int attribute, int fsLoc, int fsID, int redID, int recordID, size_t spillTally):
				applicationID_(applicationID), eventTime_(eventTime), neededResQuantity_(neededResQuantity), seizedResQuantity_(seizedResQuantity),
				nextEventType_(nextEventType), destinationEventType_(destinationEventType), eventBehavior_(eventBehavior), linkBehavior_ (linkBehavior),
				entityIns_(entityType, attribute), fsLoc_(fsLoc), fsID_(fsID), redID_(redID), recordID_(recordID), spillTally_(spillTally){
}

Event::Event(const Event& other):
	applicationID_(other.applicationID_),
	eventTime_(other.eventTime_),
	neededResQuantity_(other.neededResQuantity_),
	seizedResQuantity_(other.seizedResQuantity_),
	nextEventType_(other.nextEventType_),
	destinationEventType_(other.destinationEventType_),
	eventBehavior_(other.eventBehavior_),
	linkBehavior_ (other.linkBehavior_),
	entityIns_(other.entityIns_.getEntityType(), other.entityIns_.getAttribute()),
	fsLoc_(other.fsLoc_),
	fsID_(other.fsID_),
	redID_(other.redID_),
	recordID_(other.recordID_),
	spillTally_(other.spillTally_){

	setEntityId(other.entityIns_.getEntityId());
}

Event& Event::operator=(const Event& other){
	applicationID_ = other.applicationID_;
	eventTime_ = other.eventTime_;
	neededResQuantity_ = other.neededResQuantity_;
	seizedResQuantity_ = other.seizedResQuantity_;
	nextEventType_ =other.nextEventType_;
	destinationEventType_ = other.destinationEventType_;
	eventBehavior_ = other.eventBehavior_;
	linkBehavior_  = other.linkBehavior_;
	entityIns_.setAttribute(other.entityIns_.getAttribute());
	entityIns_.setEntityType(other.entityIns_.getEntityType());
	fsLoc_ = other.fsLoc_;
	fsID_ = other.fsID_;
	redID_ = other.redID_;
	recordID_ = other.recordID_;
	spillTally_ = other.spillTally_;

	setEntityId(other.entityIns_.getEntityId());
    return *this;
}

Event::~Event() {
}

double Event::getEventTime() const {
	return eventTime_;
}

void Event::setEntityInsAttribute(const int attrVal) {
	entityIns_.setAttribute(attrVal);
}

const Entity& Event::getEntityIns() const {
	return entityIns_;
}

int Event::getEventBehavior() const {
	return eventBehavior_;
}

void Event::setEventBehavior(int eventBehavior) {
	this->eventBehavior_ = eventBehavior;
}

double Event::getNeededResQuantity() const {
	return neededResQuantity_;
}

double Event::getSeizedResQuantity() const {
	return seizedResQuantity_;
}

void Event::setSeizedResQuantity(double seizedResQuantity) {
	seizedResQuantity_ = seizedResQuantity;
}

size_t Event::getAppID() const {
	return applicationID_;
}

int Event::getDestEventType() const {
	return destinationEventType_;
}

int Event::getNextEventType() const {
	return nextEventType_;
}

enum LinkEventBehavior Event::getLinkBehavior() const {
	return linkBehavior_;
}

int Event::getFsLoc() const {
	return fsLoc_;
}

int Event::getFsId() const {
	return fsID_;
}

void Event::setNeededResQuantity(double neededResQuantity) {
	neededResQuantity_ = neededResQuantity;
}

void Event::setEventTime(double eventTime) {
	eventTime_ = eventTime;
}

int Event::getRedId() const {
	return redID_;
}

void Event::setEntityId(size_t entityId){
	entityIns_.overwriteEntityId(entityId);
}

void Event::incGlobalEntityId(){
	entityIns_.incId();
	entityIns_.overwriteEntityId(entityIns_.getId());
}

int Event::getRecordId() const {
	return recordID_;
}

void Event::setRecordId(int recordId) {
	recordID_ = recordId;
}

void Event::setRedId(int redId) {
	redID_ = redId;
}

size_t Event::getSpillTally() const {
	return spillTally_;
}

void Event::setSpillTally(size_t spillTally) {
	spillTally_ = spillTally;
}

void Event::setNextEventType(int nextEventType) {
	nextEventType_ = nextEventType;
}
