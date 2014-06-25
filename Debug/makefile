# Copyright (c) 2014, Cornell University
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the <organization> nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

RM := rm -rf

# All of the sources participating in the build are defined here
-include sources.mk

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../src/Application.cpp \
../src/Cpu.cpp \
../src/Entity.cpp \
../src/Event.cpp \
../src/EventTimeCompare.cpp \
../src/Harddisk.cpp \
../src/Init.cpp \
../src/Link.cpp \
../src/LogMessages.cpp \
../src/MapReduceConf.cpp \
../src/Memory.cpp \
../src/Module.cpp \
../src/Node.cpp \
../src/Scheduler.cpp \
../src/Switch.cpp \
../src/yarnsim.cpp 

OBJS += \
./src/Application.o \
./src/Cpu.o \
./src/Entity.o \
./src/Event.o \
./src/EventTimeCompare.o \
./src/Harddisk.o \
./src/Init.o \
./src/Link.o \
./src/LogMessages.o \
./src/MapReduceConf.o \
./src/Memory.o \
./src/Module.o \
./src/Node.o \
./src/Scheduler.o \
./src/Switch.o \
./src/yarnsim.o 

CPP_DEPS += \
./src/Application.d \
./src/Cpu.d \
./src/Entity.d \
./src/Event.d \
./src/EventTimeCompare.d \
./src/Harddisk.d \
./src/Init.d \
./src/Link.d \
./src/LogMessages.d \
./src/MapReduceConf.d \
./src/Memory.d \
./src/Module.d \
./src/Node.d \
./src/Scheduler.d \
./src/Switch.d \
./src/yarnsim.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -std=c++11 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '

USER_OBJS :=
LIBS := -lboost_program_options

ifneq ($(MAKECMDGOALS),clean)
ifneq ($(strip $(C++_DEPS)),)
-include $(C++_DEPS)
endif
ifneq ($(strip $(C_DEPS)),)
-include $(C_DEPS)
endif
ifneq ($(strip $(CC_DEPS)),)
-include $(CC_DEPS)
endif
ifneq ($(strip $(CPP_DEPS)),)
-include $(CPP_DEPS)
endif
ifneq ($(strip $(CXX_DEPS)),)
-include $(CXX_DEPS)
endif
ifneq ($(strip $(C_UPPER_DEPS)),)
-include $(C_UPPER_DEPS)
endif
endif

# Add inputs and outputs from these tool invocations to the build variables 

# All Target
all: mkdir Troika

mkdir:
	mkdir -p src

# Tool invocations
Troika: $(OBJS) $(USER_OBJS)
	@echo 'Building target: $@'
	@echo 'Invoking: GCC C++ Linker'
	g++  -o "Troika" $(OBJS) $(USER_OBJS) $(LIBS)
	@echo 'Finished building target: $@'
	@echo ' '

# Other Targets
clean:
	-$(RM) $(OBJS)$(C++_DEPS)$(C_DEPS)$(CC_DEPS)$(CPP_DEPS)$(EXECUTABLES)$(CXX_DEPS)$(C_UPPER_DEPS) Troika test[0-9]*.txt
	-@echo ' '

.PHONY: all clean dependents
.SECONDARY:

