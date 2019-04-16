################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../Memoria.c 

OBJS += \
./Memoria.o 

C_DEPS += \
./Memoria.d 


# Each subdirectory must supply rules for building sources it contributes
Memoria.o: ../Memoria.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/utnso/projects/Memoria/Debug/readline-2.0" -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"Memoria.d" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


