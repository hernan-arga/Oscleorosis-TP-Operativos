################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../readline-2.0/bind.c \
../readline-2.0/complete.c \
../readline-2.0/display.c \
../readline-2.0/emacs_keymap.c \
../readline-2.0/funmap.c \
../readline-2.0/history.c \
../readline-2.0/isearch.c \
../readline-2.0/keymaps.c \
../readline-2.0/parens.c \
../readline-2.0/readline.c \
../readline-2.0/rltty.c \
../readline-2.0/search.c \
../readline-2.0/signals.c \
../readline-2.0/tilde.c \
../readline-2.0/vi_keymap.c \
../readline-2.0/vi_mode.c \
../readline-2.0/xmalloc.c 

OBJS += \
./readline-2.0/bind.o \
./readline-2.0/complete.o \
./readline-2.0/display.o \
./readline-2.0/emacs_keymap.o \
./readline-2.0/funmap.o \
./readline-2.0/history.o \
./readline-2.0/isearch.o \
./readline-2.0/keymaps.o \
./readline-2.0/parens.o \
./readline-2.0/readline.o \
./readline-2.0/rltty.o \
./readline-2.0/search.o \
./readline-2.0/signals.o \
./readline-2.0/tilde.o \
./readline-2.0/vi_keymap.o \
./readline-2.0/vi_mode.o \
./readline-2.0/xmalloc.o 

C_DEPS += \
./readline-2.0/bind.d \
./readline-2.0/complete.d \
./readline-2.0/display.d \
./readline-2.0/emacs_keymap.d \
./readline-2.0/funmap.d \
./readline-2.0/history.d \
./readline-2.0/isearch.d \
./readline-2.0/keymaps.d \
./readline-2.0/parens.d \
./readline-2.0/readline.d \
./readline-2.0/rltty.d \
./readline-2.0/search.d \
./readline-2.0/signals.d \
./readline-2.0/tilde.d \
./readline-2.0/vi_keymap.d \
./readline-2.0/vi_mode.d \
./readline-2.0/xmalloc.d 


# Each subdirectory must supply rules for building sources it contributes
readline-2.0/%.o: ../readline-2.0/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


