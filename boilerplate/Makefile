obj-m += monitor.o

KDIR := /lib/modules/$(shell uname -r)/build
PWD := $(shell pwd)
KERNEL_USES_CLANG := $(shell [ -f "$(KDIR)/.config" ] && grep -q '^CONFIG_CC_IS_CLANG=y' "$(KDIR)/.config" && echo 1 || echo 0)
ifeq ($(KERNEL_USES_CLANG),1)
KERNEL_BUILD_ARGS := LLVM=1
endif
# If you want host-built workload binaries to run directly inside an Alpine
# rootfs, you can override this with WORKLOAD_LDFLAGS=-static when your
# toolchain supports it.
WORKLOAD_LDFLAGS ?= -static

USER_TARGETS := engine memory_hog cpu_hog io_pulse

all: $(USER_TARGETS) module

ci: WORKLOAD_LDFLAGS =
ci: $(USER_TARGETS)

module: monitor.ko

engine: engine.c monitor_ioctl.h
	gcc -O2 -Wall -Wextra -o engine engine.c -lpthread

memory_hog: memory_hog.c
	gcc -O2 -Wall $(WORKLOAD_LDFLAGS) -o memory_hog memory_hog.c

cpu_hog: cpu_hog.c
	gcc -O2 -Wall $(WORKLOAD_LDFLAGS) -o cpu_hog cpu_hog.c

io_pulse: io_pulse.c
	gcc -O2 -Wall $(WORKLOAD_LDFLAGS) -o io_pulse io_pulse.c

monitor.ko: monitor.c monitor_ioctl.h
	$(MAKE) -C $(KDIR) M=$(PWD) $(KERNEL_BUILD_ARGS) modules

clean:
	if [ -d "$(KDIR)" ]; then $(MAKE) -C $(KDIR) M=$(PWD) $(KERNEL_BUILD_ARGS) clean; fi
	rm -f $(USER_TARGETS) *.o *.mod *.mod.c *.symvers *.order
	rm -f *.log
	rm -rf logs
	rm -f /tmp/mini_runtime.sock

.PHONY: all ci module clean
