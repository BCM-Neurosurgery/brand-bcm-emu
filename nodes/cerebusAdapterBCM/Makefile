PROJECT=cerebusAdapterBCM

# pycbsdk requires Python >=3.9; the standard `rt` env is pinned to 3.8 for
# its Cython/gcc node-build toolchain, so this node has its own env instead.
# See CLAUDE.md for the full story and what's installed in nsp_env.
ifneq ($(CONDA_DEFAULT_ENV),nsp_env)
$(error nsp_env conda env not active -- this node depends on pycbsdk, redis, and brand, all installed there (not rt); see CLAUDE.md)
endif

ROOT ?= ../../../..
include $(ROOT)/setenv.mk

PYTHON_VERSION := $(shell python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PYTHON_LIB=python$(PYTHON_VERSION)

LIBPYTHON=$(CONDA_PREFIX)/lib/
INCPYTHON=$(CONDA_PREFIX)/include/$(PYTHON_LIB)

TARGET=$(PROJECT).bin
CYTHON_TARGET=$(PROJECT).c

$(TARGET): $(PROJECT).py
	cp $(PROJECT).py $(PROJECT).pyx
	cython --embed $(PROJECT).pyx -o $(CYTHON_TARGET)
	gcc $(CYTHON_TARGET) -o $(TARGET) -I $(INCPYTHON) -L $(LIBPYTHON) -Wl,-rpath=$(LIBPYTHON) -l$(PYTHON_LIB) -lpthread -lm -lutil -ldl
	$(RM) $(PROJECT).pyx $(CYTHON_TARGET)
clean:
	$(RM) $(TARGET) $(PROJECT).pyx $(CYTHON_TARGET)
