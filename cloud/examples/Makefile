include ../../make_config.mk

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += "-DROCKSDB_JEMALLOC"
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

.PHONY: clean librocksdb

all: cloud_durable_example clone_example cloud_dump

cloud_durable_example: librocksdb cloud_durable_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o $@ ../../librocksdb.a -I../../include $(OPT) -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS) -Wno-non-virtual-dtor  -Wno-overloaded-virtual -Wno-error=implicit-function-declaration -I/root/install/usr/local/include -L/root/install/usr/local/lib -laws-cpp-sdk-core -laws-cpp-sdk-s3 -laws-cpp-sdk-kinesis  -laws-cpp-sdk-transfer -DUSE_AWS

clone_example: librocksdb clone_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o $@ ../../librocksdb.a -I../../include $(OPT) -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

cloud_dump: librocksdb cloud_dump.cc
	$(CXX) $(CXXFLAGS) $@.cc -o $@ ../../librocksdb.a -I../../include $(OPT) -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf ./cloud_durable_example  ./clone_example ./cloud_dump

librocksdb:
	cd ../.. && $(MAKE) static_lib
