# Tide-CAS

## Overview

Tide-CAS is a high-performance Content Addressable Storage (CAS) solution designed to work with build systems that utilize the [Bazel Remote Execution API](https://github.com/bazelbuild/remote-apis). 
By implementing the CAS as defined in Bazel's remote execution API, Tide-CAS serves as an effective build cache, facilitating faster build times and improved efficiency.

Compatible with any build system that adheres to the [Bazel Remote Execution (BRE) protocol](https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto), Tide-CAS offers a memory-based CAS solution backed by a file-backed mmap. 
This design allows Tide-CAS to exceed the limitations of physical memory, making it a versatile and powerful choice for developers in search of a robust build caching system.

## Build Requirement
* Linux
* Rust


## Usage

### build
```bash
cargo build --release
```

### configuration
Tide-CAS has several command line options for configuration.
#### port
The gRPC server port on which Tide-CAS listens for incoming connections.
#### path
The path to the file used for backing the mmap storage in Tide-CAS.
#### cas-size
The size of the mmap file.
#### promotion-size
Tide-CAS uses an LRU (Least Recently Used) caching strategy, but not every accessed data item will be promoted. 
Only the last N bytes in the list, where N is specified by promotion-size, will be promoted. 
Should be smaller than 1/4 of cas-size.
#### size-limit
The maximum size of a file that can be stored in Tide-CAS. Should be smaller than promotion-size.

### run
```bash
# path is "block", cas size is 32GB, promotion size is 2GB, file size limit is 1GB
./target/release/tide-cas --path block --cas-size 32 --promotion-size 2 --size-limit 1 
```