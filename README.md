# octopus


Octopus: an RDMA-enabled distributed persistent memory file system
=========

Please read the following paper for more details:
Youyou Lu, Jiwu Shu, Youmin Chen. "Octopus: an RDMA-enabled distributed
persistent memory file system", in USENIX Annual Technical Conference
(USENIX ATC'17), 2017


WARNING: The code can only used for academic research. 


Installation:

- OS: Fedora
- Dependencies: fuse-devel,
  cryptopp-devel，boost-devel，mpicxx，g++（4.9 and
higher），libibverbs，java

Build Octopus:
- create a new folder "build"
- run "cmake .. & make -j"
 * dmfs: octopus server
 * mpibw: data I/O test
 * mpitest: metadata

Octopus support two modes: fuse and library:
- Library: refer to the usage of src/client/nrfs.h and mpibw/mpitest
- FUSE (fusenrfs): octopus client over fuse
	-- run "./fusenrfs -f -o direct_io  /mnt/dmfs"

Configuration:
conf.xml: configuration of the cluster


Storage Research Group @ Tsinghua Universty