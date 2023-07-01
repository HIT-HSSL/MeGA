cmake_minimum_required(VERSION 3.10)
project(MeGA)

set(CMAKE_CXX_STANDARD 14)

aux_source_directory(Utility Utility)
aux_source_directory(RollHash RollHash)
aux_source_directory(MetadataManager MetadataManager)
aux_source_directory(DedupPipeline DedupPipeline)
aux_source_directory(RestorePipeline RestorePipeline)
aux_source_directory(ArrangementPipeline ArrangementPipeline)
aux_source_directory(Rollhash Rollhash)
add_subdirectory(gflags-2.2.2)
add_subdirectory(xdelta)

link_directories(/root/jemalloc-5.3.0/lib/)
link_directories(/root/zstd-1.5.5/lib/)
link_directories(/root/isa-l_crypto-2.24.0/.libs)
link_directories(/root/openssl-OpenSSL_1_1_1t)
link_directories(/root/lz4-1.9.4/lib)

include_directories(/usr/local/include/)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")

link_libraries(gflags::gflags libisal_crypto.a pthread libcrypto.a libzstd.a xdelta liblz4.a)

add_executable(MeGA main.cpp ${Utility} ${RollHash} ${MetadataManager} ${Pipeline} ${RestorePipeline} ${ArrangementPipeline} ${Rollhash})