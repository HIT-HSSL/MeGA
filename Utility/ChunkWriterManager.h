/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_CHUNKWRITERMANAGER_H
#define MEGA_CHUNKWRITERMANAGER_H

#include "Likely.h"
#include <zstd.h>

extern std::string ClassFilePath;
extern std::string VersionFilePath;
extern uint64_t ContainerSize;
uint64_t BufferCapacity = ContainerSize * 1.2;

struct timeval ct0, ct1, wt0, wt1;

struct WriteBuffer {
    uint64_t totalLength;
    uint64_t used;
    uint8_t *buffer = nullptr;
    uint8_t *compressBuffer = nullptr;

    void init() {
        buffer = (uint8_t *) malloc(BufferCapacity);
        compressBuffer = (uint8_t *) malloc(BufferCapacity);
        totalLength = ContainerSize;
        used = 0;
    }

    void write(uint8_t *buf, uint64_t len) {
        memcpy(buffer + used, buf, len);
        used += len;
    }

    void clear() {
        memset(buffer, 0, BufferCapacity);
        memset(compressBuffer, 0, BufferCapacity);
        used = 0;
    }

    void release() {
        free(buffer);
        free(compressBuffer);
    }
};


class ChunkWriterManager {
public:
    ChunkWriterManager(uint64_t cv) : runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock) {
        currentVersion = cv;

        sprintf(pathBuffer, ClassFilePath.data(), currentVersion, currentVersion, containerCounter);
        writer = new FileOperator(pathBuffer, FileOpenType::Write);

        writeBuffer.init();
    }

    int writeClass(uint8_t *header, uint64_t headerLen, uint8_t *buffer, uint64_t bufferLen) {
        writeBuffer.write(header, headerLen);
        writeBuffer.write(buffer, bufferLen);

        if (writeBuffer.used >= writeBuffer.totalLength) {
            flush();
            prepareNew();
        }
        return 0;
    }

    ~ChunkWriterManager() {
        printf("[DedupWriter] Compression Time : %lu, Write Time : %lu\n", compressionTime, writeTime);
        printf("BeforeCompression:%lu, AfterCompression:%lu, CompressionReduce:%lu, CompressionRatio:%f\n",
               sizeBeforeCompression, sizeAfterCompression, sizeBeforeCompression - sizeAfterCompression,
               (float) sizeBeforeCompression / sizeAfterCompression);

        flush();

        writeBuffer.release();
    }

private:

    int flush() {
        gettimeofday(&ct0, NULL);
        size_t compressedSize = ZSTD_compress(writeBuffer.compressBuffer, BufferCapacity, writeBuffer.buffer,
                                              writeBuffer.used, ZSTD_CLEVEL_DEFAULT);
        gettimeofday(&ct1, NULL);
        compressionTime += (ct1.tv_sec - ct0.tv_sec) * 1000000 + ct1.tv_usec - ct0.tv_usec;

        sizeBeforeCompression += writeBuffer.used;
        sizeAfterCompression += compressedSize;
        assert(!ZSTD_isError(compressedSize));

        gettimeofday(&wt0, NULL);
        writer->write(writeBuffer.compressBuffer, compressedSize);
        writer->fsync();
        writer->releaseBufferedData();
        gettimeofday(&wt1, NULL);
        writeTime += (wt1.tv_sec - wt0.tv_sec) * 1000000 + wt1.tv_usec - wt0.tv_usec;
        delete writer;
        writer = nullptr;
    }

    int prepareNew() {
        containerCounter++;
        sprintf(pathBuffer, ClassFilePath.data(), currentVersion, currentVersion, containerCounter);
        writer = new FileOperator(pathBuffer, FileOpenType::Write);
        writeBuffer.clear();
    }

    FileOperator *writer = nullptr;
    WriteBuffer writeBuffer;
    uint64_t currentVersion;
    char pathBuffer[256];

    uint64_t containerCounter = 0;

    bool runningFlag;
    uint64_t taskAmount;
    std::list<uint64_t> taskList;
    MutexLock mutexLock;
    Condition condition;

    uint64_t sizeBeforeCompression = 0;
    uint64_t sizeAfterCompression = 0;

    uint64_t compressionTime = 0, writeTime = 0;
};

#endif //MEGA_CHUNKWRITERMANAGER_H
