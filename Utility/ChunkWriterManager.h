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

DEFINE_uint64(WriteBufferLength,
              4194304, "WriteBufferLength");

extern std::string ClassFilePath;
extern std::string VersionFilePath;

int WorkerExitMagicNumber = -1;

struct WriteBuffer {
    uint64_t totalLength;
    uint64_t available;
};


class ChunkWriterManager {
public:
    ChunkWriterManager(uint64_t cv) : runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock) {
        currentVersion = cv;

        sprintf(pathBuffer, ClassFilePath.data(), currentVersion, currentVersion, containerCounter);
        writer = new FileOperator(pathBuffer, FileOpenType::Write);
        writeBuffer = {
                FLAGS_WriteBufferLength,
                0,
        };
    }

    int writeClass(uint8_t *header, uint64_t headerLen, uint8_t *buffer, uint64_t bufferLen) {
        writer->write((uint8_t *) header, headerLen);
        writer->write((uint8_t *) buffer, bufferLen);
        writeBuffer.available += headerLen + bufferLen;
        if (writeBuffer.available >= writeBuffer.totalLength) {
            writer->fsync();
            delete writer;
            containerCounter++;
            sprintf(pathBuffer, ClassFilePath.data(), currentVersion, currentVersion, containerCounter);
            writer = new FileOperator(pathBuffer, FileOpenType::Write);
            writeBuffer.available = 0;
        }
        return 0;
    }

    ~ChunkWriterManager() {
        writer->fsync();
        delete writer;
    }

private:

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
};

#endif //MEGA_CHUNKWRITERMANAGER_H
