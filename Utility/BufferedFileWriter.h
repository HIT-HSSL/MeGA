/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_BUFFEREDFILEWRITER_H
#define MEGA_BUFFEREDFILEWRITER_H

class BufferedFileWriter {
public:
    BufferedFileWriter(FileOperator *fd, uint64_t size, uint64_t st) : fileOperator(fd), bufferSize(size),
                                                                       syncThreshold(st) {
        writeBuffer = (uint8_t *) malloc(bufferSize);
        //writeBufferAvailable = bufferSize; olad
        writeBufferAvailable = 0;
    }

    int write(uint8_t *data, int dataLen) {
        fileOperator->write(data, dataLen);
        writeBufferAvailable += dataLen;
        if (writeBufferAvailable >= bufferSize) {
            flush();
        }
        return 0;
    }

    int write_old(uint8_t *data, int dataLen) {
        if (dataLen > writeBufferAvailable) {
            flush();
        }
        uint8_t *writePoint = writeBuffer + bufferSize - writeBufferAvailable;
        memcpy(writePoint, data, dataLen);
        writeBufferAvailable -= dataLen;
        return 0;
    }

    ~BufferedFileWriter() {
        counter += syncThreshold;
        flush();
        free(writeBuffer);
    }

private:

    int flush() {
        writeBufferAvailable = 0;
        counter++;
        if (counter >= syncThreshold) {
            fileOperator->fdatasync();
            counter = 0;
        }
    }

    int flush_old() {
        fileOperator->write(writeBuffer, bufferSize - writeBufferAvailable);
        writeBufferAvailable = bufferSize;
        counter++;
        if (counter >= syncThreshold) {
            fileOperator->fdatasync();
            counter = 0;
        }
    }

    uint64_t bufferSize;
    uint8_t *writeBuffer;
    int writeBufferAvailable;
    FileOperator *fileOperator;

    uint64_t counter = 0;
    uint64_t syncThreshold = 0;
};

#endif //MEGA_BUFFEREDFILEWRITER_H
