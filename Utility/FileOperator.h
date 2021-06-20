/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_FILEOPERATOR_H
#define MEGA_FILEOPERATOR_H

#include <sys/stat.h>
#include <unistd.h>
#include <string>
#include <cstring>
#include <cassert>
#include <fcntl.h>

enum class FileOpenType {
    Read,
    Write,
    ReadWrite,
    Append,
};

uint64_t fileCounter = 0;

class FileOperator {
public:
    FileOperator(char *path, FileOpenType fileOpenType) {
        switch (fileOpenType) {
            case FileOpenType::Write :
                file = fopen(path, "wb+");
                break;
            case FileOpenType::ReadWrite :
                file = fopen(path, "rb+");
                break;
            case FileOpenType::Append :
                file = fopen(path, "ab+");
                break;
            case FileOpenType::Read :
            default:
                file = fopen(path, "rb");
                break;
        }
        if (!file) {
            printf("Can not open file %s : %s\n", path, strerror(errno));
            status = -1;
        }else{
            fileCounter++;
        }
    }

    ~FileOperator() {
        if (file != NULL) {
            fclose(file);
            fileCounter--;
        }
    }

    int getStatus(){
        return status;
    }

    int ok(){
        if(status == -1){
            return 0;
        }else{
            return 1;
        }
    }

    size_t read(uint8_t *buffer, uint64_t length) {
        return fread(buffer, 1, length, file);
    }

    size_t write(uint8_t *buffer, uint64_t length) {
        return fwrite(buffer, 1, length, file);
    }

    int seek(uint64_t offset) {
        return fseeko64(file, offset, SEEK_SET);
    }

    int trunc(uint64_t size) {
        return ftruncate64(fileno(file), size);
    }

    static uint64_t size(const std::string &path) {
        struct stat statBuffer;
        int r = stat(path.c_str(), &statBuffer);
        if(!r){
            return statBuffer.st_size;
        } else {
            return 0;
        }

    }

    int fdatasync() {
        return ::fdatasync(file->_fileno);
    }

    int fsync() {
        return fflush(file);
        //return ::fsync(file->_fileno);
    }

    int getFd() {
        return fileno(file);
    }

    FILE *getFP() {
        return file;
    }

    int releaseBufferedData() {
        posix_fadvise(file->_fileno, 0, 0, POSIX_FADV_DONTNEED);
    }

    uint64_t getSize() {
        fseek(file, 0, SEEK_END);
        uint64_t result = ftell(file);
        fseek(file, 0, SEEK_SET);
        return result;
    }

private:
    FILE *file;
    int status = 0;
};


#endif //MEGA_FILEOPERATOR_H
