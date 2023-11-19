/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_ROLLHASH_H
#define MEGA_ROLLHASH_H

#include <functional>

enum class HashType {
    Rabin,
    Gear,
};

class RollHash {
public:
    virtual inline uint64_t rolling(uint8_t *inputPtr) {
      printf("RollHash error encountered\n");
      return 0;
    }

    virtual uint64_t reset() {
      printf("RollHash error encountered\n");
      return 0;
    }

    virtual uint64_t getDeltaMask() {
      printf("RollHash error encountered\n");
      return 0;
    }

    virtual uint64_t getChunkMask() {
      printf("RollHash error encountered\n");
      return 0;
    }

    virtual bool tryBreak(uint64_t fp) {
      printf("RollHash error encountered\n");
      return 0;
    }

    virtual uint64_t *getMatrix() {
      printf("RollHash error encountered\n");
      return 0;
    }
};

#endif //MEGA_ROLLHASH_H
