/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_MANIFEST_H
#define MEGA_MANIFEST_H

#include <string>
#include "FileOperator.h"

struct Manifest {
    uint64_t TotalVersion;
    uint64_t ArrangementFallBehind;
};

extern std::string ManifestPath;

class ManifestWriter {
public:
    ManifestWriter(const Manifest &manifest) {
      FileOperator fileOperator((char *) ManifestPath.data(), FileOpenType::Write);
      assert(fileOperator.ok());
      fileOperator.write((uint8_t *) &manifest, sizeof(Manifest));
    }

private:
};

class ManifestReader {
public:
    ManifestReader(struct Manifest *manifest) {
      printf("-----------------------Manifest-----------------------\n");
      printf("Loading Manifest..\n");
      FileOperator fileOperator((char *) ManifestPath.data(), FileOpenType::Read);
      if (fileOperator.getStatus() == -1) {
        printf("0 version in storage\n");
        manifest->TotalVersion = 0;
        manifest->ArrangementFallBehind = 0;
      } else {
        fileOperator.read((uint8_t *) manifest, sizeof(Manifest));
        printf("%lu versions in storage\n", manifest->TotalVersion);
      };
    }

private:
};

#endif //MEGA_MANIFEST_H
