/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_CONFIG_H
#define MEGA_CONFIG_H

#include <string>
#include <iostream>
#include "toml.hpp"

extern std::string LogicFilePath;
extern std::string ClassFilePath;
extern std::string VersionFilePath;
extern std::string ManifestPath;
extern std::string KVPath;
extern std::string HomePath;
extern std::string ClassFileAppendPath;
extern uint64_t RetentionTime;

class ConfigReader{
public:
    ConfigReader(std::string p){
        auto data = toml::parse(p);
        std::string path = toml::find<std::string>(data, "path");
        LogicFilePath = path + "/logicFiles/Recipe%lu";
        ClassFilePath = path + "/storageFiles/Category%lu";
        VersionFilePath = path + "/storageFiles/Volume%lu";
      ManifestPath = path + "/manifest";
      KVPath = path + "/kvstore";
      HomePath = path;
        ClassFileAppendPath = path + "/storageFiles/Category%lu_append";
        int64_t rt = toml::find<int64_t>(data, "retention");
        RetentionTime = rt;
        printf("-----------------------Configure-----------------------\n");
        printf("MeGA storage path:%s, RetentionTime:%lu\n", path.data(), rt);
    }
private:
};

#endif //MEGA_CONFIG_H
