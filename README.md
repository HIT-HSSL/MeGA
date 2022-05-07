# MeGA

### Requirement

+ isal_crypto [https://github.com/intel/isa-l_crypto]
+ jemalloc [https://github.com/jemalloc/jemalloc]
+ openssl
+ zstd [https://github.com/facebook/zstd]

### Build

```
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j
``` 

### Usage

+ Initializing

```
cd build
chmod +x init.sh
./init.sh [working path, identical to "path" in config file.]
```

+ Configuring working path

```
Edit "path" in a config file (build/config.toml is an example).
```

+ Backup a new backup into the system, which includes backup workflow, arranging workflow, and deletion workflow when
  exceeding the retaining limit.

```
./MeGA --ConfigFile=[config file path] --task=write --InputFile=[backup workload] --DeltaSelectorThreshold=[Delta Selector Threshold]
```

+ Restore a workload of from the system

```
./MeGA --ConfigFile=[config file path] --task=restore --RestorePath=[path to restore] --RestoreRecipe=[which backup to restore(1 ~ no. of the last retained backup)]
```  
