#!/bin/bash

data_path=/mnt/sda/CHRM
version=0
./init.sh

for file in $data_path/*;
do
        version=$(expr $version + 1)
        echo "#" $version ": " $file
        echo 3 > /proc/sys/vm/drop_caches
        cp $file /dev/shm/test

        echo "Deduplicating and Storing.."
        # deduplicating and storing a new backup
        ./MeGA --ConfigFile=config.toml --task=write --InputFile=/dev/shm/test --DeltaSelectorThreshold=30

        echo "Restore.."
        echo 3 > /proc/sys/vm/drop_caches
        # restoring the lastest backup.
        ./MeGA --ConfigFile=config.toml --task=restore --RestorePath=/dev/shm/test --RestoreRecipe=-1

done
