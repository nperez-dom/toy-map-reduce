#!/bin/bash

rm -r worker-fs/map-task-output/Worker1
rm -r worker-fs/map-task-output/Worker2
rm -r worker-fs/map-task-output/Worker3

mkdir worker-fs/map-task-output/Worker1
mkdir worker-fs/map-task-output/Worker2
mkdir worker-fs/map-task-output/Worker3

rm -r worker-fs/reduce-task-output/Worker1
rm -r worker-fs/reduce-task-output/Worker2
rm -r worker-fs/reduce-task-output/Worker3

mkdir worker-fs/reduce-task-output/Worker1
mkdir worker-fs/reduce-task-output/Worker2
mkdir worker-fs/reduce-task-output/Worker3