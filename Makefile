SHELL := /bin/bash # Run commands using bash

# .PHONY is used to declare objectives that do not correspond to real files, but are tasks or 
# commands. This ensures that the task is always executed, even if make finds a file in the current
# directory with the same name as the task itself.

default: docker-image


# `-f`: used to specify the dockerfile location
# `-t`: is used to asign a tag to the created image. <image name>:<tag>
# `.`: sets the build context to the current directory (root of the proyect). That means that when
#      path is used in the Dockerfile, it is relative to this directory.
docker-image:
	docker build -f ./src/server/workers/filter/Dockerfile -t "filter:latest" .
