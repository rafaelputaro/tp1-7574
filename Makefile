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
.PHONY: docker-image

# `-d`: detached mode. Run the containers in the background as to not take over the terminal session.
# `--build`: force the rebuilding of the images before starting the containers.
docker-compose-up: docker-image
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

# first `-f`: specifies the Compose file
# `logs`: display logs from all running containers defined in the Compose file
# second `-f`: follow. Used to keep streaming logs in real time
docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs

# `stop`: stop running the containers, without removing them. This means that the containers can be
#         restarted later using `docker compose start`
# `-t 1`: specifies a 1-second timeout before forcefully stopping the containers (sends a SIGKILL)
# `down`: removes all containers, networks and volumes (unless marked as external). It completely
#         tears down the environment, requiring fresh container creation on the next `docker compose up`.

# Calling `stop` before `down` ensures a graceful shutdown (though redundant in most cases)
docker-compose-down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down