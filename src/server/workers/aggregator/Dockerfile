FROM golang:1.24.1 AS builder

# Client uses docker multistage builds feature https://docs.docker.com/develop/develop-images/multistage-build/
# First stage is used to compile golang binary and second stage is used to only copy the 
# binary generated to the deploy image. 
# Docker multi stage does not delete intermediate stages used to build our image, so we need 
# to delete it by ourselves. Since docker does not give a good alternative to delete the intermediate images
# we are adding a very specific label to the image to then find these kind of images and delete them

# multi-stage builds is used so that only the Go binary is copied on the final image, making it lighter.
# 1st stage: builder, 2nd stage: final image
LABEL intermediateStageToBeDeleted=true

WORKDIR /build/
# Source code
COPY src/ .
# Build the binary
# * `CGO_ENABLED` must be disabled to run go binary in Alpine
# * `GOOS=linux` tells the compiler to generate a binary for Linux
# * `-o bin/filter` tells go where to store the binary
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o bin/aggregator ./server/workers/aggregator

# Final image --------------------------------------------------------------------------------------
# busybox is a minimalist image based on Alpine Linux
FROM busybox:latest
# Copy the compiled binary from the previous stage 
COPY --from=builder /build/bin/aggregator /aggregator
COPY ./src/server/workers/aggregator/config.yaml /config.yaml
ENTRYPOINT ["/aggregator"]





#FROM golang:1.24.1 AS builder
# Client uses docker multistage builds feature https://docs.docker.com/develop/develop-images/multistage-build/
# First stage is used to compile golang binary and second stage is used to only copy the 
# binary generated to the deploy image. 
# Docker multi stage does not delete intermediate stages used to build our image, so we need 
# to delete it by ourselves. Since docker does not give a good alternative to delete the intermediate images
# we are adding a very specific label to the image to then find these kind of images and delete them
#LABEL intermediateStageToBeDeleted=true

#RUN mkdir -p /build
#WORKDIR /build/
#COPY . .
# CGO_ENABLED must be disabled to run go binary in Alpine
#RUN CGO_ENABLED=0 GOOS=linux go build -o bin/server/workers/aggregator/ ./server/workers/aggregator/test/main.go
#FROM busybox:latest
#COPY --from=builder /build/bin/server/workers/aggregator/ /server/workers/aggregator/
#COPY ./server/workers/aggregator/config.yaml /config.yaml
#ENTRYPOINT ["/bin/sh"]

