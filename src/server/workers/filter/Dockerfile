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
RUN mkdir protobuf
RUN mkdir rabbitmq
RUN mkdir globalconfig
RUN mkdir coordinator
RUN mkdir server
RUN mkdir vendor
RUN mkdir health
COPY src/go.mod .
COPY src/go.sum .
COPY src/protobuf ./protobuf
COPY src/rabbitmq ./rabbitmq
COPY src/globalconfig ./globalconfig
COPY src/coordinator ./coordinator
COPY src/server ./server
COPY src/vendor ./vendor
COPY src/health ./health
# Build the binary
# * `CGO_ENABLED` must be disabled to run go binary in Alpine
# * `GOOS=linux` tells the compiler to generate a binary for Linux
# * `-o bin/filter` tells go where to store the binary
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o bin/filter ./server/workers/filter

# Final image --------------------------------------------------------------------------------------
# busybox is a minimalist image based on Alpine Linux
FROM busybox:latest
# Copy the compiled binary from the previous stage 
COPY --from=builder /build/bin/filter /filter
# COPY ./client/config.yaml /config.yaml
ENTRYPOINT ["/filter"]