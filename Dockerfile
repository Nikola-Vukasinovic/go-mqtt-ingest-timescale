# Use an official Go runtime as a parent image
FROM golang:1.21 AS builder

# Set the working directory
WORKDIR /app

# Download Go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code. Note the slash at the end, as explained in
# https://docs.docker.com/engine/reference/builder/#copy
COPY *.go ./

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o /mqtt-ingestion

# Expose the MQTT broker port (adjust if necessary)
EXPOSE 1883

# Run the Go application when the container starts
CMD ["/mqtt-ingestion"]