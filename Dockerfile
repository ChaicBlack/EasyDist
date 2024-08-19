# Use the official Rust image
FROM rust:latest

# Set the working directory
WORKDIR /usr/src/caogo

# Copy the Cargo.toml and Cargo.lock files
COPY Cargo.toml Cargo.lock ./

# Copy the source code
COPY src ./src

# Build the server binary
RUN cargo build --release --bin server

# Build the client binary
RUN cargo build --release --bin client

# Create a script to run the binaries
RUN echo '#!/bin/sh\n./target/release/server &' > /usr/src/caogo/run_server.sh
RUN echo './target/release/client' > /usr/src/caogo/run_client.sh
RUN chmod +x /usr/src/caogo/run_server.sh /usr/src/caogo/run_client.sh

# Set the default command to run the server
CMD ["./run_server.sh"]
