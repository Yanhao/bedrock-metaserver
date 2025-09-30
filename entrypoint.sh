#!/bin/sh

# Debug: Print environment variables to check they are set
echo "METASERVER_NAME is set to: $METASERVER_NAME"
echo "Current working directory: $(pwd)"

# Create a temporary copy of config.toml since the original is read-only
cp /opt/bedrock/metaserver/config.toml /tmp/config.toml

echo "Contents of original config.toml:"
cat /opt/bedrock/metaserver/config.toml

# Replace environment variables in the temporary config file
sed -i "s|\${METASERVER_NAME}|$METASERVER_NAME|g" /tmp/config.toml

# Add advertise addresses to config
# Keep peer_addr and client_addr as 0.0.0.0 for binding, but add advertise addresses
advertise_peer_addr="http://$METASERVER_NAME:2380"
advertise_client_addr="http://$METASERVER_NAME:2379"

# Add advertise configuration to etcd section
sed -i "/\[etcd\]/a\advertise_peer_addr = \"$advertise_peer_addr\"\nadvertise_client_addr = \"$advertise_client_addr\"" /tmp/config.toml

echo "Contents of modified config.toml:"
cat /tmp/config.toml

# Set environment variables for advertise addresses to be used in etcd.go
ADVERTISE_PEER_ADDR="$advertise_peer_addr"
ADVERTISE_CLIENT_ADDR="$advertise_client_addr"
export ADVERTISE_PEER_ADDR ADVERTISE_CLIENT_ADDR

# Run the metaserver with the modified config
./metaserver -config /tmp/config.toml