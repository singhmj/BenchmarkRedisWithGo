package common

import (
	"fmt"

	"github.com/mediocregopher/radix"
)

// ConnectToCluster :
func ConnectToCluster(clusterAddrs []string) (*radix.Cluster, error) {
	opts := []radix.ClusterOpt{}
	cluster, err := radix.NewCluster(clusterAddrs, opts...)
	if err != nil {
		return nil, fmt.Errorf("Unable to connect to the cluster. More info: %v", err)
	}

	return cluster, nil
}

// ConnectToSingleNode :
func ConnectToSingleNode(transport string, address string, poolSize int) (*radix.Pool, error) {
	opts := []radix.PoolOpt{}
	conn, err := radix.NewPool(transport, address, poolSize, opts...)
	if err != nil {
		return nil, fmt.Errorf("Unable to connect to the node. More info: %v", err)
	}

	return conn, nil
}
