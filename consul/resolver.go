package consul

import (
	"errors"
	"fmt"

	consul "github.com/hashicorp/consul/api"
	"google.golang.org/grpc/naming"
)

// Resolver is the implementaion of grpc.naming.Resolver
type Resolver struct {
	ServiceName string //service name
}

// NewResolver return Resolver with service name
func NewResolver(serviceName string) *Resolver {
	return &Resolver{ServiceName: serviceName}
}

// Resolve to resolve the service from consul, target is the dial address of consul
func (cr *Resolver) Resolve(target string) (naming.Watcher, error) {
	if cr.ServiceName == "" {
		return nil, errors.New("consul: no service name provided")
	}

	// generate consul client, return if error
	conf := &consul.Config{
		Scheme:  "http",
		Address: target,
	}
	client, err := consul.NewClient(conf)
	if err != nil {
		return nil, fmt.Errorf("consul: creat consul error: %v", err)
	}

	// return ConsulWatcher
	watcher := &Watcher{
		cr: cr,
		cc: client,
	}
	return watcher, nil
}
