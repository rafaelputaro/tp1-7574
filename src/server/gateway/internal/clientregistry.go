package internal

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"strings"
	"sync"
)

type ClientRegistry struct {
	mu     sync.Mutex
	lookup map[string]int32
	nextID int32
}

func NewClientRegistry() *ClientRegistry {
	return &ClientRegistry{lookup: make(map[string]int32)}
}

func (cr *ClientRegistry) GetOrCreateClientID(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", status.Errorf(codes.Internal, "could not get client peer info")
	}
	ip := strings.Split(p.Addr.String(), ":")[0]
	return cr.getOrCreateClientID(ip), nil
}

func (cr *ClientRegistry) getOrCreateClientID(ip string) string {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if id, ok := cr.lookup[ip]; ok {
		return string(id)
	}
	cr.nextID++
	cr.lookup[ip] = cr.nextID
	return string(cr.nextID)
}
