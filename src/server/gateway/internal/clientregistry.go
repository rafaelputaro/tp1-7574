package internal

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"strconv"
	"strings"
	"sync"
)

type ClientRegistry struct {
	mu     sync.Mutex
	lookup map[string]int
	nextID int
}

func NewClientRegistry() *ClientRegistry {
	return &ClientRegistry{lookup: make(map[string]int)}
}

func (cr *ClientRegistry) GetOrCreateClientID(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", status.Errorf(codes.Internal, "could not get client peer info")
	}
	ip := strings.Split(p.Addr.String(), ":")[0]
	return cr.getOrCreateClientID(ip), nil
}

func (cr *ClientRegistry) MarkAsDone(ctx context.Context) error {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return status.Errorf(codes.Internal, "could not get client peer info")
	}
	ip := strings.Split(p.Addr.String(), ":")[0]
	cr.mu.Lock()
	defer cr.mu.Unlock()
	if _, ok := cr.lookup[ip]; ok {
		delete(cr.lookup, ip)
	}
	return nil
}

func (cr *ClientRegistry) getOrCreateClientID(ip string) string {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if id, ok := cr.lookup[ip]; ok {
		return strconv.Itoa(id)
	}
	cr.nextID++
	cr.lookup[ip] = cr.nextID
	return strconv.Itoa(cr.nextID)
}
