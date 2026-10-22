package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"chatroom/shared"
)

const (
	maxNameLen  = 20
	idleTimeout = 10 * time.Second // client that stops polling is considered disconnected
)

type user struct {
	name     string
	queue    []shared.Message // messages waiting for the next Poll
	lastSeen time.Time
}

// ChatService is the RPC receiver. All state is protected by mu.
type ChatService struct {
	mu    sync.Mutex
	users map[string]*user // key: lower-cased username
}

func NewChatService() *ChatService {
	return &ChatService{users: make(map[string]*user)}
}

func key(name string) string { return strings.ToLower(name) }

// broadcastLocked queues msg for every user except `except`. Caller must hold mu.
func (c *ChatService) broadcastLocked(msg shared.Message, except string) {
	for k, u := range c.users {
		if k == except {
			continue
		}
		u.queue = append(u.queue, msg)
	}
}

func (c *ChatService) namesLocked() []string {
	names := make([]string, 0, len(c.users))
	for _, u := range c.users {
		names = append(names, u.name)
	}
	sort.Strings(names)
	return names
}

func notice(text string) shared.Message {
	return shared.Message{Kind: shared.KindNotice, Text: text, Time: time.Now()}
}

func (c *ChatService) Join(args *shared.JoinArgs, reply *shared.JoinReply) error {
	name := strings.TrimSpace(args.Username)
	if name == "" || len(name) > maxNameLen || strings.ContainsAny(name, " \t\r\n") {
		return fmt.Errorf("invalid username (1-%d chars, no spaces)", maxNameLen)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.users[key(name)]; exists {
		return fmt.Errorf("username %q is already taken", name)
	}
	c.users[key(name)] = &user{name: name, lastSeen: time.Now()}
	c.broadcastLocked(notice(name+" joined the chat"), key(name))
	reply.Users = c.namesLocked()
	log.Printf("JOIN  %s (%d online)", name, len(c.users))
	return nil
}

// removeLocked deletes a user and notifies the others. Caller must hold mu.
func (c *ChatService) removeLocked(k, reason string) {
	u, ok := c.users[k]
	if !ok {
		return
	}
	delete(c.users, k)
	c.broadcastLocked(notice(u.name+" "+reason), k)
	log.Printf("LEAVE %s (%s, %d online)", u.name, reason, len(c.users))
}

func (c *ChatService) Leave(args *shared.LeaveArgs, _ *shared.LeaveReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeLocked(key(args.Username), "left the chat")
	return nil
}

func (c *ChatService) Send(args *shared.SendArgs, _ *shared.SendReply) error {
	text := strings.TrimSpace(args.Text)
	if text == "" {
		return errors.New("empty message")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	u, ok := c.users[key(args.Username)]
	if !ok {
		return errors.New("not connected: please join first")
	}
	u.lastSeen = time.Now()
	// Broadcast to everyone EXCEPT the sender.
	c.broadcastLocked(shared.Message{
		Kind: shared.KindChat, From: u.name, Text: text, Time: time.Now(),
	}, key(args.Username))
	return nil
}

// Poll returns (and clears) all pending messages for the user.
func (c *ChatService) Poll(args *shared.PollArgs, reply *shared.PollReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	u, ok := c.users[key(args.Username)]
	if !ok {
		return errors.New("not connected: please join first")
	}
	u.lastSeen = time.Now()
	reply.Messages = u.queue
	u.queue = nil
	return nil
}

func (c *ChatService) ListUsers(args *shared.ListArgs, reply *shared.ListReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if u, ok := c.users[key(args.Username)]; ok {
		u.lastSeen = time.Now()
	}
	reply.Users = c.namesLocked()
	return nil
}

// reaper removes clients that vanished without calling Leave (crash, network loss).
func (c *ChatService) reaper(stop <-chan struct{}) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			c.mu.Lock()
			for k, u := range c.users {
				if time.Since(u.lastSeen) > idleTimeout {
					c.removeLocked(k, "disconnected unexpectedly")
				}
			}
			c.mu.Unlock()
		}
	}
}

func main() {
	addr := flag.String("addr", ":1234", "address to listen on")
	flag.Parse()

	svc := NewChatService()
	srv := rpc.NewServer()
	if err := srv.RegisterName("Chat", svc); err != nil {
		log.Fatal(err)
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("chat server listening on %s (Ctrl+C to stop)", ln.Addr())

	stop := make(chan struct{})
	go svc.reaper(stop)

	var (
		wg      sync.WaitGroup
		connsMu sync.Mutex
		conns   = map[net.Conn]struct{}{}
	)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			connsMu.Lock()
			conns[conn] = struct{}{}
			connsMu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				srv.ServeConn(conn) // blocks until the client disconnects
				connsMu.Lock()
				delete(conns, conn)
				connsMu.Unlock()
			}()
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Println("shutting down...")
	close(stop)
	ln.Close()
	connsMu.Lock()
	for c := range conns {
		c.Close()
	}
	connsMu.Unlock()
	wg.Wait()
	log.Println("server stopped")
}
