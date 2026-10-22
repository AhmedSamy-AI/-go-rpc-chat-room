package main

import (
	"bufio"
	"flag"
	"fmt"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"chatroom/shared"
)

var outMu sync.Mutex // serialises terminal output between goroutines

// printLine clears the current prompt line, prints text, then redraws the prompt.
func printLine(format string, a ...any) {
	outMu.Lock()
	defer outMu.Unlock()
	fmt.Printf("\r\033[K"+format+"\n> ", a...)
}

func main() {
	addr := flag.String("addr", "localhost:1234", "server address")
	name := flag.String("name", "", "username (optional, will be asked if empty)")
	interval := flag.Duration("poll", 500*time.Millisecond, "polling interval")
	flag.Parse()

	client, err := rpc.Dial("tcp", *addr)
	if err != nil {
		fmt.Println("cannot connect to server:", err)
		os.Exit(1)
	}
	defer client.Close()

	stdin := bufio.NewReader(os.Stdin)
	username := strings.TrimSpace(*name)

	// Join loop: retry on duplicate / invalid usernames.
	for {
		if username == "" {
			fmt.Print("Enter username: ")
			line, err := stdin.ReadString('\n')
			if err != nil {
				return
			}
			username = strings.TrimSpace(line)
		}
		var jr shared.JoinReply
		if err := client.Call("Chat.Join", &shared.JoinArgs{Username: username}, &jr); err != nil {
			fmt.Println("join failed:", err)
			username = ""
			continue
		}
		fmt.Printf("Welcome, %s! Online: %s\n", username, strings.Join(jr.Users, ", "))
		fmt.Println("Commands: /users  /quit   (anything else is sent as a message)")
		break
	}

	done := make(chan struct{})
	var once sync.Once
	quit := func() { once.Do(func() { close(done) }) }

	// Polling goroutine: periodically asks the server for pending messages.
	go func() {
		t := time.NewTicker(*interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				var pr shared.PollReply
				if err := client.Call("Chat.Poll", &shared.PollArgs{Username: username}, &pr); err != nil {
					printLine("*** lost connection to server: %v", err)
					quit()
					return
				}
				for _, m := range pr.Messages {
					ts := m.Time.Format("15:04:05")
					if m.Kind == shared.KindNotice {
						printLine("[%s] *** %s", ts, m.Text)
					} else {
						printLine("[%s] %s: %s", ts, m.From, m.Text)
					}
				}
			}
		}
	}()

	// Stdin goroutine.
	lines := make(chan string)
	go func() {
		for {
			line, err := stdin.ReadString('\n')
			if err != nil {
				quit() // EOF (Ctrl+D)
				return
			}
			select {
			case lines <- strings.TrimSpace(line):
			case <-done:
				return
			}
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	fmt.Print("> ")
loop:
	for {
		select {
		case <-done:
			break loop
		case <-sig:
			break loop
		case line := <-lines:
			switch {
			case line == "":
				outMu.Lock()
				fmt.Print("> ")
				outMu.Unlock()
			case line == "/quit":
				break loop
			case line == "/users":
				var lr shared.ListReply
				if err := client.Call("Chat.ListUsers", &shared.ListArgs{Username: username}, &lr); err != nil {
					printLine("error: %v", err)
				} else {
					printLine("Online (%d): %s", len(lr.Users), strings.Join(lr.Users, ", "))
				}
			default:
				var sr shared.SendReply
				if err := client.Call("Chat.Send", &shared.SendArgs{Username: username, Text: line}, &sr); err != nil {
					printLine("error: %v", err)
				} else {
					printLine("[%s] you: %s", time.Now().Format("15:04:05"), line)
				}
			}
		}
	}

	// Clean shutdown: tell the server we're leaving (best effort).
	quit()
	var lv shared.LeaveReply
	_ = client.Call("Chat.Leave", &shared.LeaveArgs{Username: username}, &lv)
	fmt.Println("\nGoodbye!")
}
