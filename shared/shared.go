// Package shared holds the RPC argument/reply types used by both server and client.
package shared

import "time"

type Kind string

const (
	KindChat   Kind = "chat"
	KindNotice Kind = "notice"
)

type Message struct {
	Kind Kind
	From string
	Text string
	Time time.Time
}

type JoinArgs struct{ Username string }
type JoinReply struct{ Users []string }

type LeaveArgs struct{ Username string }
type LeaveReply struct{}

type SendArgs struct {
	Username string
	Text     string
}
type SendReply struct{}

type PollArgs struct{ Username string }
type PollReply struct{ Messages []Message }

type ListArgs struct{ Username string }
type ListReply struct{ Users []string }
