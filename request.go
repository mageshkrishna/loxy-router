package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// Request headers clients use to influence scheduling.
const (
	headerPriority       = "X-Priority"
	headerConversationID = "X-Conversation-Id"
)

// maxRequestBodyBytes caps how much of a request body the scheduler buffers in
// memory (it must buffer the whole body to replay it on retry). Without a cap a
// single huge request could OOM the process. 32 MB is ample for very long-context
// prompts while preventing abuse.
const maxRequestBodyBytes = 32 << 20

// clientRequest holds the scheduling-relevant attributes of an incoming
// request: the model (from the JSON body) plus the priority and conversation
// id (from headers). The model is normalized so tag/case differences collapse.
type clientRequest struct {
	model          string
	conversationID string
	priority       Priority
}

// classify reads the routing attributes from r and returns the raw request body
// so it can be forwarded to a backend — and re-sent on a retry. r.Body is fully
// consumed and closed; callers forward the returned bytes instead. It returns an
// error if the body cannot be read fully (e.g. it exceeds a MaxBytesReader cap),
// so the caller can reject oversized requests instead of buffering them.
func classify(r *http.Request) (clientRequest, []byte, error) {
	req := clientRequest{
		priority:       parsePriority(r.Header.Get(headerPriority)),
		conversationID: strings.TrimSpace(r.Header.Get(headerConversationID)),
	}

	var body []byte
	if r.Body != nil {
		b, err := io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			return req, nil, err
		}
		body = b
	}

	var payload struct {
		Model string `json:"model"`
	}
	_ = json.Unmarshal(body, &payload) // a missing/!JSON body just yields an empty model.
	req.model = normalizeModel(payload.Model)

	return req, body, nil
}
