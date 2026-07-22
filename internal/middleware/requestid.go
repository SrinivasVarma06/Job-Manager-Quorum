package middleware

import (
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

func RequestID(next http.Handler) http.Handler {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		id := strconv.FormatInt(r.Int63(), 16)
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, req)
	})
}