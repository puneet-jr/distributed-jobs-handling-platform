package response

import (
	"encoding/json"
	"net/http"
)

// DRY principle. Every handler needs to:
// 1. Set Content-Type
// 2. Encode JSON
// 3. Handle encoding errors

type ErrorResponse struct {
	Error   string `json:"error"`
	Code    int    `json:"code,omitempty"`
	Details string `json:"details,omitempty"`
}

func JSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		// Can't change status code now.
		// Log for debugging, but client already got response.
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

func Error(w http.ResponseWriter, status int, message string) {
	JSON(w,status, ErrorResponse {
		Error: message,
		Code: status,
	})
}
