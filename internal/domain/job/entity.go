package http 

import (
	"http"
	"fmt"
	"context"
)

type HTTPRequest struct {
	Method string
	URL    string
	Body   []byte
}

func NewHTTPRequest(method, url string, body []byte) (*HTTPRequest, error) {

	var req = &HTTPRequest{
		Method: method,
		URL:    url,
		Body:   body,
	}
	
	if  _,err :=  req{
		return nil, err
	}
	return req, nil
}
