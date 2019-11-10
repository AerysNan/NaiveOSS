package client

import (
	"net/http"
)

type OSSClient struct {
	c     *http.Client
	Token string
}

func NewOSSClient() *OSSClient {
	return &OSSClient{
		c:     &http.Client{},
		Token: "",
	}
}

func (c *OSSClient) Do(r *http.Request) (*http.Response, error) {
	return c.c.Do(r)
}
