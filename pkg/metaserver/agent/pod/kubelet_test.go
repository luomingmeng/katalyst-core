/*
Copyright 2026 The Katalyst Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pod

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/kubewharf/katalyst-core/pkg/config/agent/global"
)

func TestGetPodListCancelsInsecureRequest(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(requestStarted)
		select {
		case <-request.Context().Done():
		case <-releaseRequest:
		}
	}))
	defer server.Close()
	defer close(releaseRequest)

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}
	_, portString, err := net.SplitHostPort(serverURL.Host)
	if err != nil {
		t.Fatalf("split test server host and port: %v", err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatalf("parse test server port: %v", err)
	}

	baseConf := global.NewBaseConfiguration()
	baseConf.KubeletReadOnlyPort = port
	baseConf.KubeletPodsEndpoint = "pods"
	fetcher := NewKubeletPodFetcher(baseConf)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := fetcher.GetPodList(ctx, nil)
		result <- err
	}()

	select {
	case <-requestStarted:
	case err := <-result:
		t.Fatalf("GetPodList returned before the request reached the server: %v", err)
	case <-time.After(time.Second):
		t.Fatal("request did not reach the server")
	}
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("GetPodList() error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("GetPodList did not return after its context was canceled")
	}
}
