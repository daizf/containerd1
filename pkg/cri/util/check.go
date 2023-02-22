package util

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"time"
)

func CheckP2PAgentStarted(port string) error {

	return WaitUntilReady(fmt.Sprintf("https://127.0.0.1:%s/debug/version", port), 0*time.Millisecond, 10*time.Millisecond, 2*time.Second)
}

func WaitUntilReady(url string, initialDelay, period, timeout time.Duration) error {

	to := time.After(timeout + initialDelay)
	hc := http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	time.Sleep(initialDelay)
	for {
		select {
		case <-to:
			return fmt.Errorf("timeout %v", timeout)
		default:
			resp, err := hc.Get(url)
			if err != nil {
				time.Sleep(period)
				continue
			}
			resp.Body.Close()
			return nil
		}
	}
}
