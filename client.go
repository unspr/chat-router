package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"gopkg.in/ini.v1"

	"net/http"
	"net/url"
)

type RequestPayload struct {
	Model                 string `json:"model"`
	Input                 string `json:"input"`
	Stream                bool   `json:"stream"`
	PreviousInteractionId string `json:"previous_interaction_id,omitempty"`
}

var theUrl, apiKey, model string
var client *http.Client

func Init() {
	cfg, err := ini.Load("config.ini")
	if err != nil {
		log.Fatalf("无法加载配置文件: %v", err)
	}

	section := cfg.Section("AIConfig")
	theUrl = section.Key("URL").String()
	apiKey = section.Key("Token").String()
	model = section.Key("Model").String()

	proxySec := cfg.Section("Proxy")

	proxyURL, err := url.Parse(fmt.Sprintf("http://%s:%s", proxySec.Key("Host").String(), proxySec.Key("Port").String())) // 替换为你的代理地址/端口
	if err != nil {
		panic(err)
	}

	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
	}

	client = &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}
}

func GetSessionChannel(sessionId string, input string) chan string {
	payload := RequestPayload{
		Model:                 model,
		Input:                 input,
		Stream:                true,
		PreviousInteractionId: sessionId,
	}
	jsonData, _ := json.Marshal(payload)

	events, _ := SubscribeSSE(jsonData)
	return events
}

type Event struct {
	Index int `json:"index"`
	Delta struct {
		Text string `json:"text"`
		Type string `json:"type"`
	} `json:"delta"`
	EventType   string `json:"event_type"`
	Interaction struct {
		Id string `json:"id"`
	} `json:"interaction"`
}

func SubscribeSSE(payload []byte) (chan string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	req, err := http.NewRequestWithContext(ctx, "POST", theUrl, bytes.NewBuffer(payload))
	if err != nil {
		defer cancel()
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-goog-api-key", apiKey)

	resp, err := client.Do(req)
	if err != nil {
		panic(err)
		// defer cancel()
		// return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		defer cancel()
		x, _ := io.ReadAll(resp.Body)
		log.Fatalf("HTTP error: %s %s", resp.Status, string(x))
		return nil, fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}

	events := make(chan string, 100)

	go func() {
		defer resp.Body.Close()
		defer close(events)
		defer cancel()

		scanner := bufio.NewScanner(resp.Body)
		var sessionId string
		for scanner.Scan() {
			line := scanner.Text()
			log.Printf(line)
			if strings.HasPrefix(line, "data: ") {
				var event Event
				json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event)

				if len(sessionId) == 0 {
					sessionId = event.Interaction.Id
					log.Printf(sessionId)
					events <- sessionId
					continue
				}

				text := event.Delta.Text
				if len(text) == 0 {
					continue
				}
				events <- text
			}
		}

		if err := scanner.Err(); err != nil {
			log.Printf("Scanner 读取出错，中断原因: %s", err)
		}
	}()

	return events, nil
}
