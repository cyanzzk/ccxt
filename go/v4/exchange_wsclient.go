package ccxt

// WebSocket transport for streaming ("pro") exchanges.
// ----------------------------------------------------
// This is intentionally minimal: it supports concurrent subscriptions,
// automatic JSON encoding/decoding and graceful shutdown.  Higher-level
// exchange code decides which messages belong to which subscription;
// the client only multiplexes frames to a per-channel inbox.
//
// The implementation uses github.com/gorilla/websocket – ensure that
// dependency is present in go.mod (go get if needed).

import (
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WSClient is a thin wrapper around a single ws:// or wss:// connection.
// Subscriptions are identified by an arbitrary hash string (often equal to the
// messageHash used in the JS/C# layers).  Each Subscribe call returns a
// receive-only channel that the caller reads updates from.

type WSClient struct {
	*Client

	ConnectionStarted int64
	Protocols         interface{}
	Options           interface{}
	StartedConnecting bool
	ProxyUrl          string

	PingMu sync.RWMutex
}

// NewWSClient dials the given URL and starts the read-loop.
func NewWSClient(url string, onMessageCallback func(client interface{}, err interface{}), onErrorCallback func(client interface{}, err interface{}), onCloseCallback func(client interface{}, err interface{}), onConnectedCallback func(client interface{}, err interface{}), proxyUrl string, config ...map[string]interface{}) *WSClient {
	// Call NewClient to do exactly the same initialization
	client := NewClient(url, onMessageCallback, onErrorCallback, onCloseCallback, onConnectedCallback, config...)

	// Wrap the Client in a WSClient
	wsClient := &WSClient{
		Client:   client,
		ProxyUrl: proxyUrl,
	}
	wsClient.StartedConnecting = false

	return wsClient
}

func (this *WSClient) CreateConnection() error {
	if this.Verbose {
		this.Log(time.Now(), "connecting to", this.Url)
	}
	this.ConnectionStarted = Milliseconds()
	this.SetConnectionTimeout()

	var proxy func(*http.Request) (*url.URL, error)
	if this.ProxyUrl != "" {
		urlproxyURL, _ := url.Parse(this.ProxyUrl)
		proxy = http.ProxyURL(urlproxyURL)
	} else {
		proxy = http.ProxyFromEnvironment
	}
	// Create WebSocket dialer
	dialer := websocket.Dialer{
		Proxy:             proxy,
		HandshakeTimeout:  10 * time.Second,
		EnableCompression: false,
	}

	// Set up headers for protocols
	headers := http.Header{}
	if this.Protocols != nil {
		if protocols, ok := this.Protocols.([]string); ok {
			headers.Set("Sec-WebSocket-Protocol", strings.Join(protocols, ", "))
		}
	}

	// Connect to WebSocket
	conn, _, err := dialer.Dial(this.Url, headers)
	if err != nil {
		return err
	}
	this.Connection = conn

	// handle connection pong here:
	this.Connection.SetPongHandler(func(string) error {
		this.OnPong()
		return nil
	})

	// Start event handling goroutines
	go this.handleOpen()
	go this.handleMessages()

	return nil
}

// Handle WebSocket open event
func (this *WSClient) handleOpen() {
	this.OnOpen()
}

// Handle incoming WebSocket messages
func (this *WSClient) handleMessages() {
	defer func() {
		this.OnClose(nil)
		if this.Connection != nil {
			this.Connection.Close()
		}
	}()

	for {
		if this.Connection == nil {
			return
		}

		messageType, data, err := this.Connection.ReadMessage()
		if err != nil {
			this.OnError(NetworkError(err))
			return
		}

		switch messageType {
		case websocket.TextMessage, websocket.BinaryMessage:
			this.OnMessage(data)
		case websocket.PingMessage:
			this.OnPing()
			// Respond with pong
			if this.Verbose {
				this.Log(time.Now(), "sending connection pong")
			}
			this.ConnectionMu.Lock()
			this.Connection.WriteMessage(websocket.PongMessage, nil)
			this.ConnectionMu.Unlock()
		case websocket.PongMessage:
			this.OnPong()
		case websocket.CloseMessage:
			return
		}
	}
}

func (this *WSClient) Connect(backoffDelay ...int) (*Future, error) {
	if !this.StartedConnecting {
		this.StartedConnecting = true
		// exponential backoff for consequent ws connections if necessary
		delay := 0
		if len(backoffDelay) > 0 {
			delay = backoffDelay[0]
		}

		if delay > 0 {
			go func() {
				time.Sleep(time.Duration(delay) * time.Millisecond)
				this.CreateConnection()
				// TODO: handle error
				// if err != nil {
				// 	return nil, err
				// }
			}()
		} else {
			err := this.CreateConnection()
			if err != nil {
				return nil, err
			}
		}
	}
	return this.Connected.(*Future), nil
}

func (this *WSClient) IsOpen() bool {
	return this.Connection != nil
}

func (this *WSClient) ResetConnection(err interface{}) {
	this.ClearConnectionTimeout()
	this.ClearPingInterval()
	this.Reject(err)
}

func (this *WSClient) SetPingInterval() {
	if this.KeepAlive.(int64) > 0 {
		ticker := time.NewTicker(time.Duration(this.KeepAlive.(int64)) * time.Millisecond)
		this.PingInterval = ticker
		go func() {
			defer ticker.Stop() // Ensure ticker is stopped when goroutine exits
			for {
				select {
				case <-ticker.C:
					this.OnPingInterval()
				case <-this.Disconnected.(*Future).Await(): // Exit when client is disconnected
					return
				}
			}
		}()
	}
}

func (this *WSClient) ClearPingInterval() {
	if this.PingInterval != nil {
		if ticker, ok := this.PingInterval.(*time.Ticker); ok {
			ticker.Stop()
		}
		this.PingInterval = nil
	}
}

func (this *WSClient) OnPingInterval() {
	this.PingMu.Lock()
	keepAlive, _ := this.KeepAlive.(int64)
	if keepAlive <= 0 {
		this.PingMu.Unlock()
		return
	}
	isConn, _ := this.IsConnected.(bool)
	if !isConn {
		this.PingMu.Unlock()
		return
	}
	now := time.Now().UnixNano() / int64(time.Millisecond)
	lastPong := this.GetLastPong()
	if lastPong == nil {
		lastPong = now
		this.SetLastPong(lastPong)
	}
	lastPongVal := lastPong.(int64)
	maxPingPongMisses := float64(2.0)
	if this.MaxPingPongMisses != nil {
		if misses, ok := this.MaxPingPongMisses.(float64); ok {
			maxPingPongMisses = misses
		}
	}
	timedOut := (lastPongVal + keepAlive*int64(maxPingPongMisses)) < now

	// Build the ping message while holding PingMu, but release PingMu
	// before any write to avoid lock-ordering deadlocks with ConnectionMu.
	var message interface{}
	if !timedOut && this.Ping != nil {
		if pingFunc, ok := this.Ping.(func(*WSClient) interface{}); ok {
			message = pingFunc(this)
		}
		if pingFunc, ok := this.Ping.(func(interface{}) interface{}); ok {
			message = pingFunc(this)
		}
	}
	this.PingMu.Unlock()

	if timedOut {
		err := RequestTimeout("Connection to " + this.Url + " timed out due to a ping-pong keepalive missing on time")
		this.OnError(err)
		return
	}

	if message != nil {
		// Send application-level ping (e.g. Bybit {"op":"ping"}).
		// Send() acquires ConnectionMu internally, so we must NOT hold PingMu here.
		go func() {
			future := this.Send(message)
			if err := <-future; err != nil {
				if b, ok := err.(bool); ok && b {
					return
				}
				this.OnError(err)
			}
		}()
	} else {
		// Protocol-level WebSocket ping frame.
		this.ConnectionMu.Lock()
		if this.Connection != nil {
			if this.Verbose {
				this.Log(time.Now(), "sending connection ping")
			}
			this.Connection.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
		}
		this.ConnectionMu.Unlock()
	}
}

func (this *WSClient) OnOpen() {
	if this.Verbose {
		this.Log(time.Now(), "onOpen")
	}
	this.ConnectionEstablished = Milliseconds()
	this.IsConnected = true
	// Signal connected channel
	this.Connected.(*Future).Resolve(true)
	this.ClearConnectionTimeout()
	this.SetPingInterval()
	if this.OnConnectedCallback != nil {
		this.OnConnectedCallback(this, nil)
	}
}

func (this *WSClient) Close() *Future {
	if this.Connection != nil {
		if this.Disconnected == nil {
			this.Disconnected = NewFuture()
		}
		this.Connection.Close()
		this.Connection = nil
	}
	return this.Disconnected.(*Future)
}

func (this *WSClient) Resolve(data interface{}, subHash interface{}) interface{} {
	return this.Client.Resolve(data, subHash)
}

func (this *WSClient) Future(messageHash interface{}) <-chan interface{} {
	return this.Client.Future(messageHash)
}

func (this *WSClient) Reject(err interface{}, messageHash ...interface{}) {
	this.Client.Reject(err, messageHash...)
}

func (this *WSClient) Send(message interface{}) <-chan interface{} {
	return this.Client.Send(message)
}

func (this *WSClient) Reset(err interface{}) {
	this.Client.Reset(err)
}

func (this *WSClient) OnPong() {
	this.Client.OnPong()
}

func (this *WSClient) GetError() error {
	return this.Client.GetError()
}

func (this *WSClient) SetError(err error) {
	this.Client.SetError(err)
}

func (this *WSClient) GetUrl() string {
	return this.Client.GetUrl()
}

func (this *WSClient) GetSubscriptions() map[string]interface{} {
	return this.Client.GetSubscriptions()
}
func (this *WSClient) GetLastPong() interface{} {
	return this.Client.GetLastPong()
}
func (this *WSClient) SetLastPong(lastPong interface{}) {
	this.Client.SetLastPong(lastPong)
}
func (this *WSClient) GetKeepAlive() interface{} {
	return this.Client.GetKeepAlive()
}
func (this *WSClient) SetKeepAlive(keepAlive interface{}) {
	this.Client.SetKeepAlive(keepAlive)
}
func (this *WSClient) GetFutures() map[string]interface{} {
	return this.Client.GetFutures()
}
