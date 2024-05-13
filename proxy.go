package websocketproxy

import (
	"crypto/tls"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
)

const (
	wsScheme   = "ws"
	wssScheme  = "wss"
	tcpNetwork = "tcp"
	bufSize    = 1024 * 32
)

func isWebSocketReq(request *http.Request) bool {
	return strings.ToLower(request.Header.Get("Connection")) != "upgrade" ||
		strings.ToLower(request.Header.Get("Upgrade")) != "websocket"
}

func isWebSocketConn(url url.URL) bool {
	switch url.Scheme {
	case wsScheme, wssScheme:
		return true
	default:
		return false
	}
}

func getRemoteConn(url url.URL) (net.Conn, error) {
	switch url.Scheme {
	case wsScheme:
		return net.Dial(tcpNetwork, url.Host)
	case wssScheme:
		return tls.Dial(tcpNetwork, url.Host, &tls.Config{InsecureSkipVerify: true})
	default:
		panic(nil)
	}
}

func Proxy(writer http.ResponseWriter, request *http.Request, url url.URL) {
	if !isWebSocketReq(request) || !isWebSocketConn(url) {
		http.Error(writer, "Must be a websocket request", http.StatusForbidden)
		return
	}
	hijacker, ok := writer.(http.Hijacker)
	if !ok {
		return
	}
	conn, _, err := hijacker.Hijack()
	if err != nil {
		return
	}
	defer conn.Close()

	req := request.Clone(request.Context())
	req.URL.Path, req.URL.RawPath, req.RequestURI = url.Path, url.Path, url.Path
	req.Host = url.Host

	remoteConn, err := getRemoteConn(url)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	defer remoteConn.Close()

	err = req.Write(remoteConn)
	if err != nil {
		return
	}

	errChan := make(chan error, 2)
	copyConn := func(a, b net.Conn) {
		buf := ByteSliceGet(bufSize)
		defer ByteSlicePut(buf)

		_, err := io.CopyBuffer(a, b, buf)
		errChan <- err
	}
	go copyConn(conn, remoteConn)
	go copyConn(remoteConn, conn)
	select {
	case err = <-errChan:
		if err != nil {
			log.Println(err)
		}
	}
}
