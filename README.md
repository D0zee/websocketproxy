# websocket proxy
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/pretty66/websocketproxy)](https://github.com/IceFireDB/IceFireDB-Proxy/blob/master/go.mod)
### 100行代码实现轻量的websocket代理库，不依赖其他三方库，支持ws、wss代理

## 目录
- [安装](#安装)
- [使用](#使用)
- [测试](#测试)
- [核心流量转发代码](#核心流量转发代码)

### 安装
> go get github.com/pretty66/websocketproxy


### 使用
```go
import (
    "github.com/pretty66/websocketproxy"
    "net/http"
)

wp, err := websocketproxy.NewProxy("ws://82.157.123.54:9010/ajaxchattest", func(r *http.Request) error {
    // 权限验证
    r.Header.Set("Cookie", "----")
    // 伪装来源
    r.Header.Set("Origin", "http://82.157.123.54:9010")
    return nil
})
if err != nil {
    t.Fatal()
}
// 代理路径
http.HandleFunc("/wsproxy", wp.Proxy)
http.ListenAndServe(":9696", nil)
```

### 测试
运行test文件启动后监听`127.0.0.1:9696`端口，使用在线测试工具`http://coolaf.com/tool/chattest` 连接代理测试请求响应

#### 示例
![示例](ws_test.png)



### 核心流量转发代码
```go
func (wp *WebsocketProxy) Proxy(writer http.ResponseWriter, request *http.Request) {
    // 判断是否是websocket请求
	if strings.ToLower(request.Header.Get("Connection")) != "upgrade" ||
		strings.ToLower(request.Header.Get("Upgrade")) != "websocket" {
		_, _ = writer.Write([]byte(`Must be a websocket request`))
		return
	}
    // 劫持连接
	hijacker, ok := writer.(http.Hijacker)
	if !ok {
		return
	}
	conn, _, err := hijacker.Hijack()
	if err != nil {
		return
	}
	defer conn.Close()
    // 克隆请求，设置目标地址路径
	req := request.Clone(context.TODO())
	req.URL.Path, req.URL.RawPath, req.RequestURI = wp.defaultPath, wp.defaultPath, wp.defaultPath
	req.Host = wp.remoteAddr
    // 握手之前回调
	if wp.beforeHandshake != nil {
		// 增加头部，权限认证 + 伪装来源
		err = wp.beforeHandshake(req)
		if err != nil {
			_, _ = writer.Write([]byte(err.Error()))
			return
		}
	}
    // 判断协议，选择拨号流程
	var remoteConn net.Conn
	switch wp.scheme {
	case WsScheme:
		remoteConn, err = net.Dial("tcp", wp.remoteAddr)
	case WssScheme:
		remoteConn, err = tls.Dial("tcp", wp.remoteAddr, wp.tlsc)
	}
	if err != nil {
		_, _ = writer.Write([]byte(err.Error()))
		return
	}
	defer remoteConn.Close()
    // request请求转换为字节流
	b, _ := httputil.DumpRequest(req, false)
    // 向目标websocket服务发送握手包
	remoteConn.Write(b)
    // 流量透传
	errChan := make(chan error, 2)
	copyConn := func(a, b net.Conn) {
		_, err := io.Copy(a, b)
		errChan <- err
	}
	go copyConn(conn, remoteConn) // response
	go copyConn(remoteConn, conn) // request
	select {
	case err = <-errChan:
		if err != nil {
			log.Println(err)
		}
	}
}
```

## License
websocketproxy is under the Apache 2.0 license. See the [LICENSE](./LICENSE) directory for details.
