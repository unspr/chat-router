package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

var socketPath = filepath.Join(os.TempDir(), "chat-router.sock")

func main() {
	logFile, err := os.OpenFile("server.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("打开日志文件失败: %v", err)
	}
	defer logFile.Close()

	log.SetOutput(logFile)

	log.Println(socketPath)
	if err := os.RemoveAll(socketPath); err != nil {
		log.Fatal(err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("监听失败: %v", err)
	}
	defer listener.Close()

	// 优雅退出：捕获系统信号，程序结束时安全删除套接字文件
	cleanupSig(listener)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("接受连接错误: %v", err)
			continue
		}

		Init()
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 1024)
	n, _ := conn.Read(buf)

	index := bytes.IndexByte(buf, '\n')

	sessionId := string(buf[:index])
	message := string(buf[index+1 : n])
	log.Printf("\n收到客户端消息: seesionId %s message %s", sessionId, message)

	ch := GetSessionChannel(sessionId, message)
	for msg := range ch {
		output := fmt.Sprintln(msg)
		conn.Write([]byte(output))
	}
}

func cleanupSig(l net.Listener) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		l.Close()
		os.Remove(socketPath)
		os.Exit(0)
	}()
}
