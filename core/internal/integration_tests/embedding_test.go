package integration_tests

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/apernet/hysteria/core/v2/client"
	"github.com/apernet/hysteria/core/v2/internal/integration_tests/mocks"
	"github.com/apernet/hysteria/core/v2/server"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type embeddedUDP struct {
	packets chan []byte
	done    chan struct{}
	once    sync.Once
}

func (u *embeddedUDP) ReadFrom(b []byte) (int, string, error) {
	select {
	case <-u.done:
		return 0, "", net.ErrClosed
	case p := <-u.packets:
		return copy(b, p), "example.com:53", nil
	}
}
func (u *embeddedUDP) WriteTo(b []byte, _ string) (int, error) {
	p := append([]byte(nil), b...)
	select {
	case <-u.done:
		return 0, net.ErrClosed
	case u.packets <- p:
		return len(b), nil
	}
}
func (u *embeddedUDP) Close() error { u.once.Do(func() { close(u.done) }); return nil }

func TestEmbeddingMetadataAndShutdown(t *testing.T) {
	require := require.New(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(err)
	tcpMeta := make(chan server.RequestMetadata, 1)
	udpMeta := make(chan server.RequestMetadata, 2)
	auth := mocks.NewMockAuthenticator(t)
	auth.EXPECT().Authenticate(mock.Anything, mock.Anything, mock.Anything).Return(true, "embed")
	s, err := server.NewServer(&server.Config{Conn: packet, TLSConfig: serverTLSConfig(), Authenticator: auth, StreamHandler: func(_ context.Context, m server.RequestMetadata, stream server.HyStream, _ string) {
		tcpMeta <- m
		_, _ = io.Copy(stream, stream)
	}, UDPHandler: func(_ context.Context, m server.RequestMetadata, _ string) (server.UDPConn, error) {
		udpMeta <- m
		return &embeddedUDP{packets: make(chan []byte, 2), done: make(chan struct{})}, nil
	}})
	require.NoError(err)
	defer s.Close()
	done := make(chan struct{})
	go func() { defer close(done); _ = s.Serve() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := client.NewClientContext(ctx, &client.Config{ServerAddr: packet.LocalAddr(), TLSConfig: client.TLSConfig{InsecureSkipVerify: true}})
	require.NoError(err)
	defer c.Close()
	conn, err := c.TCPContext(ctx, "example.com:80")
	require.NoError(err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Write([]byte("test"))
	require.NoError(err)
	buf := make([]byte, 4)
	_, err = io.ReadFull(conn, buf)
	require.NoError(err)
	m := <-tcpMeta
	source := m.Source.(*net.UDPAddr)
	local := conn.LocalAddr().(*net.UDPAddr)
	require.Equal(local.Port, source.Port)
	require.Equal(packet.LocalAddr().String(), m.Inbound.String())
	require.Equal("embed", m.AuthID)
	var last uint32
	for range 2 {
		u, err := c.UDP()
		require.NoError(err)
		defer u.Close()
		require.NoError(u.Send(make([]byte, 8192), "example.com:53"))
		p, _, err := u.Receive()
		require.NoError(err)
		require.Len(p, 8192)
		m := <-udpMeta
		require.NotZero(m.SessionID)
		require.NotEqual(last, m.SessionID)
		require.Equal(source.String(), m.Source.String())
		last = m.SessionID
	}
	require.NoError(s.Close())
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("Serve did not stop")
	}
	select {
	case <-c.Context().Done():
	case <-ctx.Done():
		t.Fatal("Close did not terminate authenticated connections")
	}
}
