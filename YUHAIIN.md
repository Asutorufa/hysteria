# yuhaiin embedding patch

This branch follows HyNetworks/hysteria master at ca8fbd874fd6ca82413948de6dc5924405e103e1. It retains upstream module paths, so yuhaiin pins this fork with a module replacement. QUIC remains github.com/apernet/quic-go at upstream's pinned revision.

The additive core API supplies authenticated source/local addresses and UDP session IDs to an embedding proxy. `StreamHandler` delegates a TCP stream after a successful protocol response; the embedding proxy owns routing, sniffing, accounting and relay. Consequently target dial failures are reported by closing the stream, like an early-accepted inbound, rather than a Hysteria rejection response. `UDPHandler` receives one callback per UDP session and returns its packet I/O. Both callbacks are optional: omitting them preserves the original Outbound behavior.

`NewClientContext` and `ContextClient.TCPContext` make setup cancellable without extending cancellation to successfully established streams. Server shutdown closes accepted QUIC connections and joins TCP request handlers before releasing transport resources. Oversized UDP messages trigger fragmentation instead of silently disappearing when the serialization buffer is too small; the server receives complete datagrams before fragmenting replies.

When updating upstream: merge upstream master into this branch, review these four modified core files and run `GOWORK=off go test -race ./...` inside core. Run yuhaiin's Hysteria integration and official interoperability/throughput harness before updating its pinned module replacement. Do not follow a moving branch in production dependencies.
