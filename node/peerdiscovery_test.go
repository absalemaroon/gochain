package node_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/volodymyrprokopyuk/go-blockchain/node"
	"github.com/volodymyrprokopyuk/go-blockchain/node/rpc"
	"google.golang.org/grpc"
)

func createPeerDiscovery(
  ctx context.Context, wg *sync.WaitGroup, bootstrap, start bool,
) *node.PeerDiscovery {
  var peerDiscCfg node.PeerDiscoveryCfg
  if bootstrap {
    peerDiscCfg = node.PeerDiscoveryCfg{NodeAddr: bootAddr, Bootstrap: true}
  } else {
    peerDiscCfg = node.PeerDiscoveryCfg{NodeAddr: nodeAddr, SeedAddr: bootAddr}
  }
  peerDisc := node.NewPeerDiscovery(ctx, wg, peerDiscCfg)
  if start {
    wg.Add(1)
    go peerDisc.DiscoverPeers(100 * time.Millisecond)
  }
  return peerDisc
}

func TestPeerDiscovery(t *testing.T) {
  ctx, cancel := context.WithCancel(context.Background())
  defer cancel()
  wg := new(sync.WaitGroup)
  // Create the peer discovery without staring for the bootstrap node
  bootPeerDisc := createPeerDiscovery(ctx, wg, true, false)
  // Start the gRPC server on the bootstrap node
  grpcStartSvr(t, bootAddr, func(grpcSrv *grpc.Server) {
    node := rpc.NewNodeSrv(bootPeerDisc, nil)
    rpc.RegisterNodeServer(grpcSrv, node)
  })
  // Create and start the peer discovery for the new node
  nodePeerDisc := createPeerDiscovery(ctx, wg, false, true)
  // Wait for the peer discovery to discover peers
  time.Sleep(150 * time.Millisecond)
  // Verify that the bootstrap node and the new node have discovered each other
  if !slices.Contains(bootPeerDisc.Peers(), nodeAddr) {
    t.Errorf("node address %v is not in bootstrap known peers", nodeAddr)
  }
  if !slices.Contains(nodePeerDisc.Peers(), bootAddr) {
    t.Errorf("bootstrap address %v is not in node known peers", bootAddr)
  }
}

func TestRemovalOfUnhealthyPeer(t *testing.T) {
  wg := new(sync.WaitGroup)
  // Create the peer discovery without staring for the bootstrap node
  bootPeerDisc := createPeerDiscovery(t.Context(), wg, true, false)
  // Start the gRPC server on the bootstrap node
  waitForPeersDiscovery := make(chan struct{})
  grpcStartSvr(t, bootAddr, func(grpcSrv *grpc.Server) {
    node := rpc.NewNodeSrv(bootPeerDisc, nil)
    rpc.RegisterNodeServer(grpcSrv, node)

    go func() {
      <-waitForPeersDiscovery
      grpcSrv.Stop()
    }()
  })
  // Create and start the peer discovery for the new node
  nodePeerDisc := createPeerDiscovery(t.Context(), wg, false, true)
  // Wait for the peer discovery to discover peers
  time.Sleep(150 * time.Millisecond)
  // Verify that the bootstrap node and the new node have discovered each other

  if !slices.Contains(bootPeerDisc.Peers(), nodeAddr) {
    t.Errorf("node address %v is not in bootstrap known peers", nodeAddr)
  }
  if !slices.Contains(nodePeerDisc.Peers(), bootAddr) {
    t.Errorf("bootstrap address %v is not in node known peers", bootAddr)
  }

  waitForPeersDiscovery <- struct{}{}
  // Wait for the peer discovery to try to discover peers
  time.Sleep(250 * time.Millisecond)

  if len(nodePeerDisc.Peers()) != 0 {
    t.Errorf("node peer discovery should not have any peers after the bootstrap node is stopped")
  }
}
