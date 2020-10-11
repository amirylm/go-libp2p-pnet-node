package storage

import (
	"errors"
	"github.com/amirylm/libp2p-facade/core"
	"github.com/ipfs/go-bitswap"
	"github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-blockservice"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exoffline "github.com/ipfs/go-ipfs-exchange-offline"
	provider "github.com/ipfs/go-ipfs-provider"
	"github.com/ipfs/go-ipfs-provider/queue"
	"github.com/ipfs/go-ipfs-provider/simple"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"time"
)

const (
	defaultReprovideInterval = 8 * time.Hour
)

// StoragePeer represents a peer with storage capabilities
type StoragePeer interface {
	core.LibP2PPeer

	DagService() ipld.DAGService
	BlockService() blockservice.BlockService
	Reprovider() provider.System
}

// storagePeer is the implementation of StoragePeer interface
type storagePeer struct {
	ipld.DAGService

	core.LibP2PPeer

	bsrv       blockservice.BlockService
	reprovider provider.System
}

func NewStoragePeer(base *core.BasePeer, offline bool) StoragePeer {
	if offline {
		dag, bsrv, _, _ := CreateOfflineDagServices(base)
		repro := provider.NewOfflineProvider()
		node := storagePeer{dag, base, bsrv, repro}
		return &node
	}
	dag, bsrv, bstore, err := CreateDagServices(base)
	if err != nil {
		base.Logger().Panic("could not create DAG services")
		return nil
	}
	repro, err := SetupReprovider(base, bstore, defaultReprovideInterval)
	node := storagePeer{dag, base, bsrv, repro}
	return &node
}

func CreateDagServices(base core.LibP2PPeer) (ipld.DAGService, blockservice.BlockService, blockstore.Blockstore, error) {
	var bsrv blockservice.BlockService
	var bs blockstore.Blockstore

	bs = blockstore.NewBlockstore(base.Store())
	bs = blockstore.NewIdStore(bs)
	bs, _ = blockstore.CachedBlockstore(base.Context(), bs, blockstore.DefaultCacheOpts())

	bswapnet := network.NewFromIpfsHost(base.Host(), base.DHT())
	bswap := bitswap.New(base.Context(), bswapnet, bs)
	bsrv = blockservice.New(bs, bswap)

	return merkledag.NewDAGService(bsrv), bsrv, bs, nil
}

func CreateOfflineDagServices(base core.LibP2PPeer) (ipld.DAGService, blockservice.BlockService, blockstore.Blockstore, error) {
	bs := blockstore.NewBlockstore(base.Store())
	bs = blockstore.NewIdStore(bs)
	bs, _ = blockstore.CachedBlockstore(base.Context(), bs, blockstore.DefaultCacheOpts())

	bsrv := blockservice.New(bs, exoffline.Exchange(bs))

	return merkledag.NewDAGService(bsrv), bsrv, bs, nil
}

func SetupReprovider(base core.LibP2PPeer, bstore blockstore.Blockstore, reprovideInterval time.Duration) (provider.System, error) {
	queue, err := queue.NewQueue(base.Context(), "repro", base.Store())
	if err != nil {
		return nil, err
	}

	prov := simple.NewProvider(
		base.Context(),
		queue,
		base.DHT(),
	)

	reprov := simple.NewReprovider(
		base.Context(),
		reprovideInterval,
		base.DHT(),
		simple.NewBlockstoreProvider(bstore),
	)

	reprovider := provider.NewSystem(prov, reprov)
	reprovider.Run()

	return reprovider, nil
}

// Session returns a session-based NodeGetter.
func Session(sp StoragePeer) ipld.NodeGetter {
	dag := sp.DagService()
	ng := merkledag.NewSession(sp.Context(), dag)
	if ng == dag {
		sp.Logger().Warn("DAGService does not support sessions")
	}
	return ng
}

func (sp *storagePeer) DagService() ipld.DAGService {
	return sp
}

func (sp *storagePeer) BlockService() blockservice.BlockService {
	return sp.bsrv
}

func (sp *storagePeer) Reprovider() provider.System {
	return sp.reprovider
}

func (sp *storagePeer) Close() error {
	errs := []error{}

	if err := sp.reprovider.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := sp.bsrv.Close(); err != nil {
		errs = append(errs, err)
	}

	baseErrs := core.Close(sp)

	if len(errs) == 0 {
		if len(baseErrs) == 0 {
			return nil
		}
	}
	return errors.New("could not close peer")
}
