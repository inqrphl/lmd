package lmd

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeerSource(t *testing.T) {
	lmd := createTestLMDInstance()
	connection := Connection{Name: "Test", Source: []string{"http://localhost/test/", "http://clusternode/test"}}
	peer := NewPeer(lmd, &connection)

	require.Len(t, peer.source, 2)
	assert.Equal(t, "http://localhost/test/", peer.source[0])
	assert.Equal(t, "http://clusternode/test", peer.source[1])
}

func TestPeerHTTPComplete(t *testing.T) {
	assert.Equal(t, "http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost"))
	assert.Equal(t, "http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/"))
	assert.Equal(t, "http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/thruk/"))
	assert.Equal(t, "http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/thruk"))
	assert.Equal(t, "http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/thruk/cgi-bin/remote.cgi"))
	assert.Equal(t, "http://localhost/sitename/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/sitename"))
	assert.Equal(t, "http://localhost/sitename/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/sitename/"))
}

func TestPeerDeltaUpdate(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(t, 1, 10, 10)
	PauseTestPeers(peer)

	store := peer.data.Load()
	err := store.updateDelta(t.Context(), 0, 0)
	require.NoError(t, err)

	err = cleanup()
	require.NoError(t, err)
}

func TestPeerUpdateResume(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(t, 1, 10, 10)
	PauseTestPeers(peer)

	err := peer.resumeFromIdle(t.Context())
	require.NoError(t, err)

	err = cleanup()
	require.NoError(t, err)
}

func TestPeerInitSerial(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(t, 1, 10, 10)
	PauseTestPeers(peer)

	store := peer.data.Load()
	err := store.initAllTablesSerial(t.Context())
	require.NoError(t, err)

	err = cleanup()
	require.NoError(t, err)
}

func TestPeerInitChunked(t *testing.T) {
	conf := `
InitialSyncBlockSize = 100
	`
	peer, cleanup, _ := StartTestPeerExtra(t, 1, 10, 1000, conf)
	PauseTestPeers(peer)

	store := peer.data.Load()
	err := store.initAllTablesParallel(t.Context())
	require.NoError(t, err)

	err = cleanup()
	require.NoError(t, err)
}

func TestLMDPeerUpdate(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(t, 3, 10, 10)
	PauseTestPeers(peer)

	store := peer.data.Load()
	peer.lastUpdate.Set(0)
	peer.setFlag(LMD)
	peer.setFlag(MultiBackend)
	store.setSyncStrategy()
	assert.IsTypef(t, &SyncStrategyLMD{}, store.sync, "expected sync strategy to be LMD")

	_, err := peer.tryUpdate(t.Context())
	require.NoError(t, err)

	peer.lastUpdate.Set(0)
	peer.resetFlags()
	peer.setFlag(MultiBackend)
	store.setSyncStrategy()
	assert.IsTypef(t, &SyncStrategyMultiBackend{}, store.sync, "expected sync strategy to be MultiBackend")
	_, err = peer.tryUpdate(t.Context())
	require.NoError(t, err)

	err = cleanup()
	require.NoError(t, err)
}

func TestPeerLog(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(t, 1, 10, 10)
	PauseTestPeers(peer)

	peer.setBroken("test")
	peer.logPeerStatus(log.Debugf)
	err := peer.initTablesIfRestartRequiredError(t.Context(), fmt.Errorf("test"))
	require.Errorf(t, err, "got no error but expected broken peer")
	assert.Contains(t, err.Error(), "test")

	err = cleanup()
	require.NoError(t, err)
}

func TestPeerConnectionErrorImportMode(t *testing.T) {
	connErr := fmt.Errorf("connection error dial tcp 127.0.0.2:666: connect: connection refused")
	makePeer := func() *Peer {
		lmd := createTestLMDInstance()
		peer := NewPeer(lmd, &Connection{Name: "Test", Source: []string{"127.0.0.1:1"}})
		// last online long in the past, so a connection error would normally
		// mark the peer as offline and drop its data
		peer.peerState.Set(PeerStatusUp)
		peer.lastOnline.Set(currentUnixTime() - 1000)
		peer.data.Store(NewDataStoreSet(peer))

		return peer
	}

	// without import mode a failed connection takes the peer offline and drops its data
	peer := makePeer()
	peer.setNextAddrFromErr(connErr, nil, peer.source)
	require.Equalf(t, PeerStatusDown, peer.peerState.Get(), "peer without import mode should go offline")
	require.Nilf(t, peer.data.Load(), "peer without import mode should drop its data")

	// in import mode the same failure must be ignored, the peer stays online and keeps its data
	peer = makePeer()
	peer.lmd.flags.flagImport = "snapshot.export"
	importData := peer.data.Load()
	peer.setNextAddrFromErr(connErr, nil, peer.source)
	require.Equalf(t, PeerStatusUp, peer.peerState.Get(), "peer in import mode must stay online")
	assert.Samef(t, importData, peer.data.Load(), "peer in import mode must keep its imported data")
}
