package hostdb

import (
	"crypto/rand"
	"errors"
	"math/big"
	"sync"

	"github.com/NebulousLabs/Sia/consensus"
	"github.com/NebulousLabs/Sia/sia/components"
)

type HostDB struct {
	hostTree      *hostNode
	activeHosts   map[string]*hostNode
	inactiveHosts map[string]*components.HostEntry

	dbLock sync.RWMutex
}

// New returns an empty HostDatabase.
func New() (hdb *HostDB) {
	hdb = &HostDB{
		activeHosts:   make(map[string]*hostNode),
		inactiveHosts: make(map[string]*components.HostEntry),
	}
	return
}

// Insert adds an entry to the hostdb.
func (hdb *HostDB) Insert(entry components.HostEntry) error {
	hdb.lock()
	defer hdb.unlock()

	_, exists := hdb.activeHosts[entry.ID]
	if exists {
		return errors.New("entry of given id already exists in host db")
	}

	if hdb.hostTree == nil {
		hdb.hostTree = createNode(nil, entry)
		hdb.activeHosts[entry.ID] = hdb.hostTree
	} else {
		_, hostNode := hdb.hostTree.insert(entry)
		hdb.activeHosts[entry.ID] = hostNode
	}
	return nil
}

// Remove deletes an entry from the hostdb.
func (hdb *HostDB) Remove(id string) error {
	hdb.lock()
	defer hdb.unlock()

	// See if the node is in the set of active hosts.
	node, exists := hdb.activeHosts[id]
	if !exists {
		// If the node is in the set of inactive hosts, delete from that set,
		// otherwise return a not found error.
		_, exists := hdb.inactiveHosts[id]
		if exists {
			delete(hdb.inactiveHosts, id)
			return nil
		} else {
			return errors.New("id not found in host database")
		}
	}

	// Delete the node from the active hosts, and remove it from the tree.
	delete(hdb.activeHosts, id)
	node.remove()

	return nil
}

// Update throws a bunch of blocks at the hostdb to be integrated.
//
// TODO: Check for repeat host announcements when parsing blocks.
func (hdb *HostDB) Update(initialStateHeight consensus.BlockHeight, rewoundBlocks []consensus.Block, appliedBlocks []consensus.Block) (err error) {
	hdb.lock()
	defer hdb.unlock()

	// Remove hosts found in blocks that were rewound. Because the hostdb is
	// like a stack, you can just pop the hosts and be certain that they are
	// the same hosts.
	for _, b := range rewoundBlocks {
		var entries []components.HostEntry
		entries, err = findHostAnnouncements(initialStateHeight, b)
		if err != nil {
			return
		}

		for _, entry := range entries {
			err = hdb.Remove(entry.ID)
			if err != nil {
				return
			}
		}
	}

	// Add hosts found in blocks that were applied.
	for _, b := range appliedBlocks {
		var entries []components.HostEntry
		entries, err = findHostAnnouncements(initialStateHeight, b)
		if err != nil {
			return
		}

		for _, entry := range entries {
			hdb.unlock()
			err = hdb.Insert(entry)
			hdb.lock()
			if err != nil {
				return
			}
		}
	}

	return
}

// RandomHost pulls a random host from the hostdb weighted according to
// whatever internal metrics exist within the hostdb.
func (hdb *HostDB) RandomHost() (h components.HostEntry, err error) {
	hdb.rLock()
	defer hdb.rUnlock()
	if len(hdb.activeHosts) == 0 {
		err = errors.New("no hosts found")
		return
	}

	// Get a random number between 0 and state.TotalWeight and then scroll
	// through state.HostList until at least that much weight has been passed.
	randInt, err := rand.Int(rand.Reader, big.NewInt(int64(hdb.hostTree.weight)))
	if err != nil {
		return
	}
	randWeight := consensus.Currency(randInt.Int64())
	return hdb.hostTree.entryAtWeight(randWeight)
}
