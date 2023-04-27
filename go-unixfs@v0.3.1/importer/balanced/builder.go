// Package balanced provides methods to build balanced DAGs, which are generalistic
// DAGs in which all leaves (nodes representing chunks of data) are at the same
// distance from the root. Nodes can have only a maximum number of children; to be
// able to store more leaf data nodes balanced DAGs are extended by increasing its
// depth (and having more intermediary nodes).
//
// Internal nodes are always represented by UnixFS nodes (of type `File`) encoded
// inside DAG nodes (see the `go-unixfs` package for details of UnixFS). In
// contrast, leaf nodes with data have multiple possible representations: UnixFS
// nodes as above, raw nodes with just the file data (no format) and Filestore
// nodes (that directly link to the file on disk using a format stored on a raw
// node, see the `go-ipfs/filestore` package for details of Filestore.)
//
// In the case the entire file fits into just one node it will be formatted as a
// (single) leaf node (without parent) with the possible representations already
// mentioned. This is the only scenario where the root can be of a type different
// that the UnixFS node.
//
// Notes:
// 1. In the implementation. `FSNodeOverDag` structure is used for representing
//    the UnixFS node encoded inside the DAG node.
//    (see https://github.com/ipfs/go-ipfs/pull/5118.)
// 2. `TFile` is used for backwards-compatibility. It was a bug causing the leaf
//    nodes to be generated with this type instead of `TRaw`. The former one
//    should be used (like the trickle builder does).
//    (See https://github.com/ipfs/go-ipfs/pull/5120.)
//
//                                                 +-------------+
//                                                 |   Root 4    |
//                                                 +-------------+
//                                                       |
//                            +--------------------------+----------------------------+
//                            |                                                       |
//                      +-------------+                                         +-------------+
//                      |   Node 2    |                                         |   Node 5    |
//                      +-------------+                                         +-------------+
//                            |                                                       |
//              +-------------+-------------+                           +-------------+
//              |                           |                           |
//       +-------------+             +-------------+             +-------------+
//       |   Node 1    |             |   Node 3    |             |   Node 6    |
//       +-------------+             +-------------+             +-------------+
//              |                           |                           |
//       +------+------+             +------+------+             +------+
//       |             |             |             |             |
//  +=========+   +=========+   +=========+   +=========+   +=========+
//  | Chunk 1 |   | Chunk 2 |   | Chunk 3 |   | Chunk 4 |   | Chunk 5 |
//  +=========+   +=========+   +=========+   +=========+   +=========+
//
package balanced

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"time"

	ft "github.com/ipfs/go-unixfs"
	h "github.com/ipfs/go-unixfs/importer/helpers"

	// blocks "github.com/ipfs/go-block-format"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"

	// dshelp "github.com/ipfs/go-ipfs-ds-help"
	dshelp "github.com/ipfs/go-ipfs-ds-help"

	ipld "github.com/ipfs/go-ipld-format"
	merkledag "github.com/ipfs/go-merkledag"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
)

var fillNodeRec_Count = 0
var ChunkSize int64 = 1024 * 256
var ChildLinkCount = 174

// var NumThread = 32

// var IPFS_Path = "/home/mssong/.ipfs"

// var IPFS_Path = "/mnt/nvme0n1/.ipfs"

func encode(key datastore.Key) (dirPath, fileName string) {
	// dir: /home/mssong/.ipfs/blocks/7P path: /home/mssong/.ipfs/blocks/7P/CIQKGXY65BAIM2G5C64GRJOK2SGPDAXNG5VGHVHW7KFQ3PFFF5YH7PI.data
	extension := ".data"
	noslash := key.String()[1:]
	// datastorePath := "/home/mssong/.ipfs/blocks"
	datastorePath := merkledag.IPFS_Path + "/blocks"

	dirPath = filepath.Join(datastorePath, noslash[len(noslash)-3:len(noslash)-1])
	fileName = noslash + extension
	return dirPath, fileName
}

func makeDAG(db *ihelper.DagBuilderHelper, level int, depthNodeCount []int, childNode []ipld.Node, childFileSize []uint64, lastChildIdx int, newFileNonLeaf *[]cid.Cid) (ipld.Node, uint64, error) {
	// fmt.Println("@@@makeDAG")
	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}
	filledNode := make([]ipld.Node, depthNodeCount[level])
	nodeFileSize := make([]uint64, depthNodeCount[level])
	for i := 0; i < depthNodeCount[level]; i++ {
		// fmt.Println("depthNodeCount[level]:", depthNodeCount[level])
		// fmt.Println("len(childNode):",len(childNode))
		// fmt.Println("len(childFileSize):",len(childFileSize))
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			node := db.NewFSNodeOverDag(ft.TFile)
			for idx := i * ChildLinkCount; idx < (i+1)*ChildLinkCount; idx++ {
				if i == depthNodeCount[level]-1 && idx > lastChildIdx {
					break
				}
				st1 := time.Now()
				// fmt.Println("i=", i, "idx =", idx)
				err := node.AddChild_mansub(childNode[idx], childFileSize[idx], db, level)
				elap1 := time.Since(st1)
				elap1 = elap1
				// fmt.Println("elap1:", elap1)

				if err != nil {
					panic("mssong - err := node.AddChild(childNode[idx], childFileSize[idx], db)")
				}
				nodeFileSize[i] = node.FileSize()
				filledNode[i], err = node.Commit()
				// node.dag.SetData(node.file.GetByes())
				// fmt.Printf("newFileNonLeaf_0:%#v\n", newFileNonLeaf)
				if err != nil {
					panic("mssong - filledNode[i], err = node.Commit()")
				}
			}
			mutex.Lock()
			// filledNode[i] 만들어지는 순서가 i순서에 맞지 않음
			*newFileNonLeaf = append(*newFileNonLeaf, filledNode[i].Cid())
			// fmt.Println("newFileNonLeaf:", filledNode[i])
			mutex.Unlock()

		}(i)
	}
	wg.Wait()

	if level+1 == len(depthNodeCount) {
		return filledNode[0], nodeFileSize[0], nil
	}
	fn, nfs, err := makeDAG(db, level+1, depthNodeCount, filledNode, nodeFileSize, depthNodeCount[level]-1, newFileNonLeaf)
	filledNode = nil
	nodeFileSize = nil
	return fn, nfs, err

}

// Layout builds a balanced DAG layout. In a balanced DAG of depth 1, leaf nodes
// with data are added to a single `root` until the maximum number of links is
// reached. Then, to continue adding more data leaf nodes, a `newRoot` is created
// pointing to the old `root` (which will now become and intermediary node),
// increasing the depth of the DAG to 2. This will increase the maximum number of
// data leaf nodes the DAG can have (`Maxlinks() ^ depth`). The `fillNodeRec`
// function will add more intermediary child nodes to `newRoot` (which already has
// `root` as child) that in turn will have leaf nodes with data added to them.
// After that process is completed (the maximum number of links is reached),
// `fillNodeRec` will return and the loop will be repeated: the `newRoot` created
// will become the old `root` and a new root will be created again to increase the
// depth of the DAG. The process is repeated until there is no more data to add
// (i.e. the DagBuilderHelper’s Done() function returns true).
//
// The nodes are filled recursively, so the DAG is built from the bottom up. Leaf
// nodes are created first using the chunked file data and its size. The size is
// then bubbled up to the parent (internal) node, which aggregates all the sizes of
// its children and bubbles that combined size up to its parent, and so on up to
// the root. This way, a balanced DAG acts like a B-tree when seeking to a byte
// offset in the file the graph represents: each internal node uses the file size
// of its children as an index when seeking.
//
//      `Layout` creates a root and hands it off to be filled:
//
//             +-------------+
//             |   Root 1    |
//             +-------------+
//                    |
//       ( fillNodeRec fills in the )
//       ( chunks on the root.      )
//                    |
//             +------+------+
//             |             |
//        + - - - - +   + - - - - +
//        | Chunk 1 |   | Chunk 2 |
//        + - - - - +   + - - - - +
//
//                           ↓
//      When the root is full but there's more data...
//                           ↓
//
//             +-------------+
//             |   Root 1    |
//             +-------------+
//                    |
//             +------+------+
//             |             |
//        +=========+   +=========+   + - - - - +
//        | Chunk 1 |   | Chunk 2 |   | Chunk 3 |
//        +=========+   +=========+   + - - - - +
//
//                           ↓
//      ...Layout's job is to create a new root.
//                           ↓
//
//                            +-------------+
//                            |   Root 2    |
//                            +-------------+
//                                  |
//                    +-------------+ - - - - - - - - +
//                    |                               |
//             +-------------+            ( fillNodeRec creates the )
//             |   Node 1    |            ( branch that connects    )
//             +-------------+            ( "Root 2" to "Chunk 3."  )
//                    |                               |
//             +------+------+             + - - - - -+
//             |             |             |
//        +=========+   +=========+   + - - - - +
//        | Chunk 1 |   | Chunk 2 |   | Chunk 3 |
//        +=========+   +=========+   + - - - - +
//
func Layout(db *h.DagBuilderHelper, fileAbsPath string) (ipld.Node, error) {
	logDebug := false
	optFlag := true
	if logDebug {
		fmt.Println("fileAbsPath:", fileAbsPath)
	}
	var newFileLeaf []cid.Cid

	if fileAbsPath != "" && optFlag {
		st := time.Now()
		depthNodeCount := make([]int, 1)

		fileInfo, err := os.Stat(fileAbsPath)
		if err != nil {
			panic(err)
		}
		if logDebug {
			fmt.Println("exist:", fileAbsPath)
		}

		fileSize := fileInfo.Size()
		// fmt.Println("fileSize:", fileSize)
		var levelNodeCount int
		if fileSize%ChunkSize != 0 {
			levelNodeCount = int(fileSize/ChunkSize) + 1
		} else {
			levelNodeCount = int(fileSize / ChunkSize)
		}
		depthNodeCount[0] = levelNodeCount //leaf node

		var root ipld.Node
		if levelNodeCount == 1 { // only one leaf node == root node
			root, _, err := db.NewLeafDataNode(ft.TFile)
			if err != nil {
				return nil, err
			}
			return root, db.Add(root)
		}

		// fmt.Println("levelNodeCount:", len(depthNodeCount), levelNodeCount)

		// depthNodeCount = NPD node
		for {
			childLinks := int64(ChildLinkCount)
			if int(fileSize%childLinks) != 0 {
				levelNodeCount = levelNodeCount/int(childLinks) + 1
				// fmt.Println("levelNodeCount:", len(depthNodeCount), levelNodeCount)
				if levelNodeCount == 1 {
					depthNodeCount = append(depthNodeCount, 1) //root node
					break
				}
			} else {
				levelNodeCount = levelNodeCount / int(childLinks)
				// fmt.Println("levelNodeCount:", len(depthNodeCount), levelNodeCount)
			}
			depthNodeCount = append(depthNodeCount, levelNodeCount)
		}

		leafNode := make([]ipld.Node, depthNodeCount[0])
		leafFileSize := make([]uint64, depthNodeCount[0])

		newFileLeaf = make([]cid.Cid, depthNodeCount[0])

		var numTh int
		var size int
		if merkledag.NumThread == 1 {
			size = depthNodeCount[0]
		} else {

			size = depthNodeCount[0] / (merkledag.NumThread - 1)
		}
		if size == 0 || size == 1 {
			if size == 0 {
				size = 1
			}
			numTh = depthNodeCount[0]
		} else {
			numTh = depthNodeCount[0] / size
			if depthNodeCount[0]%size != 0 {
				numTh = numTh + 1
			}
		}
		if logDebug {
			fmt.Println("numTh:", numTh, "depthNodeCount[0]:", depthNodeCount[0], "size:", size)
		}

		var wg sync.WaitGroup

		tempPath := merkledag.IPFS_Path + "/blocks/temp"
		os.Mkdir(tempPath, 0755)

		for i := 0; i < numTh; i++ {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()
				f, err := os.Open(fileAbsPath)
				if err != nil {
					panic(err)
				}
				offset := int64(i) * int64(size) * ChunkSize
				f.Seek(offset, 0)
				full := make([]byte, int64(size)*ChunkSize)

				n, err := io.ReadFull(f, full)
				if err == io.ErrUnexpectedEOF {
					small := make([]byte, n)
					copy(small, full)
					full = small
				}

				idx := i * size
				fileDataSize := len(full)
				count := fileDataSize / int(ChunkSize)

				if count == 0 {
					var err error
					leafNode[idx], leafFileSize[idx], err = db.NewLeafDataNode_mansub(full, ft.TFile)

					if err != nil {
						log.Fatal("mssong: leafNode[i], leafFileSize[i], err = db.NewLeafDataNode_mansub(ft.TFile, fileCidIdx)")
					}
					newFileLeaf[idx] = leafNode[idx].Cid()
					err = db.Add(leafNode[idx])
					if err != nil {
						panic(err)
					}
				} else {
					if fileDataSize%int(ChunkSize) != 0 {
						count = count + 1
					}

					for j := 0; j < count; j++ {
						start := j * int(ChunkSize)
						end := (j + 1) * int(ChunkSize)
						if end >= fileDataSize {
							end = fileDataSize
							// ssFlag = true
						}
						var err error

						// leafNode[idx+j], leafFileSize[idx+j], err = db.NewLeafDataNode_mansub(full[start:end], ft.TFile) //original

						leafNode[idx+j], leafFileSize[idx+j], err = db.NewLeafDataNode_mansub(full[start:end], ft.TFile)

						if err != nil {
							log.Fatal("mssong: leafNode[i], leafFileSize[i], err = db.NewLeafDataNode_mansub(ft.TFile, fileCidIdx)")
						}

						// err = db.Add_mansub(leafNode[idx+j]) // 이거 대신 우리는 file write 병렬처리함 ! 이거 쓰면 중간에 멈춤

						///////////////////////////

						newFileLeaf[idx+j] = leafNode[idx+j].Cid()

						dsKey := dshelp.MultihashToDsKey(newFileLeaf[idx+j].Hash())
						dirPath, fileName := encode(dsKey)

						os.Mkdir(dirPath, 0755)
						filePath := dirPath + "/" + fileName
						f, err := os.Create(filePath)
						if err != nil {
							panic(err)
						}

						tempFile := tempPath + fileName
						f, err = os.Create(tempFile)
						if err != nil {
							panic(err)
						}

						f.Write(blocks.Block(leafNode[idx+j]).RawData())

						if err != nil {
							panic(err)
						}
					}
					full = nil
				}

			}(i)

		}
		wg.Wait()

		// fmt.Println(":sleep")
		// time.Sleep(10 * time.Second)

		newFileNonLeaf := make([]cid.Cid, 0)
		st_1 := time.Now()
		root, _, err = makeDAG(db, 1, depthNodeCount, leafNode, leafFileSize, depthNodeCount[0]-1, &newFileNonLeaf)
		st_1 = st_1
		// fmt.Println("makeDag elap:", time.Since(st_1))

		/*mssong - error when we executes the concurrent requests... for this map structure */
		merkledag.PinBufferMutex.Lock()
		dagCid := merkledag.NewTierCid()
		dagCid.NonLeaf = append(dagCid.NonLeaf, newFileNonLeaf...)
		dagCid.Leaf = append(dagCid.Leaf, newFileLeaf...)
		merkledag.PinBuffer[root.Cid()] = dagCid
		merkledag.PinBufferMutex.Unlock()
		//

		// fmt.Println("newFileNonLeaf:", len(newFileNonLeaf), "leaf:", len(newFileLeaf))
		// dagTeirCid := new(merkledag.TierCid)
		// dagCid := merkledag.NewTierCid()
		// dagCid.NonLeaf = append(dagCid.NonLeaf, newFileNonLeaf...)
		// dagCid.Leaf = append(dagCid.Leaf, newFileLeaf...)
		// merkledag.PinBufferMutex.Lock()
		// merkledag.PinBuffer[root.Cid()] = dagCid
		// fmt.Printf("PinBuffer:%#v\n", merkledag.PinBuffer)
		// merkledag.PinBufferMutex.UnLock()

		// merkledag.PrintPinBuffer(root.Cid())
		if logDebug {
			fmt.Println("root CID:", root.Cid())
		}
		elap := time.Since(st)
		elap = elap
		if logDebug {
			fmt.Println("layout elap:", elap)
		}

		leafNode = nil
		leafFileSize = nil
		newFileNonLeaf = nil
		newFileNonLeaf = nil
		depthNodeCount = nil

		debug.SetGCPercent(5)
		return root, db.Add(root)
	} else {
		if db.Done() {
			// No data, return just an empty node.
			root, err := db.NewLeafNode(nil, ft.TFile)
			if err != nil {
				return nil, err
			}
			// This works without Filestore support (`ProcessFileStore`).
			// TODO: Why? Is there a test case missing?

			return root, db.Add(root)
		}

		// The first `root` will be a single leaf node with data
		// (corner case), after that subsequent `root` nodes will
		// always be internal nodes (with a depth > 0) that can
		// be handled by the loop.
		root, fileSize, err := db.NewLeafDataNode(ft.TFile)
		if err != nil {
			return nil, err
		}

		// Each time a DAG of a certain `depth` is filled (because it
		// has reached its maximum capacity of `db.Maxlinks()` per node)
		// extend it by making it a sub-DAG of a bigger DAG with `depth+1`.
		for depth := 1; !db.Done(); depth++ {

			// Add the old `root` as a child of the `newRoot`.
			newRoot := db.NewFSNodeOverDag(ft.TFile)
			newRoot.AddChild(root, fileSize, db)

			// Fill the `newRoot` (that has the old `root` already as child)
			// and make it the current `root` for the next iteration (when
			// it will become "old").
			root, fileSize, err = fillNodeRec(db, newRoot, depth)
			if err != nil {
				return nil, err
			}
		}
		if logDebug {
			fmt.Println("root CID:", root.Cid())
		}
		return root, db.Add(root)
	}

}

// fillNodeRec will "fill" the given internal (non-leaf) `node` with data by
// adding child nodes to it, either leaf data nodes (if `depth` is 1) or more
// internal nodes with higher depth (and calling itself recursively on them
// until *they* are filled with data). The data to fill the node with is
// provided by DagBuilderHelper.
//
// `node` represents a (sub-)DAG root that is being filled. If called recursively,
// it is `nil`, a new node is created. If it has been called from `Layout` (see
// diagram below) it points to the new root (that increases the depth of the DAG),
// it already has a child (the old root). New children will be added to this new
// root, and those children will in turn be filled (calling `fillNodeRec`
// recursively).
//
//                      +-------------+
//                      |   `node`    |
//                      |  (new root) |
//                      +-------------+
//                            |
//              +-------------+ - - - - - - + - - - - - - - - - - - +
//              |                           |                       |
//      +--------------+             + - - - - -  +           + - - - - -  +
//      |  (old root)  |             |  new child |           |            |
//      +--------------+             + - - - - -  +           + - - - - -  +
//              |                          |                        |
//       +------+------+             + - - + - - - +
//       |             |             |             |
//  +=========+   +=========+   + - - - - +    + - - - - +
//  | Chunk 1 |   | Chunk 2 |   | Chunk 3 |    | Chunk 4 |
//  +=========+   +=========+   + - - - - +    + - - - - +
//
// The `node` to be filled uses the `FSNodeOverDag` abstraction that allows adding
// child nodes without packing/unpacking the UnixFS layer node (having an internal
// `ft.FSNode` cache).
//
// It returns the `ipld.Node` representation of the passed `node` filled with
// children and the `nodeFileSize` with the total size of the file chunk (leaf)
// nodes stored under this node (parent nodes store this to enable efficient
// seeking through the DAG when reading data later).
//
// warning: **children** pinned indirectly, but input node IS NOT pinned.
func fillNodeRec(db *h.DagBuilderHelper, node *h.FSNodeOverDag, depth int) (filledNode ipld.Node, nodeFileSize uint64, err error) {
	if depth < 1 {
		return nil, 0, errors.New("attempt to fillNode at depth < 1")
	}

	if node == nil {
		node = db.NewFSNodeOverDag(ft.TFile)
	}

	// Child node created on every iteration to add to parent `node`.
	// It can be a leaf node or another internal node.
	var childNode ipld.Node
	// File size from the child node needed to update the `FSNode`
	// in `node` when adding the child.
	var childFileSize uint64

	// While we have room and there is data available to be added.
	for node.NumChildren() < db.Maxlinks() && !db.Done() {

		if depth == 1 {
			// Base case: add leaf node with data.
			childNode, childFileSize, err = db.NewLeafDataNode(ft.TFile)
			if err != nil {
				return nil, 0, err
			}
		} else {
			// Recursion case: create an internal node to in turn keep
			// descending in the DAG and adding child nodes to it.
			childNode, childFileSize, err = fillNodeRec(db, nil, depth-1)
			if err != nil {
				return nil, 0, err
			}
		}

		err = node.AddChild(childNode, childFileSize, db)
		if err != nil {
			return nil, 0, err
		}
	}

	nodeFileSize = node.FileSize()

	// Get the final `dag.ProtoNode` with the `FSNode` data encoded inside.
	filledNode, err = node.Commit()
	if err != nil {
		return nil, 0, err
	}

	return filledNode, nodeFileSize, nil
}
