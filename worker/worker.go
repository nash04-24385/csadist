package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sync"

	"uk.ac.bris.cs/gameoflife/gol"
)

type GOLWorker struct {
	mu sync.RWMutex

	// persistent configuration
	params gol.Params

	// global row range this worker owns: [startY, endY)
	startY int
	endY   int

	// local slice of the world (height = endY-startY, width = params.ImageWidth)
	localWorld [][]byte

	// halo rows received from neighbours (length = params.ImageWidth)
	topHalo    []byte
	bottomHalo []byte

	// neighbour worker addresses ("ip:port"), empty string if no neighbour
	aboveAddr string
	belowAddr string
}



// process the section the broker gives (baseline, full-world resync)
func (w *GOLWorker) ProcessSection(req gol.SectionRequest, res *gol.SectionResponse) error {
	p := req.Params
	world := req.World
	startY := req.StartY
	endY := req.EndY

	// call calculate state function on the requested section

	updatedSection := calculateNextStatesFullWorld(p, world, startY, endY)

	res.StartY = startY
	res.Section = updatedSection
	return nil
}


// Initialisation for halo-exchange mode
// InitialiseWorker is called once by the broker to give this worker its slice and neighbour addresses.
func (w *GOLWorker) InitialiseWorker(req gol.WorkerInitRequest, _ *struct{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.params = req.Params
	w.startY = req.StartY
	w.endY = req.EndY

	// Make a deep copy of the local slice so we own it
	h := req.EndY - req.StartY
	w.localWorld = make([][]byte, h)
	for i := 0; i < h; i++ {
		w.localWorld[i] = make([]byte, w.params.ImageWidth)
		copy(w.localWorld[i], req.LocalWorld[i])
	}

	w.topHalo = make([]byte, w.params.ImageWidth)
	w.bottomHalo = make([]byte, w.params.ImageWidth)

	w.aboveAddr = req.AboveAddr
	w.belowAddr = req.BelowAddr

	fmt.Printf("Worker [%d,%d) initialised. Above=%q Below=%q\n",
		w.startY, w.endY, w.aboveAddr, w.belowAddr)

	return nil
}


// Halo receiver RPCs (neighbours call these)
func (w *GOLWorker) SetTopHalo(req gol.HaloRow, _ *struct{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(req.Row) != w.params.ImageWidth {
		return fmt.Errorf("SetTopHalo: wrong row width %d, expected %d",
			len(req.Row), w.params.ImageWidth)
	}
	copy(w.topHalo, req.Row)
	return nil
}

func (w *GOLWorker) SetBottomHalo(req gol.HaloRow, _ *struct{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(req.Row) != w.params.ImageWidth {
		return fmt.Errorf("SetBottomHalo: wrong row width %d, expected %d",
			len(req.Row), w.params.ImageWidth)
	}
	copy(w.bottomHalo, req.Row)
	return nil
}

// ProcessHaloTurn performs one GoL iteration:
//  1. Send our boundary rows to neighbours (halo exchange).
//  2. Wait for neighbours to update our halo rows via SetTopHalo/SetBottomHalo.
//  3. Compute next local slice using localWorld + halos.
//  4. Return updated slice to broker.
func (w *GOLWorker) ProcessHaloTurn(_ struct{}, res *gol.SectionResponse) error {
	// Capture current boundaries + neighbour addresses under read lock
	w.mu.RLock()
	if w.localWorld == nil {
		w.mu.RUnlock()
		return fmt.Errorf("ProcessHaloTurn called before InitialiseWorker")
	}

	height := len(w.localWorld)
	width := w.params.ImageWidth

	topBoundary := make([]byte, width)
	bottomBoundary := make([]byte, width)
	copy(topBoundary, w.localWorld[0])
	copy(bottomBoundary, w.localWorld[height-1])

	aboveAddr := w.aboveAddr
	belowAddr := w.belowAddr
	startY := w.startY
	params := w.params
	w.mu.RUnlock()

	// Send our boundaries to neighbours (if they exist)
	var wg sync.WaitGroup

	if aboveAddr != "" {
		wg.Add(1)
		go func(addr string, row []byte) {
			defer wg.Done()
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				fmt.Println("ProcessHaloTurn: failed to dial above neighbour:", err)
				return
			}
			defer client.Close()

			req := gol.HaloRow{Row: row}
			var reply struct{}
			if err := client.Call("GOLWorker.SetBottomHalo", req, &reply); err != nil {
				fmt.Println("ProcessHaloTurn: SetBottomHalo RPC failed:", err)
			}
		}(aboveAddr, topBoundary)
	}

	if belowAddr != "" {
		wg.Add(1)
		go func(addr string, row []byte) {
			defer wg.Done()
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				fmt.Println("ProcessHaloTurn: failed to dial below neighbour:", err)
				return
			}
			defer client.Close()

			req := gol.HaloRow{Row: row}
			var reply struct{}
			if err := client.Call("GOLWorker.SetTopHalo", req, &reply); err != nil {
				fmt.Println("ProcessHaloTurn: SetTopHalo RPC failed:", err)
			}
		}(belowAddr, bottomBoundary)
	}

	wg.Wait()

	// Now that our halos should be set, compute next local slice
	w.mu.Lock()
	newLocal := calculateNextUsingHalos(params, w.localWorld, w.topHalo, w.bottomHalo)
	w.localWorld = newLocal

	// prepare response for broker
	res.StartY = startY

	// deep copy for safety
	res.Section = make([][]byte, len(newLocal))
	for i := range newLocal {
		res.Section[i] = make([]byte, width)
		copy(res.Section[i], newLocal[i])
	}
	w.mu.Unlock()

	return nil
}


// GoL logic

func calculateNextStatesFullWorld(p gol.Params, world [][]byte, startY, endY int) [][]byte {
	h := p.ImageHeight
	w := p.ImageWidth
	rows := endY - startY

	//make new grid section
	newRows := make([][]byte, rows)
	for i := 0; i < rows; i++ {
		newRows[i] = make([]byte, w)
	}

	for i := startY; i < endY; i++ {
		for j := 0; j < w; j++ { //accessing each individual cell

			count := 0
			up := (i - 1 + h) % h
			down := (i + 1) % h
			left := (j - 1 + w) % w
			right := (j + 1) % w

			if world[i][left] == 255 {
				count++
			}

			if world[i][right] == 255 {
				count++
			}

			if world[up][j] == 255 {
				count++
			}

			if world[down][j] == 255 {
				count++
			}

			if world[up][right] == 255 {
				count++
			}

			if world[up][left] == 255 {
				count++
			}

			if world[down][right] == 255 {
				count++
			}

			if world[down][left] == 255 {
				count++
			}

			if world[i][j] == 255 {
				if count == 2 || count == 3 {
					newRows[i-startY][j] = 255
				} else {
					newRows[i-startY][j] = 0
				}

			} else { 
				if count == 3 {
					newRows[i-startY][j] = 255
				} else {
					newRows[i-startY][j] = 0
				}
			}

		}
	}
	return newRows
}


// localWorld has shape [hLocal][w], topHalo & bottomHalo are length w.
func calculateNextUsingHalos(p gol.Params, localWorld [][]byte, topHalo, bottomHalo []byte) [][]byte {
	hLocal := len(localWorld)
	if hLocal == 0 {
		return nil
	}
	w := p.ImageWidth

	newLocal := make([][]byte, hLocal)
	for i := 0; i < hLocal; i++ {
		newLocal[i] = make([]byte, w)
	}

	for i := 0; i < hLocal; i++ {
		for j := 0; j < w; j++ {
			count := 0

			// compute indices with wrap in X
			left := (j - 1 + w) % w
			right := (j + 1) % w

			// figure out which row is "up" and "down" in the distributed sense
			var rowUp, rowDown []byte

			if i == 0 {
				rowUp = topHalo
			} else {
				rowUp = localWorld[i-1]
			}

			if i == hLocal-1 {
				rowDown = bottomHalo
			} else {
				rowDown = localWorld[i+1]
			}

			// neighbours:
			if localWorld[i][left] == 255 {
				count++
			}
			if localWorld[i][right] == 255 {
				count++
			}
			if rowUp[j] == 255 {
				count++
			}
			if rowDown[j] == 255 {
				count++
			}
			if rowUp[right] == 255 {
				count++
			}
			if rowUp[left] == 255 {
				count++
			}
			if rowDown[right] == 255 {
				count++
			}
			if rowDown[left] == 255 {
				count++
			}

			if localWorld[i][j] == 255 {
				if count == 2 || count == 3 {
					newLocal[i][j] = 255
				} else {
					newLocal[i][j] = 0
				}
			} else {
				if count == 3 {
					newLocal[i][j] = 255
				} else {
					newLocal[i][j] = 0
				}
			}
		}
	}

	return newLocal
}

// helper func to make worker shut down on keypress
func (w *GOLWorker) Shutdown(_ struct{}, _ *struct{}) error {
	fmt.Println("shutdown signal received, stopping worker.")
	go func() {
		os.Exit(0)
	}()
	return nil
}

func main() {
	worker := &GOLWorker{}

	if err := rpc.RegisterName("GOLWorker", worker); err != nil {
		fmt.Println("Error registering RPC:", err)
		return
	}

	port := os.Getenv("WORKER_PORT")
	if port == "" {
    	port = "8030" // default
	}
	listener, err := net.Listen("tcp4", "0.0.0.0:" + port)

	
	if err != nil {
		fmt.Println("Error starting listener:", err)
		os.Exit(1)
		return
	}
	fmt.Println("Worker listening on port 8030 (IPv4)...")

	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Connection error:", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}
