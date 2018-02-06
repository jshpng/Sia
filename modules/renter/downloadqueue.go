package renter

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"

	"github.com/NebulousLabs/Sia/modules"
)

// Download performs a file download using the passed parameters.
func (r *Renter) Download(p modules.RenterDownloadParameters) (errChan chan error) {
	// channel used to return error
	errChan = make(chan error, 1)

	// lookup the file associated with the nickname.
	lockID := r.mu.RLock()
	file, exists := r.files[p.Siapath]
	r.mu.RUnlock(lockID)
	if !exists {
		errChan <- fmt.Errorf("no file with that path: %s", p.Siapath)
		return
	}

	isHTTPResp := p.Httpwriter != nil

	// validate download parameters
	if p.Async && isHTTPResp {
		errChan <- errors.New("cannot async download to http response")
		return
	}
	if isHTTPResp && p.Destination != "" {
		errChan <- errors.New("destination cannot be specified when downloading to http response")
		return
	}
	if !isHTTPResp && p.Destination == "" {
		errChan <- errors.New("destination not supplied")
		return
	}
	if p.Destination != "" && !filepath.IsAbs(p.Destination) {
		errChan <- errors.New("destination must be an absolute path")
		return
	}
	if p.Offset == file.size {
		errChan <- errors.New("offset equals filesize")
		return
	}
	// sentinel: if length == 0, download the entire file
	if p.Length == 0 {
		p.Length = file.size - p.Offset
	}
	// Check whether offset and length is valid.
	if p.Offset < 0 || p.Offset+p.Length > file.size {
		errChan <- fmt.Errorf("offset and length combination invalid, max byte is at index %d", file.size-1)
		return
	}

	// Instantiate the correct DownloadWriter implementation
	// (e.g. content written to file or response body).
	var dw modules.DownloadWriter
	if isHTTPResp {
		dw = NewDownloadHTTPWriter(p.Httpwriter, p.Offset, p.Length)
	} else {
		dfw, err := NewDownloadFileWriter(p.Destination, p.Offset, p.Length)
		if err != nil {
			errChan <- err
			return
		}
		dw = dfw
	}

	// Create the download object and add it to the queue.
	d := r.newSectionDownload(file, dw, p.Offset, p.Length)

	lockID = r.mu.Lock()
	r.downloadQueue = append(r.downloadQueue, d)
	r.mu.Unlock(lockID)
	r.newDownloads <- d

	// Send error of completed download to errChan
	go func() {
		select {
		case <-d.downloadFinished:
			errChan <- d.Err()
		case <-r.tg.StopChan():
			errChan <- errors.New("download interrupted by shutdown")
		}
		close(errChan)
	}()
	return
}

// DownloadQueue returns the list of downloads in the queue.
func (r *Renter) DownloadQueue() []modules.DownloadInfo {
	lockID := r.mu.RLock()
	defer r.mu.RUnlock(lockID)

	// Order from most recent to least recent.
	downloads := make([]modules.DownloadInfo, len(r.downloadQueue))
	for i := range r.downloadQueue {
		d := r.downloadQueue[len(r.downloadQueue)-i-1]

		downloads[i] = modules.DownloadInfo{
			SiaPath:     d.siapath,
			Destination: d.destination,
			Filesize:    d.length,
			StartTime:   d.startTime,
		}
		downloads[i].Received = atomic.LoadUint64(&d.atomicDataReceived)

		if err := d.Err(); err != nil {
			downloads[i].Error = err.Error()
		}
	}
	return downloads
}
