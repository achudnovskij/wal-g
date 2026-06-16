package postgres

import (
	"context"
	"fmt"
	"io"
	"path"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/asm"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// WalUploader extends uploader with wal specific functionality.
//
// This is the object-store sink the wal-receiver (and `wal-push`) write through. It embeds the
// generic internal.Uploader (which owns the storage.Folder + compressor + crypter — i.e. the
// actual "send to S3/GCS/Azure/FS" machinery, backend-agnostic via the storage.Folder interface)
// and layers on PG-specific extras:
//   - DeltaFileManager: optional WAL-delta recording for delta backups (off unless configured).
//   - ArchiveStatus/PGArchiveStatusManager: track which WAL files have been archived (used by the
//     archive_command path, not strictly by wal-receive).
type WalUploader struct {
	internal.Uploader
	ArchiveStatusManager   asm.ArchiveStatusManager
	PGArchiveStatusManager asm.ArchiveStatusManager
	*DeltaFileManager
}

func (walUploader *WalUploader) getUseWalDelta() (useWalDelta bool) {
	return walUploader.DeltaFileManager != nil
}

func NewWalUploader(
	baseUploader internal.Uploader,
	deltaFileManager *DeltaFileManager,
) *WalUploader {
	return &WalUploader{
		Uploader:         baseUploader,
		DeltaFileManager: deltaFileManager,
	}
}

// Clone creates similar WalUploader with new WaitGroup
func (walUploader *WalUploader) clone() *WalUploader {
	return &WalUploader{
		Uploader:               walUploader.Clone(),
		ArchiveStatusManager:   walUploader.ArchiveStatusManager,
		PGArchiveStatusManager: walUploader.PGArchiveStatusManager,
		DeltaFileManager:       walUploader.DeltaFileManager,
	}
}

// TODO : unit tests
// UploadWalFile is what HandleWALReceive calls once per WAL segment. `file` is the WalSegment
// itself (it's an io.Reader over the in-memory buffer) wrapped with its WAL filename.
//
// Step 1 (optional): if WAL-delta recording is enabled AND this is a real WAL filename, wrap the
// reader in a NewWalDeltaRecordingReader — as the bytes stream through to storage it also parses
// them to record which blocks changed, feeding delta backups. wal-receive normally has delta off,
// so this is usually a no-op pass-through.
// Step 2: hand the (possibly wrapped) reader to the embedded base uploader's UploadFile, which does
// the real work — compress, encrypt, and PUT to the object store (see internal/uploader.go).
func (walUploader *WalUploader) UploadWalFile(ctx context.Context, file ioextensions.NamedReader) error {
	var walFileReader io.Reader

	filename := path.Base(file.Name())
	if walUploader.getUseWalDelta() && isWalFilename(filename) {
		recordingReader, err := NewWalDeltaRecordingReader(file, filename, walUploader.DeltaFileManager)
		if err != nil {
			walFileReader = file
		} else {
			walFileReader = recordingReader
			defer utility.LoggedClose(recordingReader, "")
		}
	} else {
		walFileReader = file
	}

	// Delegate to the generic upload pipeline (compress → encrypt → storage PUT). The destination
	// name is the WAL filename; the compressor extension (e.g. `.lz4`) is appended inside UploadFile.
	return walUploader.UploadFile(ctx, ioextensions.NewNamedReaderImpl(walFileReader, file.Name()))
}

func (walUploader *WalUploader) FlushFiles(ctx context.Context) {
	walUploader.DeltaFileManager.FlushFiles(ctx, walUploader)
}

func PrepareMultiStorageWalUploader(folder storage.Folder, targetStorage string) (*WalUploader, error) {
	folder = multistorage.SetPolicies(folder, policies.TakeFirstStorage)
	var err error
	if targetStorage == "" {
		folder, err = multistorage.UseFirstAliveStorage(folder)
	} else {
		folder, err = multistorage.UseSpecificStorage(targetStorage, folder)
	}
	if err != nil {
		return nil, err
	}
	tracelog.InfoLogger.Printf("Files will be uploaded to storage: %v", multistorage.UsedStorages(folder)[0])

	baseUploader, err := internal.ConfigureUploaderToFolder(folder)
	if err != nil {
		return nil, fmt.Errorf("configure base uploader: %w", err)
	}

	walUploader, err := ConfigureWalUploader(baseUploader)
	if err != nil {
		return nil, fmt.Errorf("configure wal uploader: %w", err)
	}

	archiveStatusManager, err := internal.ConfigureArchiveStatusManager()
	if err == nil {
		walUploader.ArchiveStatusManager = asm.NewDataFolderASM(archiveStatusManager)
	} else {
		tracelog.ErrorLogger.PrintError(err)
		walUploader.ArchiveStatusManager = asm.NewNopASM()
	}

	PGArchiveStatusManager, err := internal.ConfigurePGArchiveStatusManager()
	if err == nil {
		walUploader.PGArchiveStatusManager = asm.NewDataFolderASM(PGArchiveStatusManager)
	} else {
		tracelog.ErrorLogger.PrintError(err)
		walUploader.PGArchiveStatusManager = asm.NewNopASM()
	}

	walUploader.ChangeDirectory(utility.WalPath)
	return walUploader, nil
}
