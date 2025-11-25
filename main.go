package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"gopkg.in/ini.v1"
)

const (
	version = "2.0.0"
)

// Config holds application configuration
type Config struct {
	BaseDir      string   `json:"baseDir"`
	AllowedDirs  []string `json:"allowedDirs"`
	ServerPort   string   `json:"serverPort"`
	MaxDownloads int      `json:"maxDownloads"`
}

// defaultConfig returns the default configuration settings
func defaultConfig() *Config {
	return &Config{
		BaseDir:      "./downloads",
		AllowedDirs:  []string{"general", "music", "videos", "documents", "software"},
		ServerPort:   "8080",
		MaxDownloads: 10,
	}
}

// loadConfig reads configuration from config.ini file
func loadConfig() *Config {
	configFile := "config.ini"
	if _, err := os.Stat(configFile); err == nil {
		cfg, err := ini.Load(configFile)
		if err != nil {
			log.Printf("Failed to load config file, using defaults: %v", err)
			return defaultConfig()
		}
		
		baseDir := cfg.Section("server").Key("base_dir").MustString("./downloads")
		serverPort := cfg.Section("server").Key("server_port").MustString("8080")
		allowedDirsStr := cfg.Section("server").Key("allowed_dirs").MustString("general,music,videos,documents,software")
		maxDownloads := cfg.Section("server").Key("max_downloads").MustInt(10)
		
		allowedDirs := strings.Split(allowedDirsStr, ",")
		for i := range allowedDirs {
			allowedDirs[i] = strings.TrimSpace(allowedDirs[i])
		}
		
		log.Printf("Configuration loaded from %s", configFile)
		return &Config{
			BaseDir:      baseDir,
			AllowedDirs:  allowedDirs,
			ServerPort:   serverPort,
			MaxDownloads: maxDownloads,
		}
	}
	
	log.Printf("Config file not found, using defaults")
	return defaultConfig()
}

// Download represents a download task
type Download struct {
	ID          string    `json:"id"`
	URL         string    `json:"url"`
	Destination string    `json:"destination"`
	Filename    string    `json:"filename"`
	Size        int64     `json:"size"`
	Downloaded  int64     `json:"downloaded"`
	Status      string    `json:"status"`
	Progress    float64   `json:"progress"`
	StartTime   time.Time `json:"startTime"`
	EndTime     time.Time `json:"endTime,omitempty"`
	Error       string    `json:"error,omitempty"`
	lastUpdate  time.Time
}

// DownloadManager manages all download operations
type DownloadManager struct {
	config         *Config
	downloads      map[string]*Download
	activeDownload map[string]bool
	clients        map[*websocket.Conn]bool
	mutex          sync.RWMutex
	broadcast      chan *Download
	downloadQueue  chan string
	workerWg       sync.WaitGroup
	updateTicker   *time.Ticker
	pendingUpdates map[string]bool
	updatesMutex   sync.Mutex
	historyFile    string
	doneHistory    chan struct{}
	ctx            context.Context
	cancel         context.CancelFunc
}

// NewDownloadManager creates a new download manager
func NewDownloadManager(config *Config) *DownloadManager {
	ctx, cancel := context.WithCancel(context.Background())
	
	dm := &DownloadManager{
		config:         config,
		downloads:      make(map[string]*Download),
		activeDownload: make(map[string]bool),
		clients:        make(map[*websocket.Conn]bool),
		broadcast:      make(chan *Download, 100),
		downloadQueue:  make(chan string, config.MaxDownloads*2),
		pendingUpdates: make(map[string]bool),
		updateTicker:   time.NewTicker(1 * time.Second),
		historyFile:    "download_history.json",
		doneHistory:    make(chan struct{}),
		ctx:            ctx,
		cancel:         cancel,
	}

	dm.loadHistory()

	// Start worker pool
	for i := 0; i < config.MaxDownloads; i++ {
		dm.workerWg.Add(1)
		go dm.downloadWorker()
	}

	go dm.handleBroadcasts()
	go dm.handleThrottledUpdates()
	go dm.startHistorySaver()

	return dm
}

// Shutdown gracefully shuts down the manager
func (dm *DownloadManager) Shutdown() {
	log.Println("Shutting down download manager...")
	dm.cancel()
	close(dm.downloadQueue)
	dm.workerWg.Wait()
	dm.updateTicker.Stop()
	close(dm.doneHistory)
	
	if err := dm.saveHistory(); err != nil {
		log.Printf("Error saving history on shutdown: %v", err)
	}
	
	close(dm.broadcast)
	log.Println("Shutdown complete")
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func main() {
	log.Printf("Starting Download Station v%s", version)
	
	config := loadConfig()

	// Create directories
	for _, dir := range config.AllowedDirs {
		fullPath := filepath.Join(config.BaseDir, dir)
		if err := os.MkdirAll(fullPath, 0755); err != nil {
			log.Fatalf("Failed to create directory %s: %v", fullPath, err)
		}
	}

	manager := NewDownloadManager(config)

	router := mux.NewRouter()

	// API routes
	router.HandleFunc("/api/downloads", manager.createDownload).Methods("POST")
	router.HandleFunc("/api/downloads", manager.getDownloads).Methods("GET")
	router.HandleFunc("/api/downloads/{id}", manager.getDownload).Methods("GET")
	router.HandleFunc("/api/downloads/{id}/cancel", manager.cancelDownload).Methods("POST")
	router.HandleFunc("/api/downloads/{id}/pause", manager.pauseDownload).Methods("POST")
	router.HandleFunc("/api/downloads/{id}/resume", manager.resumeDownload).Methods("POST")
	router.HandleFunc("/api/downloads/{id}/remove", manager.removeDownload).Methods("POST")
	router.HandleFunc("/api/downloads/{id}/deletefile", manager.deleteFile).Methods("POST")
	router.HandleFunc("/api/config", manager.getConfig).Methods("GET")
	router.HandleFunc("/download", manager.serveFile).Methods("GET")
	router.HandleFunc("/ws", manager.handleWebSocket)

	// Serve static files
	router.PathPrefix("/").Handler(http.FileServer(http.Dir("./static")))

	server := &http.Server{
		Addr:         ":" + config.ServerPort,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt, syscall.SIGTERM)
		<-sigint
		log.Println("Shutdown signal received")
		
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("Server shutdown error: %v", err)
		}
		
		manager.Shutdown()
	}()

	log.Printf("Server starting on port %s", config.ServerPort)
	log.Printf("Base directory: %s", config.BaseDir)
	log.Printf("Allowed directories: %v", config.AllowedDirs)
	
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}

// loadHistory loads download history from file
func (dm *DownloadManager) loadHistory() {
	dm.mutex.Lock()
	defer dm.mutex.Unlock()
	
	file, err := os.Open(dm.historyFile)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("Error opening history file: %v", err)
		}
		return
	}
	defer file.Close()

	var downloads []*Download
	if err := json.NewDecoder(file).Decode(&downloads); err != nil {
		log.Printf("Error decoding history: %v", err)
		return
	}
	
	for _, d := range downloads {
		dm.downloads[d.ID] = d
	}
	log.Printf("Loaded %d downloads from history", len(downloads))
}

// saveHistory saves download history to file
func (dm *DownloadManager) saveHistory() error {
	dm.mutex.RLock()
	downloads := make([]*Download, 0, len(dm.downloads))
	for _, d := range dm.downloads {
		downloads = append(downloads, d)
	}
	dm.mutex.RUnlock()

	data, err := json.MarshalIndent(downloads, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}
	
	return os.WriteFile(dm.historyFile, data, 0644)
}

// startHistorySaver periodically saves history
func (dm *DownloadManager) startHistorySaver() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			if err := dm.saveHistory(); err != nil {
				log.Printf("Error saving history: %v", err)
			}
		case <-dm.doneHistory:
			return
		}
	}
}

// downloadWorker processes downloads from queue
func (dm *DownloadManager) downloadWorker() {
	defer dm.workerWg.Done()
	for id := range dm.downloadQueue {
		dm.processDownload(id)
	}
}

// handleThrottledUpdates sends batched updates
func (dm *DownloadManager) handleThrottledUpdates() {
	for range dm.updateTicker.C {
		dm.sendPendingUpdates()
	}
}

// sendPendingUpdates broadcasts all pending updates
func (dm *DownloadManager) sendPendingUpdates() {
	dm.updatesMutex.Lock()
	pendingIDs := make([]string, 0, len(dm.pendingUpdates))
	for id := range dm.pendingUpdates {
		pendingIDs = append(pendingIDs, id)
	}
	dm.pendingUpdates = make(map[string]bool)
	dm.updatesMutex.Unlock()

	for _, id := range pendingIDs {
		dm.mutex.RLock()
		download, exists := dm.downloads[id]
		dm.mutex.RUnlock()
		
		if exists {
			dm.broadcast <- download
		}
	}
}

// queueProgressUpdate queues a download for update
func (dm *DownloadManager) queueProgressUpdate(id string) {
	dm.updatesMutex.Lock()
	dm.pendingUpdates[id] = true
	dm.updatesMutex.Unlock()
}

// getConfig returns server configuration
func (dm *DownloadManager) getConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(dm.config)
}

// handleWebSocket handles WebSocket connections
func (dm *DownloadManager) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}

	dm.mutex.Lock()
	dm.clients[conn] = true
	
	// Send current state to new client
	for _, download := range dm.downloads {
		conn.WriteJSON(download)
	}
	dm.mutex.Unlock()

	conn.SetCloseHandler(func(code int, text string) error {
		dm.mutex.Lock()
		delete(dm.clients, conn)
		dm.mutex.Unlock()
		return nil
	})

	// Keep connection alive
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			dm.mutex.Lock()
			delete(dm.clients, conn)
			dm.mutex.Unlock()
			break
		}
	}
}

// handleBroadcasts sends updates to all connected clients
func (dm *DownloadManager) handleBroadcasts() {
	for download := range dm.broadcast {
		dm.mutex.RLock()
		for client := range dm.clients {
			if err := client.WriteJSON(download); err != nil {
				log.Printf("Broadcast error: %v", err)
				client.Close()
				dm.mutex.RUnlock()
				dm.mutex.Lock()
				delete(dm.clients, client)
				dm.mutex.Unlock()
				dm.mutex.RLock()
			}
		}
		dm.mutex.RUnlock()
	}
}

// createDownload handles new download requests
func (dm *DownloadManager) createDownload(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL         string `json:"url"`
		Destination string `json:"destination"`
		Filename    string `json:"filename,omitempty"`
	}
	
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if req.URL == "" {
		http.Error(w, "URL is required", http.StatusBadRequest)
		return
	}

	// Validate destination
	valid := false
	for _, dir := range dm.config.AllowedDirs {
		if req.Destination == dir {
			valid = true
			break
		}
	}
	if !valid {
		http.Error(w, "Invalid destination directory", http.StatusBadRequest)
		return
	}

	// Ensure destination exists
	fullDestPath := filepath.Join(dm.config.BaseDir, req.Destination)
	if err := os.MkdirAll(fullDestPath, 0755); err != nil {
		http.Error(w, "Failed to create destination", http.StatusInternalServerError)
		return
	}

	id := fmt.Sprintf("%d", time.Now().UnixNano())
	filename := req.Filename
	if filename == "" {
		filename = filepath.Base(req.URL)
	}

	download := &Download{
		ID:          id,
		URL:         req.URL,
		Destination: req.Destination,
		Filename:    filename,
		Status:      "queued",
		StartTime:   time.Now(),
		lastUpdate:  time.Now(),
	}

	dm.mutex.Lock()
	dm.downloads[id] = download
	dm.mutex.Unlock()

	dm.downloadQueue <- id

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(download)
}

// getDownloads returns all downloads
func (dm *DownloadManager) getDownloads(w http.ResponseWriter, r *http.Request) {
	dm.mutex.RLock()
	downloads := make([]*Download, 0, len(dm.downloads))
	for _, d := range dm.downloads {
		downloads = append(downloads, d)
	}
	dm.mutex.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(downloads)
}

// getDownload returns a specific download
func (dm *DownloadManager) getDownload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	dm.mutex.RLock()
	download, exists := dm.downloads[id]
	dm.mutex.RUnlock()

	if !exists {
		http.Error(w, "Download not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(download)
}

// cancelDownload cancels a download
func (dm *DownloadManager) cancelDownload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	dm.mutex.Lock()
	download, exists := dm.downloads[id]
	if exists {
		if download.Status == "downloading" {
			dm.activeDownload[id] = false
		}
		download.Status = "cancelled"
		download.EndTime = time.Now()
	}
	dm.mutex.Unlock()

	if !exists {
		http.Error(w, "Download not found", http.StatusNotFound)
		return
	}

	dm.broadcast <- download
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(download)
}

// pauseDownload pauses a download
func (dm *DownloadManager) pauseDownload(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Pause feature not implemented", http.StatusNotImplemented)
}

// resumeDownload resumes a download
func (dm *DownloadManager) resumeDownload(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Resume feature not implemented", http.StatusNotImplemented)
}

// removeDownload removes download from history
func (dm *DownloadManager) removeDownload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	dm.mutex.Lock()
	_, exists := dm.downloads[id]
	if exists {
		delete(dm.downloads, id)
		delete(dm.activeDownload, id)
	}
	dm.mutex.Unlock()

	if !exists {
		http.Error(w, "Download not found", http.StatusNotFound)
		return
	}

	if err := dm.saveHistory(); err != nil {
		log.Printf("Error saving history after removal: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "Download removed from history",
		"id":      id,
	})
}

// deleteFile deletes the downloaded file from disk
func (dm *DownloadManager) deleteFile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	dm.mutex.RLock()
	download, exists := dm.downloads[id]
	dm.mutex.RUnlock()

	if !exists {
		http.Error(w, "Download not found", http.StatusNotFound)
		return
	}

	filePath := filepath.Join(dm.config.BaseDir, download.Destination, download.Filename)
	
	// Security check: ensure path is within base directory
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		http.Error(w, "Invalid file path", http.StatusBadRequest)
		return
	}
	
	absBase, err := filepath.Abs(dm.config.BaseDir)
	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	
	if !strings.HasPrefix(absPath, absBase) {
		http.Error(w, "Invalid file path", http.StatusBadRequest)
		return
	}

	if err := os.Remove(filePath); err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "File not found", http.StatusNotFound)
		} else {
			log.Printf("Error deleting file: %v", err)
			http.Error(w, "Failed to delete file", http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "File deleted successfully",
		"id":      id,
	})
}

// processDownload performs the actual download
func (dm *DownloadManager) processDownload(id string) {
	dm.mutex.Lock()
	download, exists := dm.downloads[id]
	if !exists {
		dm.mutex.Unlock()
		return
	}
	download.Status = "downloading"
	dm.activeDownload[id] = true
	dm.mutex.Unlock()

	dm.broadcast <- download

	client := &http.Client{
		Timeout: 0, // No timeout for downloads
	}
	
	req, err := http.NewRequest("GET", download.URL, nil)
	if err != nil {
		dm.updateDownloadStatus(id, "failed", err.Error())
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		dm.updateDownloadStatus(id, "failed", err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		dm.updateDownloadStatus(id, "failed", fmt.Sprintf("HTTP %d", resp.StatusCode))
		return
	}

	dm.mutex.Lock()
	download.Size = resp.ContentLength
	dm.mutex.Unlock()

	outputPath := filepath.Join(dm.config.BaseDir, download.Destination, download.Filename)
	outputFile, err := os.Create(outputPath)
	if err != nil {
		dm.updateDownloadStatus(id, "failed", err.Error())
		return
	}
	defer outputFile.Close()

	progressReader := &ProgressReader{
		Reader: resp.Body,
		Total:  resp.ContentLength,
		OnProgress: func(downloaded int64) {
			dm.mutex.Lock()
			if dl, exists := dm.downloads[id]; exists {
				dl.Downloaded = downloaded
				if dl.Size > 0 {
					dl.Progress = float64(downloaded) / float64(dl.Size) * 100
				}
			}
			dm.mutex.Unlock()
			dm.queueProgressUpdate(id)
		},
	}

	buf := make([]byte, 32*1024)
	for {
		dm.mutex.RLock()
		active := dm.activeDownload[id]
		dm.mutex.RUnlock()
		
		if !active {
			return
		}

		n, err := progressReader.Read(buf)
		if n > 0 {
			if _, writeErr := outputFile.Write(buf[:n]); writeErr != nil {
				dm.updateDownloadStatus(id, "failed", writeErr.Error())
				return
			}
		}
		
		if err != nil {
			if err == io.EOF {
				break
			}
			dm.updateDownloadStatus(id, "failed", err.Error())
			return
		}
	}

	dm.mutex.RLock()
	active := dm.activeDownload[id]
	dm.mutex.RUnlock()
	
	if !active {
		return
	}

	dm.updateDownloadStatus(id, "completed", "")
}

// updateDownloadStatus updates download status
func (dm *DownloadManager) updateDownloadStatus(id, status, errorMsg string) {
	dm.mutex.Lock()
	download, exists := dm.downloads[id]
	if exists {
		download.Status = status
		download.Error = errorMsg
		if status == "completed" || status == "failed" {
			download.EndTime = time.Now()
			delete(dm.activeDownload, id)
		}
	}
	dm.mutex.Unlock()

	if exists {
		dm.broadcast <- download
	}
}

// ProgressReader tracks download progress
type ProgressReader struct {
	Reader     io.Reader
	Total      int64
	Downloaded int64
	OnProgress func(int64)
}

// Read implements io.Reader
func (pr *ProgressReader) Read(p []byte) (int, error) {
	n, err := pr.Reader.Read(p)
	pr.Downloaded += int64(n)
	if pr.OnProgress != nil {
		pr.OnProgress(pr.Downloaded)
	}
	return n, err
}

// serveFile serves downloaded files
func (dm *DownloadManager) serveFile(w http.ResponseWriter, r *http.Request) {
	filePath := r.URL.Query().Get("file")
	
	// Security: validate path
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	
	absBase, err := filepath.Abs(dm.config.BaseDir)
	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	
	if !strings.HasPrefix(absPath, absBase) {
		http.Error(w, "Access denied", http.StatusForbidden)
		return
	}

	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	filename := filepath.Base(absPath)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	http.ServeFile(w, r, absPath)
}
