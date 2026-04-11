package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type RawSnapshot struct {
	Timestamp         time.Time `json:"timestamp"`
	Host              string    `json:"host"`
	Load1             float64   `json:"load1"`
	Load5             float64   `json:"load5"`
	Load15            float64   `json:"load15"`
	MemTotalBytes     uint64    `json:"mem_total_bytes"`
	MemAvailableBytes uint64    `json:"mem_available_bytes"`
	SwapTotalBytes    uint64    `json:"swap_total_bytes"`
	SwapFreeBytes     uint64    `json:"swap_free_bytes"`
	PSwpInTotal       uint64    `json:"pswpin_total"`
	PSwpOutTotal      uint64    `json:"pswpout_total"`
	CPUIDleTotal      uint64    `json:"cpu_idle_total"`
	CPUTotal          uint64    `json:"cpu_total"`
	ReadIOsTotal      uint64    `json:"read_ios_total"`
	WriteIOsTotal     uint64    `json:"write_ios_total"`
	RXBytesTotal      uint64    `json:"rx_bytes_total"`
	TXBytesTotal      uint64    `json:"tx_bytes_total"`
}

func (r RawSnapshot) Validate() error {
	if r.Timestamp.IsZero() {
		return errors.New("zero timestamp")
	}
	if r.Host == "" {
		return errors.New("empty host")
	}
	if r.MemTotalBytes == 0 && r.SwapTotalBytes == 0 {
		return errors.New("both mem and swap total are zero")
	}
	return nil
}

type DerivedSnapshot struct {
	Timestamp       time.Time `json:"timestamp"`
	TimestampUnix   int64     `json:"timestamp_unix"`
	Host            string    `json:"host"`
	ElapsedSeconds  float64   `json:"elapsed_seconds"`
	Load1           float64   `json:"load1"`
	Load5           float64   `json:"load5"`
	Load15          float64   `json:"load15"`
	MemTotalBytes   uint64    `json:"mem_total_bytes"`
	MemAvailable    uint64    `json:"mem_available_bytes"`
	MemUsedBytes    uint64    `json:"mem_used_bytes"`
	MemUsedPercent  float64   `json:"mem_used_percent"`
	SwapTotalBytes  uint64    `json:"swap_total_bytes"`
	SwapFreeBytes   uint64    `json:"swap_free_bytes"`
	SwapUsedBytes   uint64    `json:"swap_used_bytes"`
	SwapUsedPercent float64   `json:"swap_used_percent"`
	PSwpInTotal     uint64    `json:"pswpin_total"`
	PSwpOutTotal    uint64    `json:"pswpout_total"`
	SwapInRate      float64   `json:"swapin_rate_per_s"`
	SwapOutRate     float64   `json:"swapout_rate_per_s"`
	CPUUsagePercent *float64  `json:"cpu_usage_percent"`
	ReadIOsTotal    uint64    `json:"read_ios_total"`
	WriteIOsTotal   uint64    `json:"write_ios_total"`
	ReadIOPS        float64   `json:"read_iops"`
	WriteIOPS       float64   `json:"write_iops"`
	RXBytesTotal    uint64    `json:"rx_bytes_total"`
	TXBytesTotal    uint64    `json:"tx_bytes_total"`
	RXRateBPS       float64   `json:"rx_rate_Bps"`
	TXRateBPS       float64   `json:"tx_rate_Bps"`
}

func (d DerivedSnapshot) CPUUsageText() string {
	if d.CPUUsagePercent == nil {
		return "N/A"
	}
	return fmt.Sprintf("%.2f%%", *d.CPUUsagePercent)
}

type Config struct {
	Interval float64
	Count    int
	Framed   bool
	JSON     bool
	CSVPath  string
	WarmupMS int
}

type CSVWriter struct {
	mu sync.Mutex
	path string
	file *os.File
	writer *csv.Writer
}

func NewCSVWriter(path string) (*CSVWriter, error) {
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	needsHeader := false
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			needsHeader = true
		} else {
			return nil, err
		}
	} else if info.Size() == 0 {
		needsHeader = true
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}

	w := csv.NewWriter(f)

	if needsHeader {
		err = w.Write([]string{
			"timestamp_unix",
			"timestamp_iso",
			"host",
			"elapsed_seconds",
			"cpu_usage_percent",
			"load1",
			"load5",
			"load15",
			"mem_total_bytes",
			"mem_available_bytes",
			"mem_used_bytes",
			"mem_used_percent",
			"swap_total_bytes",
			"swap_free_bytes",
			"swap_used_bytes",
			"swap_used_percent",
			"pswpin_total",
			"pswpout_total",
			"swapin_rate_per_s",
			"swapout_rate_per_s",
			"read_ios_total",
			"write_ios_total",
			"read_iops",
			"write_iops",
			"rx_bytes_total",
			"tx_bytes_total",
			"rx_rate_Bps",
			"tx_rate_Bps",
		})
		if err != nil {
			f.Close()
			return nil, err
		}
		w.Flush()
		if err := w.Error(); err != nil {
			f.Close()
			return nil, err
		}
	}

	return &CSVWriter{
		path: path,
		file: f,
		writer: w,
	}, nil
}

func (cw *CSVWriter) Append(d DerivedSnapshot) error {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	cpu := ""
	if d.CPUUsagePercent != nil {
		cpu = fmt.Sprintf("%.6f", *d.CPUUsagePercent)
	}

	err := cw.writer.Write([]string{
		strconv.FormatInt(d.TimestampUnix, 10),
		d.Timestamp.Format(time.RFC3339),
		d.Host,
		fmt.Sprintf("%.6f", d.ElapsedSeconds),
		cpu,
		fmt.Sprintf("%.6f", d.Load1),
		fmt.Sprintf("%.6f", d.Load5),
		fmt.Sprintf("%.6f", d.Load15),
		strconv.FormatUint(d.MemTotalBytes, 10),
		strconv.FormatUint(d.MemAvailable, 10),
		strconv.FormatUint(d.MemUsedBytes, 10),
		fmt.Sprintf("%.6f", d.MemUsedPercent),
		strconv.FormatUint(d.SwapTotalBytes, 10),
		strconv.FormatUint(d.SwapFreeBytes, 10),
		strconv.FormatUint(d.SwapUsedBytes, 10),
		fmt.Sprintf("%.6f", d.SwapUsedPercent),
		strconv.FormatUint(d.PSwpInTotal, 10),
		strconv.FormatUint(d.PSwpOutTotal, 10),
		fmt.Sprintf("%.6f", d.SwapInRate),
		fmt.Sprintf("%.6f", d.SwapOutRate),
		strconv.FormatUint(d.ReadIOsTotal, 10),
		strconv.FormatUint(d.WriteIOsTotal, 10),
		fmt.Sprintf("%.6f", d.ReadIOPS),
		fmt.Sprintf("%.6f", d.WriteIOPS),
		strconv.FormatUint(d.RXBytesTotal, 10),
		strconv.FormatUint(d.TXBytesTotal, 10),
		fmt.Sprintf("%.6f", d.RXRateBPS),
		fmt.Sprintf("%.6f", d.TXRateBPS),
	})
	if err != nil {
		return err
	}
	cw.writer.Flush()
	return cw.writer.Error()
}

func (cw *CSVWriter) Close() error {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	if cw.writer != nil {
		cw.writer.Flush()
	}
	if cw.file != nil {
		return cw.file.Close()
	}
	return nil
}

type Collector struct {
	Hostname string
}

func NewCollector() *Collector {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return &Collector{Hostname: host}
}

func (c *Collector) Collect() (RawSnapshot, error) {
	load1, load5, load15, err := c.readLoad()
	if err != nil {
		return RawSnapshot{}, err
	}

	memTotal, memAvail, swapTotal, swapFree, err := c.readMemInfo()
	if err != nil {
		return RawSnapshot{}, err
	}

	pswpIn, pswpOut, err := c.readVMStat()
	if err != nil {
		return RawSnapshot{}, err
	}

	cpuIdle, cpuTotal, err := c.readCPU()
	if err != nil {
		return RawSnapshot{}, err
	}

	readIOs, writeIOs, err := c.readDiskStats()
	if err != nil {
		return RawSnapshot{}, err
	}

	rxBytes, txBytes, err := c.readNetDev()
	if err != nil {
		return RawSnapshot{}, err
	}

	snapshot := RawSnapshot{
		Timestamp:         time.Now(),
		Host:              c.Hostname,
		Load1:             load1,
		Load5:             load5,
		Load15:            load15,
		MemTotalBytes:     memTotal,
		MemAvailableBytes: memAvail,
		SwapTotalBytes:    swapTotal,
		SwapFreeBytes:     swapFree,
		PSwpInTotal:       pswpIn,
		PSwpOutTotal:      pswpOut,
		CPUIDleTotal:      cpuIdle,
		CPUTotal:          cpuTotal,
		ReadIOsTotal:      readIOs,
		WriteIOsTotal:     writeIOs,
		RXBytesTotal:      rxBytes,
		TXBytesTotal:      txBytes,
	}

	if err := snapshot.Validate(); err != nil {
		return RawSnapshot{}, fmt.Errorf("invalid snapshot: %w", err)
	}

	return snapshot, nil
}

func (c *Collector) readLoad() (float64, float64, float64, error) {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0, 0, 0, err
	}
	fields := strings.Fields(string(data))
	if len(fields) < 3 {
		return 0, 0, 0, errors.New("invalid loadavg")
	}
	l1, err1 := strconv.ParseFloat(fields[0], 64)
	l5, err2 := strconv.ParseFloat(fields[1], 64)
	l15, err3 := strconv.ParseFloat(fields[2], 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return 0, 0, 0, errors.New("invalid load values")
	}
	return l1, l5, l15, nil
}

func (c *Collector) readMemInfo() (uint64, uint64, uint64, uint64, error) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, 0, 0, err
	}

	values := map[string]uint64{}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.Contains(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		key := strings.TrimSpace(parts[0])
		fields := strings.Fields(parts[1])
		if len(fields) == 0 {
			continue
		}
		v, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			continue
		}
		values[key] = v * 1024
	}

	return values["MemTotal"], values["MemAvailable"], values["SwapTotal"], values["SwapFree"], nil
}

func (c *Collector) readVMStat() (uint64, uint64, error) {
	data, err := os.ReadFile("/proc/vmstat")
	if err != nil {
		return 0, 0, err
	}

	var in, out uint64
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		if fields[0] == "pswpin" {
			in = v
		} else if fields[0] == "pswpout" {
			out = v
		}
	}

	return in, out, nil
}

const (
	cpuFieldUser = iota
	cpuFieldNice
	cpuFieldSystem
	cpuFieldIdle
	cpuFieldIowait
	cpuFieldIrq
	cpuFieldSoftirq
	cpuFieldSteal
	cpuFieldGuest
	cpuFieldGuestNice
)

func (c *Collector) readCPU() (uint64, uint64, error) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0, err
	}

	lines := strings.Split(string(data), "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) == "" {
		return 0, 0, errors.New("invalid cpu")
	}

	fields := strings.Fields(lines[0])
	if len(fields) < 5 || fields[0] != "cpu" {
		return 0, 0, errors.New("invalid cpu")
	}

	var total uint64
	var idle uint64

	for i, s := range fields[1:] {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return 0, 0, errors.New("invalid cpu values")
		}
		total += v
		if i == cpuFieldIdle {
			idle += v
		}
		if i == cpuFieldIowait {
			idle += v
		}
	}

	if total == 0 {
		return 0, 0, errors.New("cpu total is zero")
	}

	return idle, total, nil
}

func (c *Collector) readDiskStats() (uint64, uint64, error) {
	data, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		return 0, 0, err
	}

	var r, w uint64
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 14 {
			continue
		}
		dev := fields[2]
		if strings.HasPrefix(dev, "loop") || strings.HasPrefix(dev, "ram") {
			continue
		}
		ri, err1 := strconv.ParseUint(fields[3], 10, 64)
		wi, err2 := strconv.ParseUint(fields[7], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		r += ri
		w += wi
	}

	return r, w, nil
}

func (c *Collector) readNetDev() (uint64, uint64, error) {
	data, err := os.ReadFile("/proc/net/dev")
	if err != nil {
		return 0, 0, err
	}

	lines := strings.Split(string(data), "\n")
	if len(lines) < 3 {
		return 0, 0, errors.New("invalid /proc/net/dev")
	}

	var rx, tx uint64
	for _, line := range lines[2:] {
		if !strings.Contains(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		iface := strings.TrimSpace(parts[0])
		if iface == "lo" {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) < 9 {
			continue
		}
		rxv, err1 := strconv.ParseUint(fields[0], 10, 64)
		txv, err2 := strconv.ParseUint(fields[8], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		rx += rxv
		tx += txv
	}

	return rx, tx, nil
}

func (c *Collector) Derive(prev *RawSnapshot, curr RawSnapshot) DerivedSnapshot {
	memUsed := clampSub(curr.MemTotalBytes, curr.MemAvailableBytes)
	swapUsed := clampSub(curr.SwapTotalBytes, curr.SwapFreeBytes)

	out := DerivedSnapshot{
		Timestamp:       curr.Timestamp,
		TimestampUnix:   curr.Timestamp.Unix(),
		Host:            curr.Host,
		Load1:           curr.Load1,
		Load5:           curr.Load5,
		Load15:          curr.Load15,
		MemTotalBytes:   curr.MemTotalBytes,
		MemAvailable:    curr.MemAvailableBytes,
		MemUsedBytes:    memUsed,
		MemUsedPercent:  percent(memUsed, curr.MemTotalBytes),
		SwapTotalBytes:  curr.SwapTotalBytes,
		SwapFreeBytes:   curr.SwapFreeBytes,
		SwapUsedBytes:   swapUsed,
		SwapUsedPercent: percent(swapUsed, curr.SwapTotalBytes),
		PSwpInTotal:     curr.PSwpInTotal,
		PSwpOutTotal:    curr.PSwpOutTotal,
		ReadIOsTotal:    curr.ReadIOsTotal,
		WriteIOsTotal:   curr.WriteIOsTotal,
		RXBytesTotal:    curr.RXBytesTotal,
		TXBytesTotal:    curr.TXBytesTotal,
	}

	if prev == nil {
		return out
	}

	if prev.Host != curr.Host {
		return out
	}

	elapsed := curr.Timestamp.Sub(prev.Timestamp).Seconds()
	if elapsed <= 0 {
		return out
	}

	out.ElapsedSeconds = elapsed
	out.SwapInRate = delta(prev.PSwpInTotal, curr.PSwpInTotal, elapsed)
	out.SwapOutRate = delta(prev.PSwpOutTotal, curr.PSwpOutTotal, elapsed)
	out.ReadIOPS = delta(prev.ReadIOsTotal, curr.ReadIOsTotal, elapsed)
	out.WriteIOPS = delta(prev.WriteIOsTotal, curr.WriteIOsTotal, elapsed)
	out.RXRateBPS = delta(prev.RXBytesTotal, curr.RXBytesTotal, elapsed)
	out.TXRateBPS = delta(prev.TXBytesTotal, curr.TXBytesTotal, elapsed)

	if curr.CPUTotal >= prev.CPUTotal && curr.CPUIDleTotal >= prev.CPUIDleTotal {
		totalDelta := curr.CPUTotal - prev.CPUTotal
		idleDelta := curr.CPUIDleTotal - prev.CPUIDleTotal
		if totalDelta > 0 {
			v := 100.0 * (1.0 - float64(idleDelta)/float64(totalDelta))
			if v < 0 {
				v = 0
			}
			if v > 100 {
				v = 100
			}
			out.CPUUsagePercent = &v
		}
	}

	return out
}

func clampSub(a, b uint64) uint64 {
	if a < b {
		return 0
	}
	return a - b
}

func delta(prev, curr uint64, elapsed float64) float64 {
	if curr < prev || elapsed <= 0 {
		return 0
	}
	return float64(curr-prev) / elapsed
}

func percent(v, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return float64(v) / float64(total) * 100
}

func sleepInterruptible(d time.Duration, stop <-chan struct{}) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-stop:
		return false
	}
}

func usage() string {
	return strings.TrimSpace(`
usage: perfdelta [options]

options:
  -i, --interval <seconds>   sample interval, non-negative float
  -c, --count <n>            number of emitted samples in interval mode
  --warmup-ms <ms>           warmup delay for single-shot mode
  --framed                   framed terminal output
  --json                     JSON output
  --csv <path>               append CSV output
`)
}

func parseArgs(args []string) (Config, error) {
	cfg := Config{
		Interval: 0,
		Count:    0,
		Framed:   false,
		JSON:     false,
		CSVPath:  "",
		WarmupMS: 250,
	}

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "--help":
			return cfg, errors.New(usage())
		case "-i", "--interval":
			if i+1 >= len(args) {
				return cfg, errors.New("missing value for interval")
			}
			v, err := strconv.ParseFloat(args[i+1], 64)
			if err != nil || v < 0 {
				return cfg, errors.New("interval must be a non-negative number")
			}
			cfg.Interval = v
			i++
		case "-c", "--count":
			if i+1 >= len(args) {
				return cfg, errors.New("missing value for count")
			}
			v, err := strconv.Atoi(args[i+1])
			if err != nil || v < 0 {
				return cfg, errors.New("count must be a non-negative integer")
			}
			cfg.Count = v
			i++
		case "--warmup-ms":
			if i+1 >= len(args) {
				return cfg, errors.New("missing value for warmup-ms")
			}
			v, err := strconv.Atoi(args[i+1])
			if err != nil || v < 0 {
				return cfg, errors.New("warmup-ms must be a non-negative integer")
			}
			cfg.WarmupMS = v
			i++
		case "--framed":
			cfg.Framed = true
		case "--json":
			cfg.JSON = true
		case "--csv":
			if i+1 >= len(args) {
				return cfg, errors.New("missing value for csv")
			}
			cfg.CSVPath = args[i+1]
			i++
		default:
			return cfg, fmt.Errorf("unknown argument: %s", args[i])
		}
	}

	return cfg, nil
}

func output(d DerivedSnapshot, framed bool, jsonMode bool) error {
	if jsonMode {
		b, err := json.MarshalIndent(d, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(b))
		return nil
	}
	if framed {
		printFramed(d)
		return nil
	}
	fmt.Println(formatLine(d))
	return nil
}

func formatLine(d DerivedSnapshot) string {
	cpu := d.CPUUsageText()
	return fmt.Sprintf(
		"%d %s elapsed=%.3fs cpu=%s load=%.2f/%.2f/%.2f mem=%.1f%% swap=%.1f%% swap_delta=%.2f/%.2f disk_delta=%.2f/%.2f net_delta=%.2f/%.2f",
		d.TimestampUnix,
		d.Host,
		d.ElapsedSeconds,
		cpu,
		d.Load1,
		d.Load5,
		d.Load15,
		d.MemUsedPercent,
		d.SwapUsedPercent,
		d.SwapInRate,
		d.SwapOutRate,
		d.ReadIOPS,
		d.WriteIOPS,
		d.RXRateBPS,
		d.TXRateBPS,
	)
}

func printFramed(d DerivedSnapshot) {
	lines := []string{
		fmt.Sprintf("PERF-CHECK DELTA  host=%s  ts=%d  elapsed=%.3fs", d.Host, d.TimestampUnix, d.ElapsedSeconds),
		fmt.Sprintf("CPU  usage=%s    LOAD  %.2f/%.2f/%.2f", d.CPUUsageText(), d.Load1, d.Load5, d.Load15),
		fmt.Sprintf("MEM  used=%s (%.1f%%)    available=%s    total=%s", fmtBytes(d.MemUsedBytes), d.MemUsedPercent, fmtBytes(d.MemAvailable), fmtBytes(d.MemTotalBytes)),
		fmt.Sprintf("SWAP used=%s (%.1f%%)    free=%s    total=%s", fmtBytes(d.SwapUsedBytes), d.SwapUsedPercent, fmtBytes(d.SwapFreeBytes), fmtBytes(d.SwapTotalBytes)),
		fmt.Sprintf("SWAP Δ in=%s    out=%s    totals=%d/%d", fmtOps(d.SwapInRate), fmtOps(d.SwapOutRate), d.PSwpInTotal, d.PSwpOutTotal),
		fmt.Sprintf("DISK Δ read=%s    write=%s    totals=%d/%d", fmtOps(d.ReadIOPS), fmtOps(d.WriteIOPS), d.ReadIOsTotal, d.WriteIOsTotal),
		fmt.Sprintf("NET  Δ rx=%s    tx=%s    totals=%d/%d", fmtRate(d.RXRateBPS), fmtRate(d.TXRateBPS), d.RXBytesTotal, d.TXBytesTotal),
	}

	width := 60
	for _, line := range lines {
		if len(line)+4 > width {
			width = len(line) + 4
		}
	}

	fmt.Println("┌" + strings.Repeat("─", width-2) + "┐")
	for i, line := range lines {
		fmt.Printf("│ %-*s │\n", width-4, line)
		if i == 0 {
			fmt.Println("├" + strings.Repeat("─", width-2) + "┤")
		}
	}
	fmt.Println("└" + strings.Repeat("─", width-2) + "┘")
}

func fmtBytes(value uint64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	v := float64(value)
	idx := 0
	for v >= 1024 && idx < len(units)-1 {
		v /= 1024
		idx++
	}
	return fmt.Sprintf("%.1f %s", v, units[idx])
}

func fmtRate(value float64) string {
	units := []string{"B/s", "KiB/s", "MiB/s", "GiB/s", "TiB/s", "PiB/s"}
	v := value
	idx := 0
	for v >= 1024 && idx < len(units)-1 {
		v /= 1024
		idx++
	}
	return fmt.Sprintf("%.2f %s", v, units[idx])
}

func fmtOps(value float64) string {
	if value >= 1_000_000_000 {
		return fmt.Sprintf("%.2f G/s", value/1_000_000_000)
	}
	if value >= 1_000_000 {
		return fmt.Sprintf("%.2f M/s", value/1_000_000)
	}
	if value >= 1_000 {
		return fmt.Sprintf("%.2f K/s", value/1_000)
	}
	return fmt.Sprintf("%.2f /s", value)
}

func main() {
	cfg, err := parseArgs(os.Args[1:])
	if err != nil {
		msg := err.Error()
		if strings.HasPrefix(msg, "usage:") {
			fmt.Println(msg)
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, msg)
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, usage())
		os.Exit(1)
	}

	var csvWriter *CSVWriter
	if cfg.CSVPath != "" {
		csvWriter, err = NewCSVWriter(cfg.CSVPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer csvWriter.Close()
	}

	collector := NewCollector()

	stop := make(chan struct{})
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sig
		close(stop)
	}()

	if cfg.Interval <= 0 {
		first, err := collector.Collect()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		if !sleepInterruptible(time.Duration(cfg.WarmupMS)*time.Millisecond, stop) {
			return
		}

		second, err := collector.Collect()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		derived := collector.Derive(&first, second)
		if err := output(derived, cfg.Framed, cfg.JSON); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if csvWriter != nil {
			if err := csvWriter.Append(derived); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
		return
	}

	var prev *RawSnapshot
	emitted := 0

	for {
		select {
		case <-stop:
			return
		default:
		}

		curr, err := collector.Collect()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		derived := collector.Derive(prev, curr)
		if err := output(derived, cfg.Framed, cfg.JSON); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if csvWriter != nil {
			if err := csvWriter.Append(derived); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}

		prev = &curr
		emitted++

		if cfg.Count > 0 && emitted >= cfg.Count {
			break
		}

		if !sleepInterruptible(time.Duration(cfg.Interval*float64(time.Second)), stop) {
			return
		}
	}
}
