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

type DerivedSnapshot struct {
	Timestamp        time.Time `json:"timestamp"`
	Host             string    `json:"host"`
	ElapsedSeconds   float64   `json:"elapsed_seconds"`
	Load1            float64   `json:"load1"`
	Load5            float64   `json:"load5"`
	Load15           float64   `json:"load15"`
	MemTotalBytes    uint64    `json:"mem_total_bytes"`
	MemAvailable     uint64    `json:"mem_available_bytes"`
	MemUsedBytes     uint64    `json:"mem_used_bytes"`
	MemUsedPercent   float64   `json:"mem_used_percent"`
	SwapTotalBytes   uint64    `json:"swap_total_bytes"`
	SwapFreeBytes    uint64    `json:"swap_free_bytes"`
	SwapUsedBytes    uint64    `json:"swap_used_bytes"`
	SwapUsedPercent  float64   `json:"swap_used_percent"`
	PSwpInTotal      uint64    `json:"pswpin_total"`
	PSwpOutTotal     uint64    `json:"pswpout_total"`
	SwapInRate       float64   `json:"swapin_rate_per_s"`
	SwapOutRate      float64   `json:"swapout_rate_per_s"`
	CPUUsagePercent  *float64  `json:"cpu_usage_percent"`
	ReadIOsTotal     uint64    `json:"read_ios_total"`
	WriteIOsTotal    uint64    `json:"write_ios_total"`
	ReadIOPS         float64   `json:"read_iops"`
	WriteIOPS        float64   `json:"write_iops"`
	RXBytesTotal     uint64    `json:"rx_bytes_total"`
	TXBytesTotal     uint64    `json:"tx_bytes_total"`
	RXRateBPS        float64   `json:"rx_rate_Bps"`
	TXRateBPS        float64   `json:"tx_rate_Bps"`
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

	return RawSnapshot{
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
	}, nil
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
	l1, _ := strconv.ParseFloat(fields[0], 64)
	l5, _ := strconv.ParseFloat(fields[1], 64)
	l15, _ := strconv.ParseFloat(fields[2], 64)
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
		}
		if fields[0] == "pswpout" {
			out = v
		}
	}

	return in, out, nil
}

func (c *Collector) readCPU() (uint64, uint64, error) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0, err
	}

	fields := strings.Fields(strings.Split(string(data), "\n")[0])
	if len(fields) < 5 {
		return 0, 0, errors.New("invalid cpu")
	}

	var total uint64
	var idle uint64

	for i, s := range fields[1:] {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			continue
		}
		total += v
		if i == 3 {
			idle += v
		}
		if i == 4 {
			idle += v
		}
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
		ri, _ := strconv.ParseUint(fields[3], 10, 64)
		wi, _ := strconv.ParseUint(fields[7], 10, 64)
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

	var rx, tx uint64
	lines := strings.Split(string(data), "\n")[2:]
	for _, line := range lines {
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
		rxv, _ := strconv.ParseUint(fields[0], 10, 64)
		txv, _ := strconv.ParseUint(fields[8], 10, 64)
		rx += rxv
		tx += txv
	}

	return rx, tx, nil
}

type DeltaCalculator struct{}

func (d DeltaCalculator) Derive(prev *RawSnapshot, curr RawSnapshot) DerivedSnapshot {
	memUsed := curr.MemTotalBytes - curr.MemAvailableBytes
	swapUsed := curr.SwapTotalBytes - curr.SwapFreeBytes

	out := DerivedSnapshot{
		Timestamp:       curr.Timestamp,
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

	totalDelta := curr.CPUTotal - prev.CPUTotal
	idleDelta := curr.CPUIDleTotal - prev.CPUIDleTotal
	if totalDelta > 0 {
		v := 100.0 * (1.0 - float64(idleDelta)/float64(totalDelta))
		out.CPUUsagePercent = &v
	}

	return out
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

func main() {
	interval := 0.0
	count := 0
	framed := false
	jsonMode := false
	csvPath := ""

	parseArgs(os.Args[1:], &interval, &count, &framed, &jsonMode, &csvPath)

	collector := NewCollector()
	calc := DeltaCalculator{}

	var prev *RawSnapshot

	if csvPath != "" {
		os.MkdirAll(filepath.Dir(csvPath), 0755)
		f, _ := os.OpenFile(csvPath, os.O_CREATE, 0644)
		f.Close()
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		os.Exit(0)
	}()

	i := 0
	for {
		curr, err := collector.Collect()
		if err != nil {
			fmt.Println(err)
			return
		}

		derived := calc.Derive(prev, curr)

		if jsonMode {
			b, _ := json.MarshalIndent(derived, "", "  ")
			fmt.Println(string(b))
		} else if framed {
			printFramed(derived)
		} else {
			fmt.Println(formatLine(derived))
		}

		if csvPath != "" {
			appendCSV(csvPath, derived)
		}

		prev = &curr

		if interval <= 0 {
			break
		}

		i++
		if count > 0 && i >= count {
			break
		}

		time.Sleep(time.Duration(interval * float64(time.Second)))
	}
}

func formatLine(d DerivedSnapshot) string {
	cpu := "N/A"
	if d.CPUUsagePercent != nil {
		cpu = fmt.Sprintf("%.2f%%", *d.CPUUsagePercent)
	}
	return fmt.Sprintf("%d %s cpu=%s load=%.2f/%.2f/%.2f mem=%.1f%% swap=%.1f%%",
		d.Timestamp.Unix(),
		d.Host,
		cpu,
		d.Load1,
		d.Load5,
		d.Load15,
		d.MemUsedPercent,
		d.SwapUsedPercent,
	)
}

func printFramed(d DerivedSnapshot) {
	fmt.Println("==========")
	fmt.Println(formatLine(d))
	fmt.Println("==========")
}

func appendCSV(path string, d DerivedSnapshot) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	w.Write([]string{
		strconv.FormatInt(d.Timestamp.Unix(), 10),
		d.Host,
		fmt.Sprintf("%.2f", d.Load1),
		fmt.Sprintf("%.2f", d.MemUsedPercent),
	})
	w.Flush()
}

func parseArgs(args []string, interval *float64, count *int, framed *bool, jsonMode *bool, csvPath *string) {
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-i", "--interval":
			if i+1 < len(args) {
				v, _ := strconv.ParseFloat(args[i+1], 64)
				*interval = v
				i++
			}
		case "-c", "--count":
			if i+1 < len(args) {
				v, _ := strconv.Atoi(args[i+1])
				*count = v
				i++
			}
		case "--framed":
			*framed = true
		case "--json":
			*jsonMode = true
		case "--csv":
			if i+1 < len(args) {
				*csvPath = args[i+1]
				i++
			}
		}
	}
}
