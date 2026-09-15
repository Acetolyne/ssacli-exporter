//go:build !windows

// Package main: disk error reporting.
//
// This file provides a Prometheus collector for HPE Smart Array ADU
// (Array Diagnostics Utility) reports. It discovers every Smart Array
// controller present (via `ssacli ctrl all show status`), generates a
// text-format ADU report per controller (via
// `ssacli ctrl slot=<N> diag file=<path> zip=off ris=on`), and exposes
// per-physical-drive error metrics decoded from SCSI Sense Key / ASC /
// ASCQ codes into human-readable error types.
//
// ADU report generation is slow (can take minutes on a fully populated
// controller), so it is not done on every Prometheus scrape. Instead,
// NewDiskErrorCollector starts a background goroutine that regenerates
// the report on its own interval (config key "disk_report_interval_mins"
// under [collectors], default 15 minutes) and keeps the most recently
// parsed results in a snapshot. Prometheus scrapes always read the
// current snapshot immediately; the previous snapshot keeps being served
// until a new report finishes generating and parsing successfully, so a
// slow or failed refresh never produces a gap in the exposed metrics.
//
// Call NewDiskErrorCollector from main and register it with
// prometheus.MustRegister only when the config key "disk_error_status"
// under [collectors] is true (it defaults to false).
package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/ini.v1"
)

// defaultDiskReportIntervalMins is used when "disk_report_interval_mins"
// is missing, non-numeric, or zero/negative in the config file.
const defaultDiskReportIntervalMins = 15

// diskReportTmpPathBase is the base path used to write temporary ADU
// report files (tmpfs recommended). Each discovered controller slot gets
// its own suffixed path so a multi-controller refresh can't clobber
// itself mid-cycle.
const diskReportTmpPathBase = "/dev/shm/adu_report.txt"

// -----------------------------------------------------------------------
// Collector
// -----------------------------------------------------------------------

// driveTotalMetric is one row of the "errors logged total" / "critical"
// family, keyed per (controller slot, drive).
type driveTotalMetric struct {
	slot              string
	driveID           string
	driveDesc         string
	errorsLoggedTotal int
	critical          bool
}

// driveErrCountMetric is one row of the per-decoded-error-type count
// family, keyed per (controller slot, drive, error description).
type driveErrCountMetric struct {
	slot      string
	driveID   string
	driveDesc string
	errorDesc string
	critical  bool
	count     int
}

// diskErrorSnapshot holds one fully-parsed generation of ADU report
// results, ready to be served to Prometheus without any further parsing.
type diskErrorSnapshot struct {
	totals []driveTotalMetric
	errors []driveErrCountMetric
}

// diskErrorCollector is a prometheus.Collector that serves drive error
// metrics from a snapshot refreshed on a long interval by a background
// goroutine, since generating a fresh ADU report is too slow to do on
// every scrape.
type diskErrorCollector struct {
	mu         sync.RWMutex
	snapshot   *diskErrorSnapshot
	scrapeOK   bool
	lastScrape time.Time

	errorCountDesc *prometheus.Desc
	totalDesc      *prometheus.Desc
	criticalDesc   *prometheus.Desc
	successDesc    *prometheus.Desc
	lastScrapeDesc *prometheus.Desc
}

// NewDiskErrorCollector builds a disk error collector, reading the
// refresh interval from cfg's [collectors] disk_report_interval_mins key
// (default 15 minutes if missing or <= 0), and starts its background
// refresh goroutine. The returned collector should be registered with
// prometheus.MustRegister only when [collectors] disk_error_status is
// true; it is safe to construct and immediately register.
//
// The refresh goroutine runs for the lifetime of the process; there is
// no way to stop it, matching the process-lifetime nature of the rest of
// this exporter.
func NewDiskErrorCollector(cfg *ini.File) *diskErrorCollector {
	mins := cfg.Section("collectors").Key("disk_report_interval_mins").MustInt(defaultDiskReportIntervalMins)
	if mins <= 0 {
		mins = defaultDiskReportIntervalMins
	}
	interval := time.Duration(mins) * time.Minute

	c := &diskErrorCollector{
		errorCountDesc: prometheus.NewDesc(
			"hpraid_physical_drive_error_count",
			"Count of a specific decoded error type logged for a physical drive",
			[]string{"drive_id", "drive_desc", "error_desc", "critical", "controller_slot"}, nil,
		),
		totalDesc: prometheus.NewDesc(
			"hpraid_physical_drive_errors_logged_total",
			"Total 'Errors Logged' count reported by the controller for this drive",
			[]string{"drive_id", "drive_desc", "controller_slot"}, nil,
		),
		criticalDesc: prometheus.NewDesc(
			"hpraid_physical_drive_critical",
			"1 if this drive has any Medium Error or Hardware Error entries logged, else 0",
			[]string{"drive_id", "drive_desc", "controller_slot"}, nil,
		),
		successDesc: prometheus.NewDesc(
			"hpraid_adu_scrape_success",
			"1 if the last ADU report generation and parse succeeded, else 0",
			nil, nil,
		),
		lastScrapeDesc: prometheus.NewDesc(
			"hpraid_adu_last_scrape_timestamp_seconds",
			"Unix timestamp of the last successful ADU report parse",
			nil, nil,
		),
	}

	go c.refreshLoop(diskReportTmpPathBase, interval)
	return c
}

func (c *diskErrorCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.errorCountDesc
	ch <- c.totalDesc
	ch <- c.criticalDesc
	ch <- c.successDesc
	ch <- c.lastScrapeDesc
}

func (c *diskErrorCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	snap := c.snapshot
	scrapeOK := c.scrapeOK
	lastScrape := c.lastScrape
	c.mu.RUnlock()

	if snap != nil {
		for _, m := range snap.totals {
			ch <- prometheus.MustNewConstMetric(
				c.totalDesc, prometheus.GaugeValue, float64(m.errorsLoggedTotal),
				m.driveID, m.driveDesc, m.slot,
			)
			critVal := 0.0
			if m.critical {
				critVal = 1
			}
			ch <- prometheus.MustNewConstMetric(
				c.criticalDesc, prometheus.GaugeValue, critVal,
				m.driveID, m.driveDesc, m.slot,
			)
		}
		for _, m := range snap.errors {
			critLabel := "false"
			if m.critical {
				critLabel = "true"
			}
			ch <- prometheus.MustNewConstMetric(
				c.errorCountDesc, prometheus.GaugeValue, float64(m.count),
				m.driveID, m.driveDesc, m.errorDesc, critLabel, m.slot,
			)
		}
	}

	successVal := 0.0
	if scrapeOK {
		successVal = 1
	}
	ch <- prometheus.MustNewConstMetric(c.successDesc, prometheus.GaugeValue, successVal)
	ch <- prometheus.MustNewConstMetric(c.lastScrapeDesc, prometheus.GaugeValue, float64(lastScrape.Unix()))
}

// -----------------------------------------------------------------------
// Report generation / refresh
// -----------------------------------------------------------------------

// discoverControllerSlots runs `ssacli ctrl all show status` and returns
// the slot number of every Smart Array controller found, reusing the
// same header-parsing logic as the cache battery collector.
func discoverControllerSlots(ctx context.Context) ([]int, error) {
	out, err := exec.CommandContext(ctx, "ssacli", "ctrl", "all", "show", "status").Output()
	if err != nil {
		return nil, fmt.Errorf("running ssacli: %w", err)
	}

	var slots []int
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		trimmed := strings.TrimSpace(scanner.Text())
		if trimmed == "" {
			continue
		}
		if _, slotStr, ok := parseControllerHeader(trimmed); ok {
			if n, err := strconv.Atoi(slotStr); err == nil {
				slots = append(slots, n)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("parsing ssacli output: %w", err)
	}
	if len(slots) == 0 {
		return nil, fmt.Errorf("no Smart Array controllers found")
	}
	return slots, nil
}

// fileWaitAttempts/fileWaitDelay bound how long readGeneratedReportFile
// will retry locating and reading the report file after ssacli's process
// exits. ssacli has been observed to still be finalizing the file (e.g.
// renaming it to add a ".zip" suffix) for a brief moment after it prints
// "done" and its process exits, so a single immediate check can race it.
const (
	fileWaitAttempts = 10
	fileWaitDelay    = 500 * time.Millisecond
)

// readGeneratedReportFile tries to read the report ssacli just generated
// for tmpPath, retrying briefly against both the requested name and a
// "<tmpPath>.zip" variant (which ssacli has been observed to write even
// with zip=off) to ride out the rename race described above. It returns
// whichever path actually worked, its raw bytes, and does not remove the
// file -- the caller is responsible for cleanup once it's done with it.
func readGeneratedReportFile(tmpPath string) (path string, raw []byte, err error) {
	candidates := []string{tmpPath, tmpPath + ".zip"}
	for attempt := 0; attempt < fileWaitAttempts; attempt++ {
		for _, p := range candidates {
			data, rerr := os.ReadFile(p)
			if rerr == nil {
				return p, data, nil
			}
			err = rerr
		}
		time.Sleep(fileWaitDelay)
	}
	return "", nil, fmt.Errorf("no readable file at %s or %s after %d attempts: %w",
		candidates[0], candidates[1], fileWaitAttempts, err)
}

// generateADUReport runs ssacli to produce a fresh ADU report for the
// given controller slot and returns its parsed-out text contents.
//
// ssacli's own (short) console output is always logged, on success or
// failure, along with the byte count read back from disk -- this is
// what tells you whether ssacli actually ran (and what it said) versus
// ran but produced a report file that couldn't be read back correctly.
//
// Despite passing zip=off, ssacli has been observed to still write the
// report to "<tmpPath>.zip" rather than the bare requested tmpPath, so
// both names are checked. And since it's not guaranteed whether that
// ".zip" file actually holds a compressed archive or is plain text under
// a misleading extension, extractReportText sniffs the content instead
// of trusting the name.
func generateADUReport(ctx context.Context, slot int, tmpPath string) (string, error) {
	args := []string{"ctrl", fmt.Sprintf("slot=%d", slot), "diag", fmt.Sprintf("file=%s", tmpPath), "zip=off", "ris=on"}
	cmd := exec.CommandContext(ctx, "ssacli", args...)
	out, err := cmd.CombinedOutput()
	log.Printf("[disk_errors] slot %d: ran `ssacli %s`, output: %q", slot, strings.Join(args, " "), strings.TrimSpace(string(out)))
	if err != nil {
		return "", fmt.Errorf("ssacli failed: %w (output: %s)", err, string(out))
	}

	actualPath, raw, err := readGeneratedReportFile(tmpPath)
	if err != nil {
		return "", fmt.Errorf("reading generated report: %w", err)
	}
	defer os.Remove(actualPath)
	log.Printf("[disk_errors] slot %d: read %d bytes from %s", slot, len(raw), actualPath)

	text, err := extractReportText(slot, raw)
	if err != nil {
		return "", fmt.Errorf("extracting report from %s: %w", actualPath, err)
	}
	return text, nil
}

// reportPreviewBytes bounds how much of the final report text gets
// logged as a preview, so a diagnosis-friendly snippet lands in the log
// without dumping the entire (often hundreds-of-KB) report every cycle.
const reportPreviewBytes = 400

// logReportPreview logs a short prefix of the report text that's about
// to be handed to ParseADUReport, so a "0 rows parsed" result can be
// diagnosed by comparing what ssacli actually produced against what
// driveHeaderRE expects.
func logReportPreview(slot int, data []byte) {
	preview := data
	suffix := ""
	if len(preview) > reportPreviewBytes {
		preview = preview[:reportPreviewBytes]
		suffix = "... (truncated)"
	}
	log.Printf("[disk_errors] slot %d: report text preview: %q%s", slot, string(preview), suffix)
}

// aduReportEntryName is the fixed name ssacli uses, inside the zip it
// generates, for the actual report text -- regardless of what path was
// passed via file= on the command line.
const aduReportEntryName = "ADUReport.txt"

// extractReportText returns the ADU report text contained in raw. If raw
// is a genuine zip archive (detected by its magic number, not by file
// extension), the entry named ADUReport.txt is read out and returned.
// Otherwise raw is assumed to already be plain report text and is
// returned as-is. Either way, a preview of the final text is logged so a
// parse that comes back with 0 rows can still be diagnosed.
func extractReportText(slot int, raw []byte) (string, error) {
	if !bytes.HasPrefix(raw, []byte("PK")) {
		logReportPreview(slot, raw)
		return string(raw), nil
	}

	zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		return "", fmt.Errorf("opening zip archive: %w", err)
	}
	if len(zr.File) == 0 {
		return "", fmt.Errorf("zip archive contains no files")
	}

	var report *zip.File
	names := make([]string, 0, len(zr.File))
	for _, f := range zr.File {
		names = append(names, f.Name)
		if filepath.Base(f.Name) == aduReportEntryName {
			report = f
		}
	}
	if report == nil {
		return "", fmt.Errorf("zip archive has no %s entry (contains: %s)", aduReportEntryName, strings.Join(names, ", "))
	}

	rc, err := report.Open()
	if err != nil {
		return "", fmt.Errorf("opening %s inside zip archive: %w", report.Name, err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return "", fmt.Errorf("reading %s inside zip archive: %w", report.Name, err)
	}
	logReportPreview(slot, data)
	return string(data), nil
}

// buildSnapshot merges parsed rows from one or more controllers'
// reports into a snapshot, deduplicating so each metric label
// combination is emitted exactly once regardless of how many report
// rows contributed to it.
func buildSnapshot(rows []DriveError) *diskErrorSnapshot {
	type driveKey struct {
		slot      string
		driveID   string
		driveDesc string
	}
	type errKey struct {
		driveKey
		errorDesc string
		critical  bool
	}

	totals := map[driveKey]*driveTotalMetric{}
	errCounts := map[errKey]int{}

	for _, row := range rows {
		dk := driveKey{row.Slot, row.DriveID, row.DriveDesc}
		t, ok := totals[dk]
		if !ok {
			t = &driveTotalMetric{slot: row.Slot, driveID: row.DriveID, driveDesc: row.DriveDesc}
			totals[dk] = t
		}
		t.errorsLoggedTotal = row.ErrorsLoggedTotal
		t.critical = t.critical || row.Critical

		// Always record an entry, including the zero-count "none" row
		// ParseADUReport emits for a drive with a clean error log, so a
		// healthy drive still gets an explicit
		// hpraid_physical_drive_error_count{...} 0 series instead of no
		// series at all -- important for queries/alerts that expect
		// every known drive to have a value rather than being silently
		// absent.
		ek := errKey{dk, row.ErrorDesc, row.Critical}
		errCounts[ek] += row.Count
	}

	snap := &diskErrorSnapshot{}
	for _, t := range totals {
		snap.totals = append(snap.totals, *t)
	}
	for ek, count := range errCounts {
		snap.errors = append(snap.errors, driveErrCountMetric{
			slot: ek.slot, driveID: ek.driveID, driveDesc: ek.driveDesc,
			errorDesc: ek.errorDesc, critical: ek.critical, count: count,
		})
	}
	return snap
}

// refreshOnce discovers all controllers, generates and parses a fresh
// ADU report for each, and — only if every controller's report is
// generated and parsed successfully — atomically swaps in the new
// snapshot. On any failure the previous snapshot (if any) is left in
// place untouched, so consumers keep seeing the last-good metrics; only
// the success/last-scrape metrics reflect the failure.
func (c *diskErrorCollector) refreshOnce(tmpPathBase string) {
	discCtx, discCancel := context.WithTimeout(context.Background(), ssacliTimeout)
	slots, err := discoverControllerSlots(discCtx)
	discCancel()
	if err != nil {
		log.Printf("[disk_errors] controller discovery failed: %v", err)
		c.mu.Lock()
		c.scrapeOK = false
		c.mu.Unlock()
		return
	}
	log.Printf("[disk_errors] discovered controller slots: %v", slots)

	var allRows []DriveError
	for _, slot := range slots {
		tmpPath := fmt.Sprintf("%s.slot%d", tmpPathBase, slot)
		var rowCount int
		err := func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()

			// generateADUReport removes whichever file it actually found
			// and read (tmpPath itself, or tmpPath+".zip") once it's done.
			text, err := generateADUReport(ctx, slot, tmpPath)
			if err != nil {
				return err
			}
			rows := ParseADUReport(text)
			rowCount = len(rows)
			allRows = append(allRows, rows...)
			return nil
		}()
		if err != nil {
			log.Printf("[disk_errors] slot %d report generation failed: %v", slot, err)
			c.mu.Lock()
			c.scrapeOK = false
			c.mu.Unlock()
			return
		}
		log.Printf("[disk_errors] slot %d: parsed %d row(s) from report", slot, rowCount)
	}

	snap := buildSnapshot(allRows)
	log.Printf("[disk_errors] refresh succeeded: %d drive(s), %d error row(s) across %d controller(s)",
		len(snap.totals), len(snap.errors), len(slots))
	c.mu.Lock()
	c.snapshot = snap
	c.scrapeOK = true
	c.lastScrape = time.Now()
	c.mu.Unlock()
}

// refreshLoop calls refreshOnce immediately, then again every interval,
// forever. It is meant to be run in its own goroutine for the lifetime
// of the process.
func (c *diskErrorCollector) refreshLoop(tmpPathBase string, interval time.Duration) {
	for {
		c.refreshOnce(tmpPathBase)
		time.Sleep(interval)
	}
}

// -----------------------------------------------------------------------
// ADU report parsing
//
// SCSI Sense Key lookup (top-level error category), ASC/ASCQ decoding,
// and the regex-based text parser for HPE Smart Array ADU reports.
// -----------------------------------------------------------------------

var senseKeyMap = map[int]string{
	0x00: "No Sense",
	0x01: "Recovered Error",
	0x02: "Not Ready",
	0x03: "Medium Error",
	0x04: "Hardware Error",
	0x05: "Illegal Request",
	0x06: "Unit Attention",
	0x07: "Data Protect",
	0x08: "Blank Check",
	0x09: "Vendor Specific",
	0x0A: "Copy Aborted",
	0x0B: "Aborted Command",
	0x0C: "Reserved (Equal)",
	0x0D: "Volume Overflow",
	0x0E: "Miscompare",
	0x0F: "Reserved",
}

type ascKey struct {
	asc  int
	ascq int
}

// ASC/ASCQ lookup for the specific codes worth naming explicitly.
// Anything not in this table falls back to a generic "ASC 0xXX/ASCQ 0xXX" label.
var ascAscqMap = map[ascKey]string{
	{0x11, 0x00}: "Unrecovered Read Error",
	{0x11, 0x01}: "Read Retries Exhausted",
	{0x11, 0x04}: "Unrecovered Read Error - Auto Reallocate Failed",
	{0x29, 0x00}: "Power On, Reset, or Bus Device Reset Occurred",
	{0x21, 0x00}: "Logical Block Address Out of Range",
	{0x44, 0x00}: "Internal Target Failure",
	{0x00, 0x00}: "No Additional Sense Information",
}

// Sense keys that indicate a genuine, actionable drive/media problem.
// Everything else (Unit Attention from a reset, aborted commands from a
// rescan, etc.) is logged but not treated as critical.
var criticalSenseKeys = map[int]bool{
	0x03: true, // Medium Error
	0x04: true, // Hardware Error
}

// Header for each physical drive's error log section, e.g.:
// "Smart Array P840 in slot 1 : Internal Drive Cage at Port 1I : Box 1 :
//
//	Physical Drive (2 TB SATA HDD) 1I:1:11 : Serial SCSI Physical Drive Error Log"
var driveHeaderRE = regexp.MustCompile(
	`(?i)Smart Array .*? in slot (?P<slot>\d+).*?` +
		`Physical Drive \((?P<drivedesc>[^)]+)\)\s+(?P<driveid>[0-9A-Za-z:]+)\s*:\s*` +
		`Serial SCSI Physical Drive Error Log`,
)

var errorsLoggedRE = regexp.MustCompile(`Errors Logged\s+(\d+)`)

// The entry table for a drive is bounded by "Physical Drive Error Log
// Entries" and the next standalone "Reference Time" SUMMARY line -- it is
// NOT safe to scan all the way to the next drive's header, because
// unrelated hex-column tables (Mode Sense pages, Inquiry data, etc.) appear
// in between and can produce false-positive matches against the entry-row
// regex. Note: "Reference Time" also appears as a *column header label*
// directly after "Physical Drive Error Log Entries" (as part of "...Block
// Reference Time Additional Information"), so a bare substring search
// matches too early even when real entries follow. The real summary line
// is "Reference Time" immediately followed by a hex value on the same
// line; the column-header occurrence is followed by more header text
// ("Additional Information"), not a hex value -- that's the distinguisher.
var referenceTimeRE = regexp.MustCompile(`Reference Time\s+0x[0-9A-Fa-f]+`)

// One entry row, e.g.:
// 0x01   0x28   0x02   0x04   0x03   0x11 0x00 0x01   0x0d6d2ba0   0x0005a99e   0x0000
var entryRE = regexp.MustCompile(
	`^\s*0x([0-9A-Fa-f]{2})\s+` + // Error Type
		`0x([0-9A-Fa-f]{2})\s+` + // SCSI Operation Code
		`0x([0-9A-Fa-f]{2})\s+` + // SCSI Status
		`0x([0-9A-Fa-f]{2})\s+` + // CAM Status
		`0x([0-9A-Fa-f]{2})\s+` + // Sense Key
		`0x([0-9A-Fa-f]{2})\s+` + // ASC
		`0x([0-9A-Fa-f]{2})\s+` + // ASCQ
		`0x([0-9A-Fa-f]{2})\s+`, // Block Valid
)

// DriveError is one (drive, decoded-error-type) result row.
type DriveError struct {
	Slot              string
	DriveID           string
	DriveDesc         string
	ErrorsLoggedTotal int
	SenseKey          int // -1 if this row represents a clean drive with no entries
	ErrorDesc         string
	Count             int
	Critical          bool
}

func hexByte(s string) int {
	v, _ := strconv.ParseInt(s, 16, 32)
	return int(v)
}

func decodeError(senseKey, asc, ascq int) string {
	senseDesc, ok := senseKeyMap[senseKey]
	if !ok {
		senseDesc = "Unknown Sense Key 0x" + strconv.FormatInt(int64(senseKey), 16)
	}
	if ascDesc, ok := ascAscqMap[ascKey{asc, ascq}]; ok {
		return senseDesc + " - " + ascDesc
	}
	return senseDesc + " (ASC 0x" + hexPad2(asc) + "/ASCQ 0x" + hexPad2(ascq) + ")"
}

func hexPad2(v int) string {
	s := strconv.FormatInt(int64(v), 16)
	if len(s) < 2 {
		s = "0" + s
	}
	return s
}

// ParseADUReport parses a full ADU text report and returns one row per
// (drive, decoded error type) combination, plus a zero-count "none" row
// for any drive with a clean error log.
func ParseADUReport(text string) []DriveError {
	var results []DriveError

	headerMatches := driveHeaderRE.FindAllStringSubmatchIndex(text, -1)
	slotIdx := driveHeaderRE.SubexpIndex("slot")
	descIdx := driveHeaderRE.SubexpIndex("drivedesc")
	idIdx := driveHeaderRE.SubexpIndex("driveid")

	for i, hm := range headerMatches {
		start := hm[1] // end of the full header match
		var regionEnd int
		if i+1 < len(headerMatches) {
			regionEnd = headerMatches[i+1][0]
		} else {
			regionEnd = len(text)
		}
		searchRegion := text[start:regionEnd]

		slot := text[hm[2*slotIdx]:hm[2*slotIdx+1]]
		driveID := text[hm[2*idIdx]:hm[2*idIdx+1]]
		driveDesc := text[hm[2*descIdx]:hm[2*descIdx+1]]

		errorsLoggedTotal := 0
		if m := errorsLoggedRE.FindStringSubmatch(searchRegion); m != nil {
			n, _ := strconv.Atoi(m[1])
			errorsLoggedTotal = n
		}

		section := searchRegion
		if refMatch := referenceTimeRE.FindStringIndex(searchRegion); refMatch != nil {
			section = searchRegion[:refMatch[0]]
		}

		type errKey struct {
			senseKey int
			desc     string
		}
		errorCounts := map[errKey]int{}

		for _, rawLine := range strings.Split(section, "\n") {
			line := strings.TrimRight(rawLine, "\r")
			m := entryRE.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			senseKey := hexByte(m[5])
			asc := hexByte(m[6])
			ascq := hexByte(m[7])
			desc := decodeError(senseKey, asc, ascq)
			errorCounts[errKey{senseKey, desc}]++
		}

		if len(errorCounts) == 0 {
			results = append(results, DriveError{
				Slot:              slot,
				DriveID:           driveID,
				DriveDesc:         driveDesc,
				ErrorsLoggedTotal: errorsLoggedTotal,
				SenseKey:          -1,
				ErrorDesc:         "none",
				Count:             0,
				Critical:          false,
			})
			continue
		}

		for k, count := range errorCounts {
			results = append(results, DriveError{
				Slot:              slot,
				DriveID:           driveID,
				DriveDesc:         driveDesc,
				ErrorsLoggedTotal: errorsLoggedTotal,
				SenseKey:          k.senseKey,
				ErrorDesc:         k.desc,
				Count:             count,
				Critical:          criticalSenseKeys[k.senseKey],
			})
		}
	}

	return results
}
