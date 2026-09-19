//go:build !windows

// Package main: logical drive RAID status reporting.
//
// This file provides a Prometheus collector that exports, for every
// logical drive on every Smart Array controller, its rebuild progress as
// a percentage. It is registered from main only when the config key
// "raid_status" under [collectors] is explicitly true (missing or false disables it).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type raidStatusCollector struct {
	rebuildDesc *prometheus.Desc

	// instance is this host's name, attached to the metric so the
	// source host is identifiable.
	instance string
}

// NewRaidStatusCollector builds the logical drive status collector.
func NewRaidStatusCollector() *raidStatusCollector {
	instance, err := os.Hostname()
	if err != nil || instance == "" {
		log.Printf("[raid_status] could not determine hostname (%v); using %q as instance label", err, "unknown")
		instance = "unknown"
	}
	return &raidStatusCollector{
		instance: instance,
		rebuildDesc: prometheus.NewDesc(
			"hpraid_logical_drive_rebuild_percent",
			"Logical drive rebuild progress as a percentage (0-100): 100 if the drive status is OK, the rebuild percentage if it is rebuilding, 0 if it is not OK and not rebuilding",
			[]string{"instance", "controller_slot", "logical_drive", "logical_drive_desc"}, nil,
		),
	}
}

func (c *raidStatusCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.rebuildDesc
}

// Collect queries `ssacli ... ld all show status` live on each scrape
// (it is fast, unlike the ADU report). On failure the error is logged and
// no logical drive series are emitted for that controller/scrape.
func (c *raidStatusCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), ssacliTimeout)
	defer cancel()

	slots, err := discoverControllerSlots(ctx)
	if err != nil {
		log.Printf("[raid_status] controller discovery failed: %v", err)
		return
	}
	for _, slot := range slots {
		out, err := exec.CommandContext(ctx, "ssacli", "ctrl", fmt.Sprintf("slot=%d", slot), "ld", "all", "show", "status").Output()
		if err != nil {
			log.Printf("[raid_status] slot %d: logical drive status failed: %v", slot, err)
			continue
		}
		slotStr := strconv.Itoa(slot)
		for _, ld := range parseLogicalDriveStatus(string(out)) {
			ch <- prometheus.MustNewConstMetric(
				c.rebuildDesc, prometheus.GaugeValue, ld.percent,
				c.instance, slotStr, ld.id, ld.desc,
			)
		}
	}
}

type logicalDriveRebuild struct {
	id      string
	desc    string
	percent float64
}

// logicalDriveRE matches a line of `ssacli ctrl slot=N ld all show
// status` output, e.g.:
//
//	logicaldrive 1 (279.4 GB, RAID 1): OK
//	logicaldrive 2 (558.9 GB, RAID 5): Recovering, 45% complete
//	logicaldrive 3 (136.7 GB, RAID 1): Interim Recovery Mode
var logicalDriveRE = regexp.MustCompile(`(?i)^logicaldrive\s+(\d+)\s+\(([^)]*)\):\s*(.+)$`)

var percentCompleteRE = regexp.MustCompile(`(\d+(?:\.\d+)?)\s*%\s*complete`)

// parseLogicalDriveStatus returns one entry per logicaldrive line. The
// percent is 100 for status "OK"; otherwise the "N% complete" figure if
// the status contains one, else 0. The value is clamped to 0-100.
func parseLogicalDriveStatus(out string) []logicalDriveRebuild {
	var results []logicalDriveRebuild
	for _, rawLine := range strings.Split(out, "\n") {
		m := logicalDriveRE.FindStringSubmatch(strings.TrimSpace(rawLine))
		if m == nil {
			continue
		}
		status := strings.TrimSpace(m[3])
		percent := 0.0
		if strings.EqualFold(status, "OK") {
			percent = 100
		} else if pm := percentCompleteRE.FindStringSubmatch(status); pm != nil {
			percent, _ = strconv.ParseFloat(pm[1], 64)
		}
		if percent < 0 {
			percent = 0
		} else if percent > 100 {
			percent = 100
		}
		results = append(results, logicalDriveRebuild{id: m[1], desc: strings.TrimSpace(m[2]), percent: percent})
	}
	return results
}
