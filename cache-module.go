//go:build !windows

package main

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const ssacliTimeout = 10 * time.Second

// batteryStatusKey is the ssacli label preceding a controller's
// cache battery/capacitor status, e.g.:
//
//	Smart Array P840 in Slot 1
//	   Controller Status: OK
//	   Cache Status: OK
//	   Battery/Capacitor Status: OK
const batteryStatusKey = "Battery/Capacitor Status"

type batteryStatus struct {
	slot   string
	model  string
	status string
}

type batteryCollector struct {
	ok      *prometheus.Desc
	info    *prometheus.Desc
	success *prometheus.Desc
}

func newBatteryCollector() *batteryCollector {
	return &batteryCollector{
		ok: prometheus.NewDesc(
			"ssacli_cache_battery_ok",
			"Whether the controller's cache battery/capacitor status is OK (1) or not (0)",
			[]string{"controller_slot", "controller_model"}, nil,
		),
		info: prometheus.NewDesc(
			"ssacli_cache_battery_info",
			"Raw cache battery/capacitor status string reported by ssacli",
			[]string{"controller_slot", "controller_model", "status"}, nil,
		),
		success: prometheus.NewDesc(
			"ssacli_scrape_success",
			"Whether the last scrape of this collector succeeded",
			[]string{"collector"}, nil,
		),
	}
}

func (c *batteryCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.ok
	ch <- c.info
	ch <- c.success
}

func (c *batteryCollector) Collect(ch chan<- prometheus.Metric) {
	statuses, err := getBatteryStatuses()
	if err != nil {
		fmt.Println("cache battery collector error:", err)
		ch <- prometheus.MustNewConstMetric(c.success, prometheus.GaugeValue, 0, "cache_battery")
		return
	}

	for _, s := range statuses {
		ok := 0.0
		if s.status == "OK" {
			ok = 1
		}
		ch <- prometheus.MustNewConstMetric(c.ok, prometheus.GaugeValue, ok, s.slot, s.model)
		ch <- prometheus.MustNewConstMetric(c.info, prometheus.GaugeValue, 1, s.slot, s.model, s.status)
	}
	ch <- prometheus.MustNewConstMetric(c.success, prometheus.GaugeValue, 1, "cache_battery")
}

// parseControllerHeader parses a line like "Smart Array P840 in Slot 1"
// (optionally followed by "(Embedded)") into model and slot number.
func parseControllerHeader(line string) (model, slot string, ok bool) {
	const marker = " in Slot "
	idx := strings.LastIndex(line, marker)
	if idx == -1 {
		return "", "", false
	}
	model = strings.TrimSpace(line[:idx])
	slot = strings.TrimSpace(line[idx+len(marker):])
	if sp := strings.IndexByte(slot, ' '); sp != -1 {
		slot = slot[:sp]
	}
	if model == "" || slot == "" {
		return "", "", false
	}
	return model, slot, true
}

func getBatteryStatuses() ([]batteryStatus, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ssacliTimeout)
	defer cancel()

	out, err := exec.CommandContext(ctx, "ssacli", "ctrl", "all", "show", "status").Output()
	if err != nil {
		return nil, fmt.Errorf("running ssacli: %w", err)
	}

	var statuses []batteryStatus
	var curModel, curSlot string

	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		trimmed := strings.TrimSpace(scanner.Text())
		if trimmed == "" {
			curModel, curSlot = "", ""
			continue
		}
		if model, slot, headerOK := parseControllerHeader(trimmed); headerOK {
			curModel, curSlot = model, slot
			continue
		}
		if curModel == "" {
			continue
		}
		key, value, hasSep := strings.Cut(trimmed, ":")
		if !hasSep || strings.TrimSpace(key) != batteryStatusKey {
			continue
		}
		statuses = append(statuses, batteryStatus{
			slot:   curSlot,
			model:  curModel,
			status: strings.TrimSpace(value),
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("parsing ssacli output: %w", err)
	}

	return statuses, nil
}
