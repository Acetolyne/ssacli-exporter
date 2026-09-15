package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"testing"

	"gopkg.in/ini.v1"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	configDir  = "/etc/ssacli-exporter"
	configFile = configDir + "/settings.ini"
	logFile    = configDir + "/error.log"

	defaultConfig = `[general]
ip = 0.0.0.0
port = 9290
delay_seconds = 60

[collectors]
cache_battery_status = true
disk_error_status = false
disk_report_interval_mins = 15
`
)

var Cfg *ini.File
var Delay int

func init() {
	// ssacli itself requires root to talk to the Smart Array controllers,
	// and this exporter also needs to write into the root-owned configDir
	// (settings.ini, error.log). Fail fast with a clear message instead
	// of limping along and hitting confusing permission errors later.
	// Skipped under `go test` (testing.Testing(), Go 1.21+) so the test
	// suite doesn't require root -- this init() runs in every test binary
	// for this package too, root or not.
	if euid := os.Geteuid(); euid != 0 && !testing.Testing() {
		log.Fatalf("ssacli-exporter must be run as root (uid 0); current effective uid is %d", euid)
	}

	// configDir must exist before we can open the log file below, whether
	// or not settings.ini already exists there.
	if err := os.MkdirAll(configDir, 0755); err != nil {
		log.Fatalf("creating config directory %s: %v", configDir, err)
	}

	// Send every log.* call (from this file and every other collector)
	// to both stderr (so `journalctl -u <service>` / foreground runs
	// still show it live) and a persistent file, so errors and ssacli
	// output survive process restarts and can be reviewed after the
	// fact. This is best-effort, not fatal: configDir is root-owned in
	// production (ssacli itself requires root), so a plain `go build`/
	// `go test`/local run as a non-root user can't create the file --
	// that should just fall back to stderr-only logging, not crash.
	if logf, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err != nil {
		log.Printf("warning: could not open log file %s (%v); logging to stderr only", logFile, err)
	} else {
		log.SetOutput(io.MultiWriter(os.Stderr, logf))
	}

	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		if err := os.WriteFile(configFile, []byte(defaultConfig), 0644); err != nil {
			log.Fatalf("writing default config %s: %v", configFile, err)
		}
	}

	cfg, err := ini.Load(configFile)
	if err != nil {
		log.Fatalf("loading config %s: %v", configFile, err)
	}
	Cfg = cfg
	Delay, _ = Cfg.Section("general").Key("delay_seconds").Int()
}

//@todo register the binary as a service
//@todo make matching exporter files for windows
//@todo make update argument that will automatically update the agent from master git branch
//@todo auto build binaries with github webhooks
func main() {

	//Get the settings from config file
	//fmt.Println("App Mode:", cfg.Section("").Key("app_mode").String())
	//fmt.Println("Allowed Drive Prefixes", Cfg.Section("drives").Key("allowed_prefixes").String())
	//go ExportDisks()
	//go ExportMemory()

	if enabled, _ := Cfg.Section("collectors").Key("cache_battery_status").Bool(); enabled {
		prometheus.MustRegister(newBatteryCollector())
	}

	if enabled, _ := Cfg.Section("collectors").Key("disk_error_status").Bool(); enabled {
		prometheus.MustRegister(NewDiskErrorCollector(Cfg))
	}

	// The Handler function provides a default handler to expose metrics
	// via an HTTP server. "/metrics" is the usual endpoint for that.
	http.Handle("/metrics", promhttp.Handler())

	log.Fatal(http.ListenAndServe(Cfg.Section("general").Key("ip").String()+":"+Cfg.Section("general").Key("port").String(), nil))
}
