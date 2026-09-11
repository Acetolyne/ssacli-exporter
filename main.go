package main

import (
	"log"
	"net/http"
	"os"

	"gopkg.in/ini.v1"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	configDir  = "/etc/ssacli-exporter"
	configFile = configDir + "/settings.ini"

	defaultConfig = `[general]
ip = 0.0.0.0
port = 9290
delay_seconds = 60

[collectors]
cache_battery_status = true
`
)

var Cfg *ini.File
var Delay int

func init() {
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		if err := os.MkdirAll(configDir, 0755); err != nil {
			log.Fatalf("creating config directory %s: %v", configDir, err)
		}
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

	// The Handler function provides a default handler to expose metrics
	// via an HTTP server. "/metrics" is the usual endpoint for that.
	http.Handle("/metrics", promhttp.Handler())

	log.Fatal(http.ListenAndServe(Cfg.Section("general").Key("ip").String()+":"+Cfg.Section("general").Key("port").String(), nil))
}
