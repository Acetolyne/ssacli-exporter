# ssacli-exporter

## Description
ssacli-exporter is a prometheus exporter that runs on HP servers that have the HPE ssacli installed TODO: link to ssacli documentation.

## Badges
On some READMEs, you may see small images that convey metadata, such as whether or not all the tests are passing for the project. You can use Shields to add some to your README. Many services also have instructions for adding a badge.

## Installation
Install the ssacli-exporter on the server and run it as a service.

### Prerequisites
you must install the ssacli on the server that the ssacli exporter runs on as it needs to use the ssacli to collect data. The data it collects depends on the settings you enable in the settings.ini file.

## Configuration
On startup, the exporter looks for `/etc/ssacli-exporter/settings.ini`. If it does not exist, the directory and file are created automatically with default values (see `Example_settings.ini` for the format). Edit that file to change the listen address/port, scrape delay, and which collectors are enabled.

## Usage
To run ssacli-exporter as a systemd service (running as root, since ssacli requires root access to query the controller), with the binary installed at `/sbin/ssacli-exporter`, create `/etc/systemd/system/ssacli-exporter.service`:

```ini
[Unit]
Description=ssacli Prometheus Exporter
After=network.target

[Service]
Type=simple
User=root
ExecStart=/sbin/ssacli-exporter
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Then enable and start it:

```
sudo systemctl daemon-reload
sudo systemctl enable --now ssacli-exporter
```
