# ctrlX Data Layer Node Mapper (.NET)

Reads a list of source/destination node pairs from the **ctrlX app data storage**,
subscribes to every source node, and forwards each data change to the mapped
destination nodes with a single **bulk write**.

Built against `ctrlX-DataLayer` 6.x (`Datalayer` .NET API 6.0.1).

## How it works

| Step | Where | What |
| --- | --- | --- |
| 1 | `AppDataStorage.Load()` | Reads `mappings.json` from `$SNAP_COMMON/solutions/activeConfiguration/sdk-net-datalayer-mapper/`. Writes a template on first start. |
| 2 | `NodeMapper.Apply()` | Iterates all pairs and builds the route table `source -> destination[]`. |
| 3 | `NodeMapper.Apply()` | Creates **one** subscription and calls `SubscribeMulti(sources)` with every distinct source. Falls back to individual `Subscribe()` calls if one address is unreachable. |
| 4 | `NodeMapper.OnDataChanged()` | Resolves the source address from the `NotifyInfo` flatbuffer, copies the value once per destination into a pending buffer, signals the flush loop. |
| 5 | `NodeMapper.FlushAsync()` | Coalesces the buffer into `client.BulkWriteAsync(addresses, values)` — one round trip per publish batch. |
| 6 | `AppDataService` | Serves the ctrlX `save`/`load` commands; `load` re-reads the file and re-applies it without restarting the app. |

Two details worth keeping if you adapt this:

- **Nothing synchronous is called from the subscription callback.** The Data Layer
  returns `WOULD_BLOCK` if you call a synchronous API method in that context, so the
  callback only copies values and the bulk write happens on the flush task via
  `BulkWriteAsync`.
- **Notification values are copied.** `args.Item.Value` is owned by the subscription
  and is not valid after the callback returns, so each destination gets its own
  `new Variant(args.Item.Value)`, disposed after the write completes.

`writeDebounceMillis` is the small grace period after the first pending change before
the batch goes out. It's what turns N individual notifications from one publish cycle
into one bulk write. Set it to `0` to write as soon as the first change arrives.

## Configuration

`mappings.json`, in the app directory of the active configuration. The root element is
a JSON object (not an array), per the ctrlX persistence guideline; the pairs live under
`mappings`.

```json
{
  "subscriptionId": "net-node-mapper",
  "publishIntervalMillis": 1000,
  "keepaliveIntervalMillis": 10000,
  "errorIntervalMillis": 10000,
  "samplingIntervalMicros": 1000000,
  "deadbandValue": 0.0,
  "writeDebounceMillis": 50,
  "mappings": [
    {
      "source": "framework/metrics/system/cpu-utilisation-percent",
      "destination": "plc/app/Application/sym/PLC_PRG/cpuLoad",
      "enabled": true
    }
  ]
}
```

| Field | Meaning |
| --- | --- |
| `subscriptionId` | Id of the Data Layer subscription (visible under `datalayer/subscriptions`). |
| `publishIntervalMillis` | How often the Data Layer publishes changes. |
| `samplingIntervalMicros` | Sampling rate of the source nodes. |
| `deadbandValue` | Absolute deadband filter; `0` publishes every change. |
| `writeDebounceMillis` | Batching window before the bulk write is issued. |
| `mappings[].enabled` | Keep a pair in the file but exclude it from the run. |

Pairs are rejected (with a log line) when an address is empty, when source equals
destination, or when two sources target the same destination.

Destination nodes must exist and be writable, and the value type must match — the
mapper forwards the source variant unchanged. Per-node failures show up as
`Write failed for '<address>': <result>` and don't stop the rest of the batch.

## Build and install

```bash
# amd64
./build-snap-amd64.sh

# arm64
./build-snap-arm64.sh
```

Place the project next to the other `samples-net` projects so the relative paths in
`nuget.config` (`../nuget`) and the build scripts (`../../scripts/...`) resolve.

Then install the snap on the ctrlX CORE via **Apps**, and connect the interfaces if
your device doesn't do it automatically:

```bash
snap connections sdk-net-datalayer-mapper
```

You need both `datalayer` (Data Layer access) and `active-solution` (app data storage).

## Editing the mappings on the device

1. **Settings → Manage app data** shows the `sdk-net-datalayer-mapper` directory.
2. Edit `mappings.json` over WebDAV: `https://<device>/solutions/webdav/appdata/sdk-net-datalayer-mapper/mappings.json`
   (WinSCP with protocol WebDAV works too; you need *manage configuration* rights).
3. Load the configuration from **Manage app data** — the app's `load` command fires,
   the file is re-read and the subscription is rebuilt in place. No restart needed.

## Console output

```
Running inside snap: True
Loaded configuration from '/var/snap/sdk-net-datalayer-mapper/common/solutions/activeConfiguration/sdk-net-datalayer-mapper/mappings.json' (3 mapping(s)).
ctrlX Data Layer system started.
ctrlX Data Layer client created.
Mapping: framework/metrics/system/cpu-utilisation-percent -> plc/app/Application/sym/PLC_PRG/cpuLoad
Mapping: framework/metrics/system/memused-percent -> plc/app/Application/sym/PLC_PRG/memUsed
Subscription 'net-node-mapper' active: 2 source(s), 2 destination(s), publish interval 1000 ms.
Listening to HTTP: http://localhost:5556/sdk-net-datalayer-mapper/api/v1/load/, http://localhost:5556/sdk-net-datalayer-mapper/api/v1/save/
Waiting for process exit event 'SIGTERM' ...
```

Watch it live with:

```bash
sudo snap logs sdk-net-datalayer-mapper.app -f
```

## Files

```
Program.cs          Startup, connection watchdog, shutdown
MapperConfig.cs     Config model, validation, app data load/save
NodeMapper.cs       Subscription, route table, bulk write flush loop
AppDataService.cs   HTTP endpoint for the ctrlX save/load workflow
configs/            Example mappings.json + package manifest
snap/snapcraft.yaml Packaging, datalayer + active-solution plugs
```
