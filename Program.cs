/*
 * SPDX-License-Identifier: MIT
 */

using Datalayer;
using Samples.Datalayer.Mapper;
using System;
using System.Threading;
using System.Threading.Tasks;

// Create TaskCompletionSource to wait for process termination.
var tcs = new TaskCompletionSource();

// Handle process exit event (SIGTERM).
AppDomain.CurrentDomain.ProcessExit += (_, _) =>
{
    Console.WriteLine("Received 'SIGTERM' event.");
    tcs.TrySetResult();
};

Console.WriteLine($"Running inside snap: {AppDataStorage.IsSnapped}");

// ---------------------------------------------------------------------------
// 1. Load the mapping configuration from the ctrlX app data storage.
// ---------------------------------------------------------------------------
var loaded = AppDataStorage.Load();
if (loaded is null)
{
    // Exit and retry after the app restart-delay (see snapcraft.yaml).
    Console.WriteLine("Failed to load configuration -> exit.");
    return;
}

var config = loaded;

// ---------------------------------------------------------------------------
// 2. Start the ctrlX Data Layer system and connect a client.
// ---------------------------------------------------------------------------
using var system = new DatalayerSystem();

// startBroker: false, because a broker is already running on ctrlX CORE.
system.Start(startBroker: false);
Console.WriteLine("ctrlX Data Layer system started.");

// Inside a snap this resolves to 'ipc', outside to 'tcp'.
// Adapt ip/user/password to your environment when running remotely.
var remote = new Remote(ip: "192.168.1.1", sslPort: 443).ToString();

using var client = system.Factory.CreateClient(remote);
Console.WriteLine("ctrlX Data Layer client created.");

if (!client.IsConnected)
{
    Console.WriteLine("Client is not connected -> exit.");
    return;
}

// ---------------------------------------------------------------------------
// 3. Subscribe to all sources and map them onto the destinations (bulk write).
// ---------------------------------------------------------------------------
await using var mapper = new NodeMapper(client);

if (!mapper.Apply(config))
{
    Console.WriteLine("Failed to apply the mapping configuration -> exit.");
    return;
}

// ---------------------------------------------------------------------------
// 4. Participate in the ctrlX save/load workflow.
// ---------------------------------------------------------------------------
using var appDataService = new AppDataService(
    onLoad: () =>
    {
        var reloaded = AppDataStorage.Load();
        if (reloaded is null)
        {
            return false;
        }

        config = reloaded;
        Console.WriteLine("Re-applying mapping configuration ...");
        return mapper.Apply(reloaded);
    },
    onSave: () => AppDataStorage.Save(config));

if (!appDataService.Start())
{
    Console.WriteLine("Failed to start the app data service -> exit.");
    return;
}

// ---------------------------------------------------------------------------
// 5. Watch the connection. On connection loss we exit and let snapd restart us.
// ---------------------------------------------------------------------------
using var watchdogCts = new CancellationTokenSource();

_ = Task.Run(async () =>
{
    while (!watchdogCts.IsCancellationRequested)
    {
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(5), watchdogCts.Token);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        if (client.IsConnected)
        {
            continue;
        }

        Console.WriteLine("Client connection lost -> restarting app.");
        tcs.TrySetResult();
        return;
    }
});

// Wait for process termination.
Console.WriteLine("Waiting for process exit event 'SIGTERM' ...");
await tcs.Task;

watchdogCts.Cancel();
Console.WriteLine("Graceful shutdown.");

// Stop the ctrlX Data Layer system.
system.Stop();
Console.WriteLine("ctrlX Data Layer system stopped.");
