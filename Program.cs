/*
 * SPDX-License-Identifier: MIT
 */

using Datalayer;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Samples.Datalayer.Mapper
{
    internal static class Program
    {
        private const int WatchdogIntervalSeconds = 5;

        // Set when the app should shut down, either by SIGTERM or by the watchdog.
        // A static field rather than a captured local, so the event handler below
        // is an ordinary method instead of a closure.
        private static readonly TaskCompletionSource ShutdownSignal = new TaskCompletionSource();

        private static async Task Main()
        {
            AppDomain.CurrentDomain.ProcessExit += OnProcessExit;

            Console.WriteLine("Running inside snap: " + AppDataStorage.IsSnapped);

            // -----------------------------------------------------------------
            // 1. Load the mapping configuration from the ctrlX app data storage.
            // -----------------------------------------------------------------
            MapperConfig? config = AppDataStorage.Load();
            if (config == null)
            {
                // Exit and retry after the app restart-delay (see snapcraft.yaml).
                Console.WriteLine("Failed to load configuration -> exit.");
                return;
            }

            // -----------------------------------------------------------------
            // 2. Start the ctrlX Data Layer system and connect a client.
            // -----------------------------------------------------------------
            using DatalayerSystem system = new DatalayerSystem();

            // startBroker: false, because a broker is already running on ctrlX CORE.
            system.Start(false);
            Console.WriteLine("ctrlX Data Layer system started.");

            // Inside a snap this resolves to 'ipc', outside to 'tcp'.
            // Adapt ip/user/password to your environment when running remotely.
            Remote remote = new Remote(ip: "192.168.1.1", sslPort: 443);

            using IClient client = system.Factory.CreateClient(remote.ToString());
            Console.WriteLine("ctrlX Data Layer client created.");

            if (!client.IsConnected)
            {
                Console.WriteLine("Client is not connected -> exit.");
                return;
            }

            // -----------------------------------------------------------------
            // 3. Subscribe to all sources and map them onto the destinations.
            // -----------------------------------------------------------------
            await using NodeMapper mapper = new NodeMapper(client);

            if (!mapper.Apply(config))
            {
                Console.WriteLine("Failed to apply the mapping configuration -> exit.");
                return;
            }

            // -----------------------------------------------------------------
            // 4. Participate in the ctrlX save/load workflow.
            // -----------------------------------------------------------------
            MapperAppDataHandler appDataHandler = new MapperAppDataHandler(mapper, config);

            using AppDataService appDataService = new AppDataService(appDataHandler);

            if (!appDataService.Start())
            {
                Console.WriteLine("Failed to start the app data service -> exit.");
                return;
            }

            // -----------------------------------------------------------------
            // 5. Watch the connection. On connection loss we exit and let snapd
            //    restart us.
            // -----------------------------------------------------------------
            using CancellationTokenSource watchdogCancellation = new CancellationTokenSource();

            // Runs concurrently: the method yields at its first await.
            Task watchdogTask = WatchConnectionAsync(
                client,
                ShutdownSignal,
                WatchdogIntervalSeconds,
                watchdogCancellation.Token);

            // Wait for process termination.
            Console.WriteLine("Waiting for process exit event 'SIGTERM' ...");
            await ShutdownSignal.Task;

            // Stop the watchdog and wait for it to actually leave its loop.
            watchdogCancellation.Cancel();
            await watchdogTask;

            Console.WriteLine("Graceful shutdown.");

            system.Stop();
            Console.WriteLine("ctrlX Data Layer system stopped.");
        }

        /// <summary>
        /// Polls the client connection until cancelled. Everything it needs is a
        /// parameter, so there is no hidden captured state.
        /// </summary>
        private static async Task WatchConnectionAsync(
            IClient client,
            TaskCompletionSource shutdownSignal,
            int intervalSeconds,
            CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Cancellation aborts the wait immediately instead of running
                    // the full interval out.
                    await Task.Delay(TimeSpan.FromSeconds(intervalSeconds), cancellationToken);
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
                shutdownSignal.TrySetResult();
                return;
            }
        }

        private static void OnProcessExit(object? sender, EventArgs args)
        {
            Console.WriteLine("Received 'SIGTERM' event.");
            ShutdownSignal.TrySetResult();
        }
    }
}
