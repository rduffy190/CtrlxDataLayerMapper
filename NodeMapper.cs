/*
 * SPDX-License-Identifier: MIT
 */

using comm.datalayer;
using Datalayer;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Samples.Datalayer.Mapper
{
    /// <summary>
    /// Subscribes to all configured source nodes and forwards every data change
    /// to the mapped destination nodes using a single bulk write.
    ///
    /// Flow:
    ///   1. Iterate the configured pairs -> route table (source -> destinations).
    ///   2. Create one subscription and SubscribeMulti() to all distinct sources.
    ///   3. DataChanged copies the value into the active buffer and signals the
    ///      flush loop (no Data Layer call in the callback context).
    ///   4. The flush loop swaps the buffers and bulk writes the retired one.
    ///
    /// Concurrency:
    ///   - The route table is immutable once published and swapped by reference,
    ///     so the callback reads it without any lock.
    ///   - Two pending buffers alternate. The callback holds the swap lock only
    ///     for a single dictionary insert; the flush loop holds it only for a
    ///     reference swap. Neither ever holds it across the bulk write.
    /// </summary>
    internal sealed class NodeMapper : IAsyncDisposable
    {
        private readonly IClient _client;

        // Route table: source address -> destination addresses.
        // Never mutated after publication - replaced wholesale on Apply().
        private Dictionary<string, string[]> _routes =
            new Dictionary<string, string[]>(StringComparer.Ordinal);

        // Double buffer of pending writes, keyed by destination address.
        // The callback fills _activeBuffer; the flush loop swaps it out and owns
        // the retired buffer exclusively until it has been written and cleared.
        private Dictionary<string, IVariant> _activeBuffer =
            new Dictionary<string, IVariant>(StringComparer.Ordinal);
        private Dictionary<string, IVariant> _spareBuffer =
            new Dictionary<string, IVariant>(StringComparer.Ordinal);
        private readonly object _swapLock = new object();

        private readonly SemaphoreSlim _flushSignal = new SemaphoreSlim(0, 1);
        private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();

        private int _signaled;
        private Task? _flushTask;
        private ISubscription? _subscription;
        private MapperConfig _config = new MapperConfig();

        public NodeMapper(IClient client)
        {
            if (client == null)
            {
                throw new ArgumentNullException(nameof(client));
            }

            _client = client;
        }

        /// <summary>
        /// Applies a configuration. Safe to call again at runtime (app data "load"
        /// phase); the old subscription is torn down and a new one is created,
        /// because the subscription properties themselves may have changed.
        /// </summary>
        public bool Apply(MapperConfig config)
        {
            if (config == null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            List<NodeMapping> mappings = config.GetValidMappings();

            if (mappings.Count == 0)
            {
                Console.WriteLine("No usable mappings configured.");
                DisposeSubscription();
                PublishRoutes(new Dictionary<string, string[]>(StringComparer.Ordinal));
                ClearPending();
                _config = config;
                return false;
            }

            // 1. Iterate over all pairs and build the source -> destinations map.
            Dictionary<string, List<string>> collected =
                new Dictionary<string, List<string>>(StringComparer.Ordinal);

            for (int i = 0; i < mappings.Count; i++)
            {
                NodeMapping mapping = mappings[i];
                List<string>? destinations;

                if (!collected.TryGetValue(mapping.Source, out destinations))
                {
                    destinations = new List<string>();
                    collected.Add(mapping.Source, destinations);
                }

                destinations.Add(mapping.Destination);
            }

            // Freeze into arrays. The finished table is published as a single
            // reference assignment; the callback picks it up on its next read.
            Dictionary<string, string[]> published =
                new Dictionary<string, string[]>(collected.Count, StringComparer.Ordinal);

            foreach (KeyValuePair<string, List<string>> entry in collected)
            {
                published.Add(entry.Key, entry.Value.ToArray());
            }

            PublishRoutes(published);

            _config = config;

            DisposeSubscription();
            ClearPending();

            // 2. One subscription for all sources.
            SubscriptionPropertiesBuilder propertiesBuilder =
                new SubscriptionPropertiesBuilder(config.SubscriptionId);

            propertiesBuilder.SetPublishIntervalMillis(config.PublishIntervalMillis);
            propertiesBuilder.SetKeepAliveIntervalMillis(config.KeepaliveIntervalMillis);
            propertiesBuilder.SetErrorIntervalMillis(config.ErrorIntervalMillis);
            propertiesBuilder.SetSamplingIntervalMicros(config.SamplingIntervalMicros);

            if (config.DeadbandValue > 0.0f)
            {
                propertiesBuilder.SetDataChangeFilter(config.DeadbandValue);
            }

            Variant properties = propertiesBuilder.Build();

            DLR_RESULT createResult;
            ISubscription subscription;

            (createResult, subscription) = _client.CreateSubscription(properties, null);

            if (createResult.IsBad())
            {
                Console.WriteLine(
                    "Failed to create subscription '" + config.SubscriptionId + "': " + createResult);
                return false;
            }

            subscription.DataChanged += OnDataChanged;
            _subscription = subscription;

            string[] sources = new string[published.Count];
            published.Keys.CopyTo(sources, 0);

            DLR_RESULT subscribeResult = subscription.SubscribeMulti(sources);

            if (subscribeResult.IsBad())
            {
                // A single unreachable address fails the whole multi-subscribe, so
                // fall back to subscribing one by one and keep what works.
                Console.WriteLine(
                    "SubscribeMulti failed: " + subscribeResult + " -> subscribing individually.");

                int subscribed = 0;

                for (int i = 0; i < sources.Length; i++)
                {
                    DLR_RESULT singleResult = subscription.Subscribe(sources[i]);

                    if (singleResult.IsBad())
                    {
                        Console.WriteLine(
                            "Failed to subscribe '" + sources[i] + "': " + singleResult);
                        continue;
                    }

                    subscribed = subscribed + 1;
                }

                if (subscribed == 0)
                {
                    Console.WriteLine("No source node could be subscribed.");
                    DisposeSubscription();
                    return false;
                }
            }

            for (int i = 0; i < mappings.Count; i++)
            {
                Console.WriteLine("Mapping: " + mappings[i]);
            }

            Console.WriteLine(
                "Subscription '" + config.SubscriptionId + "' active: " +
                sources.Length + " source(s), " +
                mappings.Count + " destination(s), publish interval " +
                config.PublishIntervalMillis + " ms.");

            // 3. Start the flush loop once. The method yields at its first await,
            //    so calling it directly is enough - no Task.Run needed.
            if (_flushTask == null)
            {
                _flushTask = FlushLoopAsync(_cancellation.Token);
            }

            return true;
        }

        private void PublishRoutes(Dictionary<string, string[]> routes)
        {
            Volatile.Write(ref _routes, routes);
        }

        /// <summary>
        /// Data change callback.
        ///
        /// IMPORTANT: synchronous Data Layer calls are not allowed in this context
        /// (WOULD_BLOCK). We only copy the value into the active buffer; the flush
        /// loop does the writing.
        /// </summary>
        private void OnDataChanged(ISubscription subscription, IDataChangedEventArgs args)
        {
            if (args.Result.IsBad())
            {
                Console.WriteLine("Data change notification reported: " + args.Result);
                return;
            }

            NotifyInfo notifyInfo = NotifyInfo.GetRootAsNotifyInfo(args.Item.Info.ToFlatbuffers());
            string source = notifyInfo.Node;

            if (string.IsNullOrEmpty(source))
            {
                return;
            }

            // Lock-free read: the table is immutable, only the reference changes.
            Dictionary<string, string[]> routes = Volatile.Read(ref _routes);
            string[]? destinations;

            if (!routes.TryGetValue(source, out destinations))
            {
                return;
            }

            for (int i = 0; i < destinations.Length; i++)
            {
                string destination = destinations[i];

                // The notification value is owned by the subscription and is not
                // valid after this callback returns, so take a copy per
                // destination. Allocated outside the lock.
                Variant value = new Variant(args.Item.Value);
                IVariant? superseded;

                lock (_swapLock)
                {
                    // Single hash insert. The flush loop only ever holds this lock
                    // for a reference swap, so there is nothing long to wait on.
                    _activeBuffer.Remove(destination, out superseded);
                    _activeBuffer[destination] = value;
                }

                // Disposed outside the lock: once removed from the buffer, nothing
                // else can reach it.
                if (superseded != null)
                {
                    superseded.Dispose();
                }
            }

            Signal();
        }

        private void Signal()
        {
            // Release at most once until the flush loop has picked the signal up.
            if (Interlocked.Exchange(ref _signaled, 1) == 0)
            {
                _flushSignal.Release();
            }
        }

        private async Task FlushLoopAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await _flushSignal.WaitAsync(cancellationToken);
                    Interlocked.Exchange(ref _signaled, 0);

                    // Let the rest of the publish batch arrive so it goes out in
                    // one write.
                    int debounce = _config.WriteDebounceMillis;

                    if (debounce > 0)
                    {
                        await Task.Delay(debounce, cancellationToken);
                    }

                    await FlushAsync();
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception exc)
                {
                    // Never let the loop die on a single bad batch.
                    Console.WriteLine("Bulk write cycle failed! " + exc.Message);
                }
            }
        }

        /// <summary>
        /// 4. Swaps the buffers and writes the retired one in a single bulk write.
        ///
        /// Only ever called from the flush loop, so the retired buffer is
        /// guaranteed to be emptied and back in place as the spare before the
        /// next swap.
        /// </summary>
        private async Task FlushAsync()
        {
            Dictionary<string, IVariant> batch;

            lock (_swapLock)
            {
                if (_activeBuffer.Count == 0)
                {
                    return;
                }

                // The whole critical section: two reference assignments.
                // The callback resumes filling the (empty) spare immediately.
                batch = _activeBuffer;
                _activeBuffer = _spareBuffer;
                _spareBuffer = batch;
            }

            // From here on the batch is ours alone - no lock needed.
            string[] addresses = new string[batch.Count];
            IVariant[] values = new IVariant[batch.Count];

            int index = 0;

            foreach (KeyValuePair<string, IVariant> entry in batch)
            {
                addresses[index] = entry.Key;
                values[index] = entry.Value;
                index = index + 1;
            }

            try
            {
                // Bulk write: one round trip for the whole batch.
                IClientAsyncBulkResult bulkResult = await _client.BulkWriteAsync(addresses, values);

                if (bulkResult.Result.IsBad())
                {
                    Console.WriteLine(
                        "BulkWrite of " + addresses.Length + " node(s) failed: " + bulkResult.Result);
                    return;
                }

                IBulkItem[] items = bulkResult.Items;

                if (items != null)
                {
                    for (int i = 0; i < items.Length; i++)
                    {
                        if (items[i].Result.IsBad())
                        {
                            Console.WriteLine(
                                "Write failed for '" + items[i].Address + "': " + items[i].Result);
                        }
                    }
                }
            }
            finally
            {
                for (int i = 0; i < values.Length; i++)
                {
                    values[i].Dispose();
                }

                // Hand the buffer back empty, ready to be swapped in again.
                batch.Clear();
            }
        }

        private void ClearPending()
        {
            lock (_swapLock)
            {
                foreach (IVariant value in _activeBuffer.Values)
                {
                    value.Dispose();
                }

                foreach (IVariant value in _spareBuffer.Values)
                {
                    value.Dispose();
                }

                _activeBuffer.Clear();
                _spareBuffer.Clear();
            }
        }

        private void DisposeSubscription()
        {
            if (_subscription == null)
            {
                return;
            }

            _subscription.DataChanged -= OnDataChanged;
            _subscription.UnsubscribeAll();
            _subscription.Dispose();
            _subscription = null;
        }

        public async ValueTask DisposeAsync()
        {
            DisposeSubscription();

            _cancellation.Cancel();

            if (_flushTask != null)
            {
                try
                {
                    await _flushTask;
                }
                catch (OperationCanceledException)
                {
                    // Expected on shutdown.
                }

                _flushTask = null;
            }

            ClearPending();

            _cancellation.Dispose();
            _flushSignal.Dispose();
        }
    }
}
