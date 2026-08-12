/*
 * SPDX-License-Identifier: MIT
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Samples.Datalayer.Mapper
{
    /// <summary>
    /// A single source -> destination node pair.
    /// </summary>
    public sealed record NodeMapping
    {
        /// <summary>The ctrlX Data Layer address to subscribe to.</summary>
        public string Source { get; init; } = string.Empty;

        /// <summary>The ctrlX Data Layer address the source value is written to.</summary>
        public string Destination { get; init; } = string.Empty;

        /// <summary>Set to false to keep a pair in the file but exclude it from the run.</summary>
        public bool Enabled { get; init; } = true;

        public override string ToString() => $"{Source} -> {Destination}";
    }

    /// <summary>
    /// The application configuration.
    ///
    /// NOTE: The ctrlX persistence guideline requires a JSON *object* as root element
    /// (not an array), so the list of pairs lives under the "mappings" member.
    /// </summary>
    public sealed record MapperConfig
    {
        /// <summary>Id of the ctrlX Data Layer subscription.</summary>
        public string SubscriptionId { get; init; } = "net-node-mapper";

        /// <summary>Publish interval of the subscription in milliseconds.</summary>
        public uint PublishIntervalMillis { get; init; } = 1000;

        /// <summary>Keep alive interval of the subscription in milliseconds.</summary>
        public uint KeepaliveIntervalMillis { get; init; } = 10000;

        /// <summary>Error interval of the subscription in milliseconds.</summary>
        public uint ErrorIntervalMillis { get; init; } = 10000;

        /// <summary>Sampling interval of the subscription in microseconds.</summary>
        public ulong SamplingIntervalMicros { get; init; } = 1000000;

        /// <summary>Absolute deadband. 0 publishes every change.</summary>
        public float DeadbandValue { get; init; } = 0.0f;

        /// <summary>
        /// Grace period after the first pending change before the bulk write is issued.
        /// Lets the values of one publish batch coalesce into a single bulk write.
        /// </summary>
        public int WriteDebounceMillis { get; init; } = 50;

        /// <summary>The source/destination pairs.</summary>
        public IReadOnlyList<NodeMapping> Mappings { get; init; } = Array.Empty<NodeMapping>();

        /// <summary>
        /// Returns the usable pairs and reports the rejected ones.
        /// </summary>
        public IReadOnlyList<NodeMapping> GetValidMappings()
        {
            var valid = new List<NodeMapping>();
            var seenDestinations = new HashSet<string>(StringComparer.Ordinal);

            foreach (var mapping in Mappings)
            {
                if (!mapping.Enabled)
                {
                    continue;
                }

                if (string.IsNullOrWhiteSpace(mapping.Source) || string.IsNullOrWhiteSpace(mapping.Destination))
                {
                    Console.WriteLine($"Skipping mapping with empty address: '{mapping}'.");
                    continue;
                }

                if (string.Equals(mapping.Source, mapping.Destination, StringComparison.Ordinal))
                {
                    Console.WriteLine($"Skipping self-referencing mapping: '{mapping}'.");
                    continue;
                }

                // Two sources feeding one destination means the last publish always wins.
                if (!seenDestinations.Add(mapping.Destination))
                {
                    Console.WriteLine($"Skipping mapping with duplicate destination: '{mapping}'.");
                    continue;
                }

                valid.Add(mapping);
            }

            return valid;
        }
    }

    [JsonSourceGenerationOptions(
        WriteIndented = true,
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
    [JsonSerializable(typeof(MapperConfig))]
    internal partial class MapperConfigSerializerContext : JsonSerializerContext { }

    /// <summary>
    /// Reads and writes the configuration in the ctrlX app data storage.
    ///
    /// Snapped (ctrlX OS): $SNAP_COMMON/solutions/activeConfiguration/{StorageFolderName}
    /// Windows/dev:        %USERPROFILE%\Documents\My ctrlX\{StorageFolderName}
    /// </summary>
    internal static class AppDataStorage
    {
        /// <summary>MUST match the app directory declared in *.package-manifest.json.</summary>
        public const string StorageFolderName = "sdk-net-datalayer-mapper";

        private const string StorageFileName = "mappings.json";

        public static bool IsSnapped => !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("SNAP"));

        private static string SnapCommonLocation => Environment.GetEnvironmentVariable("SNAP_COMMON") ?? string.Empty;

        private static string BaseStorageLocation => IsSnapped
            ? Path.Combine(SnapCommonLocation, "solutions", "activeConfiguration")
            : Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "My ctrlX");

        public static string StorageLocation => Path.Combine(BaseStorageLocation, StorageFolderName);

        public static string StorageFile => Path.Combine(StorageLocation, StorageFileName);

        /// <summary>
        /// Loads the configuration. Writes a template on first start so the user has
        /// something to edit in the "Manage app data" view.
        /// </summary>
        public static MapperConfig? Load()
        {
            var path = StorageFile;

            if (!File.Exists(path))
            {
                Console.WriteLine($"No configuration found at '{path}' -> creating a template.");
                var template = CreateTemplate();
                return Save(template) ? template : null;
            }

            try
            {
                var json = File.ReadAllText(path, Encoding.UTF8);
                var config = JsonSerializer.Deserialize(json, MapperConfigSerializerContext.Default.MapperConfig);

                if (config is null)
                {
                    Console.WriteLine($"Configuration '{path}' is empty.");
                    return null;
                }

                Console.WriteLine($"Loaded configuration from '{path}' ({config.Mappings.Count} mapping(s)).");
                return config;
            }
            catch (Exception exc) when (exc is IOException || exc is JsonException || exc is UnauthorizedAccessException)
            {
                Console.WriteLine($"Loading configuration from '{path}' failed! {exc.Message}");
                return null;
            }
        }

        /// <summary>
        /// Persists the configuration into the active configuration (appdata directory).
        /// </summary>
        public static bool Save(MapperConfig config)
        {
            if (!EnsureStorageLocation())
            {
                return false;
            }

            var path = StorageFile;

            try
            {
                var json = JsonSerializer.Serialize(config, MapperConfigSerializerContext.Default.MapperConfig);
                File.WriteAllText(path, json, Encoding.UTF8);
                Console.WriteLine($"Saved configuration to '{path}'.");
                return true;
            }
            catch (Exception exc) when (exc is IOException || exc is UnauthorizedAccessException)
            {
                Console.WriteLine($"Saving configuration to '{path}' failed! {exc.Message}");
                return false;
            }
        }

        private static bool EnsureStorageLocation()
        {
            var path = StorageLocation;

            if (Directory.Exists(path))
            {
                return true;
            }

            try
            {
                Directory.CreateDirectory(path);
                Console.WriteLine($"Created storage location '{path}'.");
                return true;
            }
            catch (Exception exc) when (exc is IOException || exc is UnauthorizedAccessException)
            {
                Console.WriteLine($"Creating storage location '{path}' failed! {exc.Message}");
                return false;
            }
        }

        /// <summary>
        /// A template using nodes that exist on every ctrlX CORE.
        /// </summary>
        private static MapperConfig CreateTemplate() => new()
        {
            Mappings =
            [
                new NodeMapping
                {
                    Source = "framework/metrics/system/cpu-utilisation-percent",
                    Destination = "sdk/net/mapper/cpu-utilisation-percent"
                },
                new NodeMapping
                {
                    Source = "framework/metrics/system/memused-percent",
                    Destination = "sdk/net/mapper/memused-percent"
                }
            ]
        };
    }
}
