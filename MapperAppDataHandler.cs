/*
 * SPDX-License-Identifier: MIT
 */

using System;

namespace Samples.Datalayer.Mapper
{
    /// <summary>
    /// Handles the ctrlX save/load workflow for the node mapper.
    ///
    /// Holds the mapper and the current configuration as ordinary fields, so the
    /// data the callbacks operate on is visible in the constructor signature.
    /// </summary>
    internal sealed class MapperAppDataHandler : IAppDataHandler
    {
        private readonly NodeMapper _mapper;

        // Replaced on every successful load, read by Save(). Both can happen on
        // the HTTP listener thread, so the reference is guarded.
        private readonly object _configLock = new object();
        private MapperConfig _config;

        public MapperAppDataHandler(NodeMapper mapper, MapperConfig config)
        {
            if (mapper == null)
            {
                throw new ArgumentNullException(nameof(mapper));
            }

            if (config == null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            _mapper = mapper;
            _config = config;
        }

        /// <summary>
        /// Re-reads the configuration and rebuilds the subscription in place.
        /// </summary>
        public bool Load()
        {
            MapperConfig? reloaded = AppDataStorage.Load();
            if (reloaded == null)
            {
                return false;
            }

            lock (_configLock)
            {
                _config = reloaded;
            }

            Console.WriteLine("Re-applying mapping configuration ...");
            return _mapper.Apply(reloaded);
        }

        /// <summary>
        /// Persists the configuration currently in use.
        /// </summary>
        public bool Save()
        {
            MapperConfig current;

            lock (_configLock)
            {
                current = _config;
            }

            return AppDataStorage.Save(current);
        }
    }
}
