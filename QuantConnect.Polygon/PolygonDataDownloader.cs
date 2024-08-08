/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using NodaTime;
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Configuration;
using System.Collections.Concurrent;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Data downloader class for pulling data from Polygon.io
    /// </summary>
    public class PolygonDataDownloader : IDataDownloader, IDisposable
    {
        /// <inheritdoc cref="PolygonDataProvider"/>
        private readonly PolygonDataProvider _historyProvider;

        /// <inheritdoc cref="MarketHoursDatabase" />
        private readonly MarketHoursDatabase _marketHoursDatabase;

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonDataDownloader"/>
        /// </summary>
        /// <param name="apiKey">The Polygon.io API key</param>
        /// <param name="subscriptionPlan">Polygon subscription plan</param>
        public PolygonDataDownloader(string apiKey)
        {
            _historyProvider = new PolygonDataProvider(apiKey, false);
            _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonDataDownloader"/>
        /// getting the Polygon.io API key from the configuration
        /// </summary>
        public PolygonDataDownloader()
            : this(Config.Get("polygon-api-key"))
        {
        }

        /// <summary>
        /// Get historical data enumerable for a single symbol, type and resolution given this start and end time (in UTC).
        /// </summary>
        /// <param name="parameters">Parameters for the historical data request</param>
        /// <returns>Enumerable of base data for this symbol</returns>
        public IEnumerable<BaseData>? Get(DataDownloaderGetParameters parameters)
        {
            var symbol = parameters.Symbol;
            var resolution = parameters.Resolution;
            var startUtc = parameters.StartUtc;
            var endUtc = parameters.EndUtc;
            var tickType = parameters.TickType;

            var dataType = LeanData.GetDataType(resolution, tickType);
            var exchangeHours = _marketHoursDatabase.GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);
            var dataTimeZone = _marketHoursDatabase.GetDataTimeZone(symbol.ID.Market, symbol, symbol.SecurityType);

            if (symbol.IsCanonical())
            {
                return GetCanonicalOptionHistory(symbol, startUtc, endUtc, dataType, resolution, exchangeHours, dataTimeZone, tickType);
            }
            else
            {
                var historyRequest = new HistoryRequest(startUtc, endUtc, dataType, symbol, resolution, exchangeHours, dataTimeZone, resolution,
                    true, false, DataNormalizationMode.Raw, tickType);

                var historyData = _historyProvider.GetHistory(historyRequest);

                if (historyData == null)
                {
                    return null;
                }

                return historyData;
            }
        }

        private IEnumerable<BaseData>? GetCanonicalOptionHistory(Symbol symbol, DateTime startUtc, DateTime endUtc, Type dataType,
            Resolution resolution, SecurityExchangeHours exchangeHours, DateTimeZone dataTimeZone, TickType tickType)
        {
            var blockingOptionCollection = new BlockingCollection<BaseData>();
            var symbols = GetOptions(symbol, startUtc, endUtc);

            // Symbol can have a lot of Option parameters
            Task.Run(() => Parallel.ForEach(symbols, targetSymbol =>
            {
                var historyRequest = new HistoryRequest(startUtc, endUtc, dataType, targetSymbol, resolution, exchangeHours, dataTimeZone,
                    resolution, true, false, DataNormalizationMode.Raw, tickType);

                var history = _historyProvider.GetHistory(historyRequest);

                // If history is null, it indicates an incorrect or missing request for historical data,
                // so we skip processing for this symbol and move to the next one.
                if (history == null)
                {
                    return;
                }

                foreach (var data in history)
                {
                    blockingOptionCollection.Add(data);
                }
            })).ContinueWith(task =>
            {
                blockingOptionCollection.CompleteAdding();
                if (task.IsFaulted && task.Exception != null)
                {
                    var aggregateException = task.Exception.Flatten();
                    var errorMessages = string.Join("; ", aggregateException.InnerExceptions.Select(e => e.Message));
                    Log.Error($"{nameof(PolygonDataDownloader)}.{nameof(GetCanonicalOptionHistory)}: Task failed with error(s): {errorMessages}");
                }
            });

            var options = blockingOptionCollection.GetConsumingEnumerable();

            // Validate if the collection contains at least one successful response from history.
            if (!options.Any())
            {
                return null;
            }

            return options;
        }

        protected virtual IEnumerable<Symbol> GetOptions(Symbol symbol, DateTime startUtc, DateTime endUtc)
        {
            foreach (var date in Time.EachDay(startUtc.Date, endUtc.Date))
            {
                foreach (var option in _historyProvider.GetOptionChain(symbol, date))
                {
                    yield return option;
                }
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            _historyProvider.DisposeSafely();
        }
    }
}
