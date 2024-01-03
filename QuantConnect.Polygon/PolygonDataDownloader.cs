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

using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Securities;
using QuantConnect.Util;
using System.Collections.Concurrent;

namespace QuantConnect.Polygon
{
    /// <summary>
    /// Data downloader class for pulling data from Polygon.io
    /// </summary>
    public class PolygonDataDownloader : IDataDownloader, IDisposable
    {
        private readonly PolygonDataQueueHandler _historyProvider;

        private readonly MarketHoursDatabase _marketHoursDatabase;

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonDataDownloader"/>
        /// </summary>
        /// <param name="apiKey">The Polygon.io API key</param>
        /// <param name="subscriptionPlan">Polygon subscription plan</param>
        public PolygonDataDownloader(string apiKey)
        {
            _historyProvider = new PolygonDataQueueHandler(apiKey, false);
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
        public IEnumerable<BaseData> Get(DataDownloaderGetParameters parameters)
        {
            var symbol = parameters.Symbol;
            var resolution = parameters.Resolution;
            var startUtc = parameters.StartUtc;
            var endUtc = parameters.EndUtc;
            var tickType = parameters.TickType;

            if (endUtc < startUtc)
            {
                yield break;
            }

            var dataType = LeanData.GetDataType(resolution, tickType);
            var exchangeHours = _marketHoursDatabase.GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);
            var dataTimeZone = _marketHoursDatabase.GetDataTimeZone(symbol.ID.Market, symbol, symbol.SecurityType);

            if (symbol.IsCanonical())
            {
                var symbols = new List<Symbol>();
                foreach (var date in Time.EachDay(startUtc.Date, endUtc.Date))
                {
                    symbols.AddRange(_historyProvider.GetOptionChain(symbol, date));
                }

                using var queue = new BlockingCollection<BaseData>();
                var symbolsProcessedCount = 0;
                Task.Run(() => Parallel.ForEach(symbols, targetSymbol =>
                {
                    var historyRequest = new HistoryRequest(startUtc, endUtc, dataType, symbol, resolution, exchangeHours, dataTimeZone, resolution,
                        true, false, DataNormalizationMode.Raw, tickType);
                    foreach (var data in _historyProvider.GetHistory(historyRequest))
                    {
                        queue.Add(data);
                    }

                    if (Interlocked.Increment(ref symbolsProcessedCount) == symbols.Count)
                    {
                        queue.CompleteAdding();
                    }
                }));

                foreach (var data in queue.GetConsumingEnumerable())
                {
                    yield return data;
                }
            }
            else
            {
                var historyRequest = new HistoryRequest(startUtc, endUtc, dataType, symbol, resolution, exchangeHours, dataTimeZone, resolution,
                    true, false, DataNormalizationMode.Raw, tickType);
                foreach (var data in _historyProvider.GetHistory(historyRequest))
                {
                    yield return data;
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
