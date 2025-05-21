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
using RestSharp;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Data.Market;
using System.Collections.Concurrent;
using QuantConnect.Lean.DataSource.Polygon.Rest;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Schedules and handles the processing of open interest data for options.
    /// It retrieves open interest data from the Polygon.io API and updates the corresponding <see cref="Tick"/> objects.
    /// </summary>
    public class PolygonOpenInterestProcessorManager : IDisposable
    {
        /// <summary>
        /// Timer used to schedule the execution of the <see cref="ProcessOpenInterest"/> method.
        /// </summary>
        private Timer? _openInterestScheduler;

        /// <summary>
        /// Gets the time zone for New York City, USA. This is a daylight savings time zone.
        /// </summary>
        private static readonly DateTimeZone _nyTimeZone = TimeZones.NewYork;

        /// <summary>
        /// The time provider instance.
        /// </summary>
        private readonly ITimeProvider _timeProvider;

        /// <summary>
        /// The <see cref="PolygonRestApiClient"/> REST API client instance.
        /// </summary>
        private readonly PolygonRestApiClient _polygonRestApiClient;

        /// <summary>
        /// Provides the mapping between Lean symbols and Polygon.io symbols.
        /// </summary>
        private readonly PolygonSymbolMapper _symbolMapper;

        /// <summary>
        /// Aggregates Polygon.io trade bars into same or higher resolution bars
        /// </summary>
        private readonly PolygonAggregationManager _dataAggregator;

        /// <summary>
        /// A delegate that retrieves the tick time for a given symbol and UTC timestamp.
        /// </summary>
        /// <param name="symbol">The financial instrument or symbol for which the tick time is being retrieved.</param>
        /// <param name="utcTime">The UTC time for which the tick time is being calculated.</param>
        /// <returns>
        /// The tick time as a <see cref="DateTime"/> for the given <paramref name="symbol"/> and <paramref name="utcTime"/>.
        /// </returns>
        private readonly Func<Symbol, DateTime, DateTime> _getTickTime;

        /// <summary>
        /// Stores the last request time for open interest per symbol.
        /// </summary>
        private readonly ConcurrentDictionary<Symbol, (DateTime lastDateTimeUpdate, decimal openInterest)> _lastOpenInterestRequestTimeBySymbol = new();

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonOpenInterestProcessorManager"/> class.
        /// </summary>
        /// <param name="timeProvider"></param>
        /// <param name="polygonRestApiClient"></param>
        /// <param name="symbolMapper"></param>
        /// <param name="dataAggregator"></param>
        /// <param name="getTickTime"></param>
        public PolygonOpenInterestProcessorManager(ITimeProvider timeProvider, PolygonRestApiClient polygonRestApiClient, PolygonSymbolMapper symbolMapper,
            PolygonAggregationManager dataAggregator, Func<Symbol, DateTime, DateTime> getTickTime)
        {
            _getTickTime = getTickTime;
            _timeProvider = timeProvider;
            _symbolMapper = symbolMapper;
            _dataAggregator = dataAggregator;
            _polygonRestApiClient = polygonRestApiClient;
        }

        /// <summary>
        /// Adds the given symbols to the tracking dictionary and resets their last request time.
        /// </summary>
        /// <param name="symbols">The symbols to add.</param>
        public void AddSymbols(IEnumerable<Symbol> symbols)
        {
            foreach (var symbol in symbols)
            {
                _lastOpenInterestRequestTimeBySymbol.TryAdd(symbol, default);
            }
            ScheduleNextRun(useScheduledDelay: false, newSubscription: true);
        }

        /// <summary>
        /// Removes the given symbols from the tracking dictionary.
        /// </summary>
        /// <param name="symbols">The symbols to remove.</param>
        public void RemoveSymbols(IEnumerable<Symbol> symbols)
        {
            foreach (var symbol in symbols)
            {
                _lastOpenInterestRequestTimeBySymbol.TryRemove(symbol, out _);
            }
        }

        /// <summary>
        /// Retrieves the latest open interest data for the specified symbol and creates a corresponding <see cref="Tick"/>.
        /// </summary>
        /// <param name="symbol">The symbol for which to generate the open interest tick.</param>
        /// <param name="tickTime">The timestamp to assign to the generated <see cref="Tick"/>.</param>
        /// <returns>
        /// A <see cref="Tick"/> containing the open interest data if available; otherwise, <c>null</c>.
        /// </returns>
        public Tick? GetOpenInterestTick(Symbol symbol, DateTime tickTime)
        {
            if (_lastOpenInterestRequestTimeBySymbol.TryGetValue(symbol, out var value))
            {
                return new Tick(tickTime, symbol, value.openInterest);
            }
            return null;
        }

        /// <summary>
        /// Schedules the next execution of the <see cref="ProcessOpenInterest"/> method
        /// based on whether the scheduler should apply the planned delay.
        /// </summary>
        /// <param name="useScheduledDelay">
        /// Indicates whether the full scheduled delay should be used before the next run.
        /// </param>
        /// <param name="newSubscription">
        /// Indicates whether to use a small delay to fetch open interest data for new subscriptions as soon as possible.
        /// </param>
        private void ScheduleNextRun(bool useScheduledDelay = false, bool newSubscription = false)
        {
            var delay = default(TimeSpan);
            if (useScheduledDelay)
            {
                var nowNewYork = _timeProvider.GetUtcNow().ConvertFromUtc(_nyTimeZone);
                var nextRunTimeNewYork = GetNextRunTime(nowNewYork);
                delay = nextRunTimeNewYork - nowNewYork;
            }
            else
            {
                delay = newSubscription ? TimeSpan.FromSeconds(5) : TimeSpan.FromMinutes(1);
            }

            if (_openInterestScheduler != null)
            {
                _openInterestScheduler.Change(delay, Timeout.InfiniteTimeSpan);
            }
            else
            {
                _openInterestScheduler = new Timer(RunProcessOpenInterest, null, delay, Timeout.InfiniteTimeSpan);
            }
        }

        /// <summary>
        /// Runs the <see cref="ProcessOpenInterest"/> method and reschedules the next execution.
        /// </summary>
        private void RunProcessOpenInterest(object? _)
        {
            var useScheduledDelay = false;

            var subscribedSymbol = new List<Symbol>();
            var nyNow = _timeProvider.GetUtcNow().ConvertFromUtc(_nyTimeZone);

            // Between 8:00 and 8:01 AM NY time: include all symbols
            if (nyNow >= nyNow.Date.AddHours(8) && nyNow < nyNow.Date.AddHours(8).AddMinutes(1))
            {
                subscribedSymbol.AddRange(_lastOpenInterestRequestTimeBySymbol.Keys);
            }
            else
            {
                // Outside 8:00–8:01 AM: include only symbols not requested today
                foreach (var (symbol, lastOpenInterestRequestTime) in _lastOpenInterestRequestTimeBySymbol)
                {
                    // Add symbols never requested or not requested today
                    if (nyNow.Date != lastOpenInterestRequestTime.lastDateTimeUpdate)
                    {
                        subscribedSymbol.Add(symbol);
                    }
                }
            }

            try
            {
                if (subscribedSymbol.Count != 0)
                {
                    ProcessOpenInterest(subscribedSymbol);
                    useScheduledDelay = true;
                }
            }
            catch (Exception ex)
            {
                Log.Error($"{nameof(PolygonOpenInterestProcessorManager)}.{nameof(RunProcessOpenInterest)}: {ex.Message}");
            }
            finally
            {
                ScheduleNextRun(useScheduledDelay);
            }
        }

        private void ProcessOpenInterest(IReadOnlyCollection<Symbol> subscribedSymbols)
        {
            var subscribedBrokerageSymbols = subscribedSymbols.Select(_symbolMapper.GetBrokerageSymbol);

            var restRequest = new RestRequest($"v3/snapshot?ticker.any_of={string.Join(',', subscribedBrokerageSymbols)}", Method.GET);
            restRequest.AddQueryParameter("limit", "250");

            var nowUtc = DateTime.UtcNow;
            foreach (var universalSnapshot in _polygonRestApiClient.DownloadAndParseData<UniversalSnapshotResponse>(restRequest).SelectMany(response => response.Results))
            {
                var leanSymbol = _symbolMapper.GetLeanSymbol(universalSnapshot.Ticker!);
                var time = _getTickTime(leanSymbol, nowUtc);
                _lastOpenInterestRequestTimeBySymbol[leanSymbol] = (time, universalSnapshot.OpenInterest);
                var openInterestTick = new Tick(time, leanSymbol, universalSnapshot.OpenInterest);
                lock (_dataAggregator)
                {
                    _dataAggregator.Update(openInterestTick);
                }
            }
        }

        /// <summary>
        /// Determines the next scheduled run time, either 8:00 AM today or 8:00 AM tomorrow in New York time,
        /// depending on the current time.
        /// </summary>
        /// <param name="currentTimeNewYork">The current local time in the New York time zone.</param>
        /// <returns>
        /// A <see cref="DateTime"/> representing the next run time at 8:00 AM, either today or the next day.
        /// </returns>
        private static DateTime GetNextRunTime(DateTime currentTimeNewYork)
        {
            var today8AM = currentTimeNewYork.Date.AddHours(8);

            if (currentTimeNewYork < today8AM)
            {
                // Schedule for today at 8:00 AM
                return today8AM;
            }
            else
            {
                // Schedule for tomorrow at 8:00 AM
                return today8AM.AddDays(1);
            }
        }

        /// <summary>
        /// Disposes the resources used by the <see cref="OpenInterestProcessor"/>.
        /// </summary>
        public void Dispose()
        {
            _openInterestScheduler?.Dispose();
        }
    }
}
