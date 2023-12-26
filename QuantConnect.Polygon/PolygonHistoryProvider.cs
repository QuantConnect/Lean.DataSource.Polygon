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

using System.Net;
using Newtonsoft.Json.Linq;
using NodaTime;
using RestSharp;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Lean.Engine.HistoricalData;
using QuantConnect.Logging;
using QuantConnect.Configuration;
using QuantConnect.Util;
using QuantConnect.Data.Consolidators;

namespace QuantConnect.Polygon
{
    public partial class PolygonDataQueueHandler : SynchronizingHistoryProvider
    {
        private static string RestApiBaseUrl = Config.Get("polygon-api-url", "https://api.polygon.io");

        private readonly int ApiResponseLimit = Config.GetInt("polygon-aggregate-response-limit", 5000);

        private readonly RestClient _restClient;

        protected RateGate RestApiRateLimiter { get; set; } = new(300, TimeSpan.FromSeconds(1));

        private int _dataPointCount;

        /// <summary>
        /// Gets the total number of data points emitted by this history provider
        /// </summary>
        public override int DataPointCount => _dataPointCount;

        /// <summary>
        /// Initializes this history provider to work for the specified job
        /// </summary>
        /// <param name="parameters">The initialization parameters</param>
        public override void Initialize(HistoryProviderInitializeParameters parameters)
        {
        }

        /// <summary>
        /// Gets the history for the requested securities
        /// </summary>
        /// <param name="requests">The historical data requests</param>
        /// <param name="sliceTimeZone">The time zone used when time stamping the slice instances</param>
        /// <returns>An enumerable of the slices of data covering the span specified in each request</returns>
        public override IEnumerable<Slice> GetHistory(IEnumerable<HistoryRequest> requests, DateTimeZone sliceTimeZone)
        {
            var subscriptions = new List<Subscription>();
            foreach (var request in requests)
            {
                var history = GetHistory(request);
                var subscription = CreateSubscription(request, history);
                subscriptions.Add(subscription);
            }

            return CreateSliceEnumerableFromSubscriptions(subscriptions, sliceTimeZone);
        }

        /// <summary>
        /// Gets the history for the requested security
        /// </summary>
        /// <param name="request">The historical data request</param>
        /// <returns>An enumerable of BaseData points</returns>
        public IEnumerable<BaseData> GetHistory(HistoryRequest request)
        {
            if (string.IsNullOrWhiteSpace(_apiKey))
            {
                throw new PolygonAuthenticationException("History calls for Polygon.io require an API key.");
            }

            if (!IsSupported(request.Symbol.SecurityType, request.DataType, request.TickType, request.Resolution))
            {
                yield break;
            }

            // Quote data can only be fetched from Polygon from their Quote Tick endpoint,
            // which would be too slow for anything above second resolution or long time spans.
            if (request.TickType == TickType.Quote && request.Resolution > Resolution.Second)
            {
                Log.Error("PolygonDataQueueHandler.GetHistory(): Quote data above second resolution is not supported.");
                yield break;
            }

            IDataConsolidator consolidator = null;
            IEnumerable<BaseData> history;
            var gettingAggregates = false;

            try
            {
                // Use the trade aggregates API for resolutions above tick for fastest results
                if (request.TickType == TickType.Trade && request.Resolution > Resolution.Tick)
                {
                    history = GetAggregates(request);
                    gettingAggregates = true;
                }
                else if (request.TickType == TickType.Trade)
                {
                    consolidator = request.Resolution != Resolution.Tick
                        ? new TickConsolidator(request.Resolution.ToTimeSpan())
                        : FilteredIdentityDataConsolidator.ForTickType(request.TickType);
                    history = GetTrades(request);
                }
                else
                {
                    consolidator = request.Resolution != Resolution.Tick
                        ? new TickQuoteBarConsolidator(request.Resolution.ToTimeSpan())
                        : FilteredIdentityDataConsolidator.ForTickType(request.TickType);
                    history = GetQuotes(request);
                }
            }
            catch (PolygonForbiddenResourceException)
            {
                if (request.TickType == TickType.Quote)
                {
                    Log.Error("PolygonDataQueueHandler.GetHistory(): Quote data is not available for your Polygon subscription plan.");
                    yield break;
                }

                history = GetAggregates(request);
                gettingAggregates = true;
            }

            if (gettingAggregates)
            {
                foreach (var data in history)
                {
                    Interlocked.Increment(ref _dataPointCount);
                    yield return data;
                }
            }
            else
            {

                BaseData? consolidatedData = null;
                DataConsolidatedHandler onDataConsolidated = (s, e) =>
                {
                    consolidatedData = (BaseData)e;
                };
                consolidator.DataConsolidated += onDataConsolidated;

                foreach (var data in history)
                {
                    consolidator.Update(data);
                    if (consolidatedData != null)
                    {
                        Interlocked.Increment(ref _dataPointCount);
                        yield return consolidatedData;
                        consolidatedData = null;
                    }
                }

                consolidator.DataConsolidated -= onDataConsolidated;
                consolidator.DisposeSafely();
            }
        }

        /// <summary>
        /// Gets the trade bars for the specified history request
        /// </summary>
        private IEnumerable<TradeBar> GetAggregates(HistoryRequest request)
        {
            var ticker = _symbolMapper.GetBrokerageSymbol(request.Symbol);
            var resolutionTimeSpan = request.Resolution.ToTimeSpan();
            // Aggregates API gets timestamps in milliseconds
            var start = Time.DateTimeToUnixTimeStampMilliseconds(request.StartTimeUtc.RoundDown(resolutionTimeSpan));
            var end = Time.DateTimeToUnixTimeStampMilliseconds(request.EndTimeUtc.RoundDown(resolutionTimeSpan));
            var historyTimespan = GetHistoryTimespan(request.Resolution);

            var uri = $"{RestApiBaseUrl}/v2/aggs/ticker/{ticker}/range/1/{historyTimespan}/{start}/{end}";
            var restRequest = new RestRequest(uri, Method.GET);
            restRequest.AddQueryParameter("limit", ApiResponseLimit.ToString());
            restRequest.AddQueryParameter("adjusted", (request.DataNormalizationMode != DataNormalizationMode.Raw).ToString());

            foreach (var bar in DownloadAndParseData<AggregatesResponse>(restRequest).SelectMany(response => response.Results))
            {
                var utcTime = Time.UnixMillisecondTimeStampToDateTime(bar.Timestamp);
                var time = GetTickTime(request.Symbol, utcTime);

                yield return new TradeBar(time, request.Symbol, bar.Open, bar.High, bar.Low, bar.Close,
                    bar.Volume, resolutionTimeSpan);
            }
        }

        /// <summary>
        /// Gets the trade ticks that will potentially be aggregated for the specified history request
        /// </summary>
        private IEnumerable<Tick> GetTrades(HistoryRequest request)
        {
            return GetTicks<TradesResponse, Trade>(request,
                (time, symbol, responseTick) => new Tick(time, request.Symbol, string.Empty, GetExchangeCode(responseTick.ExchangeID),
                    responseTick.Volume, responseTick.Price));
        }

        /// <summary>
        /// Gets the quote ticks that will potentially be aggregated for the specified history request
        /// </summary>
        private IEnumerable<Tick> GetQuotes(HistoryRequest request)
        {
            Tick makeTick<T>(DateTime time, Symbol symbol, T responseTick) where T : Quote =>
                new Tick(time, request.Symbol, string.Empty, GetExchangeCode(responseTick.ExchangeID),
                    responseTick.BidSize, responseTick.BidPrice, responseTick.AskSize, responseTick.AskPrice);

            if (request.Symbol.SecurityType == SecurityType.Option)
            {
                return GetTicks<OptionQuotesResponse, OptionQuote>(request, makeTick);
            }

            return GetTicks<QuotesResponse, Quote>(request, makeTick);
        }

        private IEnumerable<Tick> GetTicks<TResponse, TTick>(HistoryRequest request, Func<DateTime, Symbol, TTick, Tick> tickFactory)
            where TResponse : BaseResultsResponse<TTick>
            where TTick : ResponseTick
        {
            var resolutionTimeSpan = request.Resolution.ToTimeSpan();
            // Trades API gets timestamps in nanoseconds
            var start = Time.DateTimeToUnixTimeStampNanoseconds(request.StartTimeUtc.RoundDown(resolutionTimeSpan));
            var end = Time.DateTimeToUnixTimeStampNanoseconds(request.EndTimeUtc.RoundDown(resolutionTimeSpan));
            var ticker = _symbolMapper.GetBrokerageSymbol(request.Symbol);

            var tickTypeStr = request.TickType == TickType.Trade ? "trades" : "quotes";
            var uri = $"v3/{tickTypeStr}/{ticker}";
            var restRequest = new RestRequest(uri, Method.GET);
            restRequest.AddQueryParameter("timestamp.gte", start.ToString());
            restRequest.AddQueryParameter("timestamp.lt", end.ToString());
            restRequest.AddQueryParameter("order", "asc");
            restRequest.AddQueryParameter("limit", ApiResponseLimit.ToString());

            foreach (var tick in DownloadAndParseData<TResponse>(restRequest).SelectMany(response => response.Results))
            {
                var utcTime = Time.UnixNanosecondTimeStampToDateTime(tick.Timestamp);
                var time = GetTickTime(request.Symbol, utcTime);

                yield return tickFactory(time, request.Symbol, tick);
            }
        }

        /// <summary>
        /// Downloads data and tries to parse the JSON response data into the specified type
        /// </summary>
        protected virtual IEnumerable<T> DownloadAndParseData<T>(RestRequest? request)
            where T : BaseResponse
        {
            while (request != null)
            {
                Log.Debug($"PolygonDataQueueHandler.DownloadAndParseData(): Downloading {request.Resource}");

                if (RestApiRateLimiter != null)
                {
                    if (RestApiRateLimiter.IsRateLimited)
                    {
                        Log.Trace("PolygonDataQueueHandler.DownloadAndParseData(): Rest API calls are limited; waiting to proceed.");
                    }
                    RestApiRateLimiter.WaitToProceed();
                }

                request.AddHeader("Authorization", $"Bearer {_apiKey}");

                var response = _restClient.Execute(request);
                if (response == null)
                {
                    Log.Debug($"PolygonDataQueueHandler.DownloadAndParseData(): No response for {request.Resource}");
                    yield break;
                }

                if (response.StatusCode == HttpStatusCode.Forbidden)
                {
                    throw new PolygonForbiddenResourceException("Not authorized");
                }

                // If the data download was not successful, log the reason
                var resultJson = JObject.Parse(response.Content);
                if (resultJson["status"]?.ToString().ToUpperInvariant() != "OK")
                {
                    Log.Debug($"PolygonDataQueueHandler.DownloadAndParseData(): No data for {request.Resource}. Reason: {response}");
                    yield break;
                }

                var result = resultJson.ToObject<T>();
                if (result == null)
                {
                    Log.Debug($"PolygonDataQueueHandler.DownloadAndParseData(): Unable to parse response for {request.Resource}. Response: {response}");
                    yield break;
                }

                yield return result;

                request = result.NextUrl != null ? new RestRequest(result.NextUrl, Method.GET) : null;
            }
        }

        /// <summary>
        /// Converts the given resolution into the corresponding timespan for the Polygon.io API
        /// </summary>
        private static string GetHistoryTimespan(Resolution resolution)
        {
            switch (resolution)
            {
                case Resolution.Daily:
                    return "day";

                case Resolution.Hour:
                    return "hour";

                case Resolution.Minute:
                    return "minute";

                case Resolution.Second:
                    return "second";

                default:
                    throw new Exception($"Unsupported resolution: {resolution}.");
            }
        }
    }
}
