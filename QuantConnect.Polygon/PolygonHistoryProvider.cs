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

using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using NodaTime;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Lean.Engine.HistoricalData;
using QuantConnect.Logging;
using QuantConnect.Configuration;
using static QuantConnect.StringExtensions;

namespace QuantConnect.Polygon
{
    public partial class PolygonDataQueueHandler : SynchronizingHistoryProvider
    {
        private const string HistoryBaseUrl = "https://api.polygon.io/v2";
        private readonly int AggregateDataResponseLimit = Config.GetInt("polygon-aggregate-response-limit", 5000);

        private int _dataPointCount;

        public override int DataPointCount => _dataPointCount;

        public override void Initialize(HistoryProviderInitializeParameters parameters)
        {
        }

        public override IEnumerable<Slice> GetHistory(IEnumerable<Data.HistoryRequest> requests, DateTimeZone sliceTimeZone)
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
            return ProcessHistoryRequest(request);
        }

        private IEnumerable<BaseData> ProcessHistoryRequest(HistoryRequest request)
        {
            if (string.IsNullOrWhiteSpace(_apiKey))
            {
                Log.Error("PolygonDataQueueHandler.GetHistory(): History calls for Polygon.io require an API key.");
                yield break;
            }

            // check security type
            if (!_supportedSecurityTypes.Contains(request.Symbol.SecurityType))
            {
                Log.Error($"PolygonDataQueueHandler.ProcessHistoryRequests(): Unsupported security type: {request.Symbol.SecurityType}.");
                yield break;
            }

            // we only support minute, hour and daily resolution for option data
            if (request.Resolution < Resolution.Minute)
            {
                Log.Error($"PolygonDataQueueHandler.ProcessHistoryRequests(): Unsupported resolution: {request.Resolution}.");
                yield break;
            }

            // check tick type
            if (request.TickType != TickType.Trade)
            {
                Log.Error($"PolygonDataQueueHandler.ProcessHistoryRequests(): Unsupported tick type: {request.TickType}.");
                yield break;
            }

            Log.Trace("PolygonDataQueueHandler.ProcessHistoryRequests(): Submitting request: " +
                Invariant($"{request.Symbol.SecurityType}-{request.TickType}-{request.Symbol.Value}: {request.Resolution} {request.StartTimeUtc}->{request.EndTimeUtc}"));

            foreach (var tradeBar in GetTradeBars(request))
            {
                Interlocked.Increment(ref _dataPointCount);

                yield return tradeBar;
            }
        }

        private IEnumerable<TradeBar> GetTradeBars(HistoryRequest request)
        {
            var historyTimespan = GetHistoryTimespan(request.Resolution);
            var resolutionTimeSpan = request.Resolution.ToTimeSpan();
            var start = Time.DateTimeToUnixTimeStampMilliseconds(request.StartTimeUtc.RoundDown(resolutionTimeSpan));
            var end = Time.DateTimeToUnixTimeStampMilliseconds(request.EndTimeUtc.RoundDown(resolutionTimeSpan));

            var url = $"{HistoryBaseUrl}/aggs/ticker/{_symbolMapper.GetBrokerageSymbol(request.Symbol)}/range/1/{historyTimespan}/{start}/{end}";
            var baseQuery = $"apiKey={_apiKey}&limit={AggregateDataResponseLimit}";

            while (!string.IsNullOrEmpty(url))
            {
                var response = DownloadAndParseData<AggregatesResponse>(AddQueryToUrl(new Uri(url), baseQuery));
                if (response == null)
                {
                    break;
                }

                var responseTradeBars = response.Results;

                foreach (var responseBar in responseTradeBars)
                {
                    var utcTime = Time.UnixMillisecondTimeStampToDateTime(responseBar.Timestamp);
                    var time = GetTickTime(request.Symbol, utcTime);

                    yield return new TradeBar(time, request.Symbol, responseBar.Open, responseBar.High, responseBar.Low, responseBar.Close,
                        responseBar.Volume);
                }

                url = response.NextUrl;
            }
        }

        protected virtual T DownloadAndParseData<T>(string url)
        {
            var result = url.DownloadData();
            if (result == null)
            {
                return default;
            }

            // If the data download was not successful, log the reason
            var parsedResult = JObject.Parse(result);
            var success = parsedResult["success"]?.Value<bool>() ?? false;
            if (!success)
            {
                success = parsedResult["status"]?.ToString().ToUpperInvariant() == "OK";
            }

            if (!success)
            {
                Log.Debug($"No data for {url}. Reason: {result}");
                return default;
            }

            return result == null ? default : JsonConvert.DeserializeObject<T>(result);
        }

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

                default:
                    throw new Exception($"PolygonDataQueueHandler.GetHistoryTimespan(): unsupported resolution: {resolution}.");
            }
        }

        /// <summary>
        /// Adds the given query to the url, making sure it keeps the existing query parameters if any
        /// </summary>
        private static string AddQueryToUrl(Uri url, string query)
        {
            var newQuery = url.Query;
            if (!string.IsNullOrEmpty(newQuery))
            {
                newQuery += $"&{query}";
            }
            else
            {
                newQuery = $"?{query}";
            }

            return new Uri(url, newQuery).ToString();
        }
    }
}
