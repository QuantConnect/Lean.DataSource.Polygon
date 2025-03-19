/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Util;
using Newtonsoft.Json;
using QuantConnect.Configuration;
using System.Diagnostics;
using System;
using System.Linq;
using System.Collections.Generic;
using RestSharp;
using QuantConnect.Tests;
using NodaTime;

namespace QuantConnect.Lean.DataSource.Polygon.Tests
{
    [TestFixture]
    public class PolygonHistoryTests
    {
        private readonly string _apiKey = Config.Get("polygon-api-key");
        private PolygonDataProvider _historyProvider;

        [SetUp]
        public void SetUp()
        {
            _historyProvider = new PolygonDataProvider(_apiKey, streamingEnabled: false);
            _historyProvider.Initialize(new HistoryProviderInitializeParameters(null, null, null, null, null, null, null, false, null, null, null));

        }

        [TearDown]
        public void TearDown()
        {
            if (_historyProvider != null)
            {
                _historyProvider.Dispose();
            }
        }

        internal static IEnumerable<TestCaseData> HistoricalDataTestCases
        {
            get
            {
                var SPY = Symbols.SPY;
                yield return new TestCaseData(SPY, Resolution.Tick, TimeSpan.FromMinutes(5), TickType.Trade);
                yield return new TestCaseData(SPY, Resolution.Second, TimeSpan.FromMinutes(30), TickType.Trade);
                yield return new TestCaseData(SPY, Resolution.Minute, TimeSpan.FromDays(15), TickType.Trade);
                yield return new TestCaseData(SPY, Resolution.Hour, TimeSpan.FromDays(180), TickType.Trade);
                yield return new TestCaseData(SPY, Resolution.Daily, TimeSpan.FromDays(3650), TickType.Trade);

                // Quotes (Only Tick and Second resolutions are supported)
                yield return new TestCaseData(SPY, Resolution.Tick, TimeSpan.FromMinutes(5), TickType.Quote);
                yield return new TestCaseData(SPY, Resolution.Second, TimeSpan.FromMinutes(5), TickType.Quote);

                var SPY_Option = Symbol.CreateOption(Symbols.SPY, Market.USA, OptionStyle.American, OptionRight.Call, 469m, new DateTime(2023, 12, 15));

                yield return new TestCaseData(SPY_Option, Resolution.Tick, TimeSpan.FromMinutes(5), TickType.Trade);
                yield return new TestCaseData(SPY_Option, Resolution.Second, TimeSpan.FromMinutes(30), TickType.Trade);
                yield return new TestCaseData(SPY_Option, Resolution.Minute, TimeSpan.FromDays(15), TickType.Trade);
                yield return new TestCaseData(SPY_Option, Resolution.Hour, TimeSpan.FromDays(180), TickType.Trade);
                yield return new TestCaseData(SPY_Option, Resolution.Daily, TimeSpan.FromDays(3650), TickType.Trade);

                // Quotes (Only Tick and Second resolutions are supported)
                yield return new TestCaseData(SPY_Option, Resolution.Tick, TimeSpan.FromMinutes(5), TickType.Quote);
                yield return new TestCaseData(SPY_Option, Resolution.Second, TimeSpan.FromMinutes(5), TickType.Quote);
            }
        }

        [TestCaseSource(nameof(HistoricalDataTestCases))]
        [Explicit("This tests require a Polygon.io api key, requires internet and are long.")]
        public void GetsHistoricalData(Symbol symbol, Resolution resolution, TimeSpan period, TickType tickType)
        {
            var requests = new List<HistoryRequest> { CreateHistoryRequest(symbol, resolution, tickType, period) };

            var history = _historyProvider.GetHistory(requests, TimeZones.Utc).ToList();

            Log.Trace("Data points retrieved: " + history.Count);

            AssertHistoricalDataResults(history.Select(x => x.AllData).SelectMany(x => x).ToList(), resolution, _historyProvider.DataPointCount);
        }

        [TestCase("GOOGL", "2014/4/1", "2016/4/1", Resolution.Daily, Description = "The stock split on July 15 2022. [GOOG -> GOOGL]")]
        [TestCase("GOOGL", "2014/4/1", "2014/4/4", Resolution.Hour)]
        public void GetsRenamedSymbolHistoricalData(string ticker, DateTime startDateTime, DateTime endDateTime, Resolution resolution)
        {
            var symbol = Symbol.Create(ticker, SecurityType.Equity, Market.USA);

            var request = CreateHistoryRequest(symbol, resolution, TickType.Trade, startDateTime, endDateTime);

            var history = _historyProvider.GetHistory(new[] { request }, TimeZones.NewYork)?.ToList();

            Log.Trace("Data points retrieved: " + history.Count);

            Assert.IsNotNull(history);
            Assert.IsNotEmpty(history);
            Assert.That(history.First().Time.Date, Is.EqualTo(startDateTime));
            Assert.That(history.Last().Time.Date, Is.LessThanOrEqualTo(endDateTime));
            Assert.That(history.First().AllData.First().Symbol.Value, Is.EqualTo("GOOG"));
            Assert.That(history.Last().AllData.First().Symbol.Value, Is.EqualTo("GOOGL"));

            AssertHistoricalDataResults(history.Select(x => x.AllData).SelectMany(x => x).ToList(), resolution);
        }

        internal static void AssertHistoricalDataResults(List<BaseData> history, Resolution resolution, int? expectedCount = null)
        {
            // Assert that we got some data
            Assert.That(history, Is.Not.Empty);
            if (expectedCount.HasValue)
            {
                Assert.That(history.Count, Is.EqualTo(expectedCount));
            }

            if (resolution > Resolution.Tick)
            {
                // No repeating bars
                var timesArray = history.Select(x => x.Time).ToList();
                Assert.That(timesArray.Distinct().Count(), Is.EqualTo(timesArray.Count));

                // Resolution is respected
                var timeSpan = resolution.ToTimeSpan();
                Assert.That(history, Is.All.Matches<BaseData>(x => x.EndTime - x.Time == timeSpan),
                    $"All bars periods should be equal to {timeSpan} ({resolution})");
            }
            else
            {
                // All data in the slice are ticks
                Assert.That(history, Is.All.Matches<BaseData>(tick => tick.GetType() == typeof(Tick)));
            }
        }

        internal static TestCaseData[] IndexHistoricalDataTestCases
        {
            get
            {
                return new[]
                {
                    // Trades
                    new TestCaseData(Resolution.Tick, TimeSpan.FromMinutes(5), TickType.Trade, true),   // Tick data is not available for indexes
                    new TestCaseData(Resolution.Second, TimeSpan.FromMinutes(30), TickType.Trade, false),
                    new TestCaseData(Resolution.Minute, TimeSpan.FromDays(15), TickType.Trade, false),
                    new TestCaseData(Resolution.Hour, TimeSpan.FromDays(180), TickType.Trade, false),
                    new TestCaseData(Resolution.Daily, TimeSpan.FromDays(3650), TickType.Trade, false),

                    // Quotes: quote data is not available for indexes
                    new TestCaseData(Resolution.Tick, TimeSpan.FromMinutes(5), TickType.Quote, true),
                    new TestCaseData(Resolution.Second, TimeSpan.FromMinutes(5), TickType.Quote, true)
                };
            }
        }

        [TestCaseSource(nameof(IndexHistoricalDataTestCases))]
        [Explicit("This tests require a Polygon.io api key, requires internet and are long.")]
        public void GetsIndexHistoricalData(Resolution resolution, TimeSpan period, TickType tickType, bool shouldBeNull)
        {
            var history = GetIndexHistory(resolution, period, tickType);

            if (shouldBeNull)
            {
                Assert.IsNull(history);
            }
            else
            {
                Log.Trace("Data points retrieved: " + history.Count);
                AssertHistoricalDataResults(history.Select(x => x.AllData).SelectMany(x => x).ToList(), resolution, _historyProvider.DataPointCount);
            }
        }

        internal static TestCaseData[] IndexHistoricalInvalidDataTestCases
        {
            get
            {
                return new[]
                {
                    new TestCaseData(Resolution.Daily, TimeSpan.FromMinutes(5), TickType.Quote),
                    new TestCaseData(Resolution.Hour, TimeSpan.FromMinutes(5), TickType.Quote),
                    new TestCaseData(Resolution.Minute, TimeSpan.FromMinutes(5), TickType.Quote),
                };
            }
        }

        [TestCaseSource(nameof(IndexHistoricalInvalidDataTestCases))]
        [Explicit("This tests require a Polygon.io api key, requires internet and are long.")]
        public void GetsIndexInvalidHistoricalData(Resolution resolution, TimeSpan period, TickType tickType)
        {
            var history = GetIndexHistory(resolution, period, tickType);

            Assert.IsNull(history);
        }

        [Test]
        [Explicit("This tests require a Polygon.io api key, requires internet and are long.")]
        public void GetsSameBarCountForDifferentResponseLimits()
        {
            // Set a high limit for the first request so less requests are made
            using var historyProvider = new ConfigurableResponseLimitPolygonHistoryProvider(_apiKey, 5000);

            const Resolution resolution = Resolution.Minute;
            const TickType tickType = TickType.Trade;
            var symbol = Symbol.CreateOption(Symbols.SPY, Market.USA, OptionStyle.American, OptionRight.Call, 429m, new DateTime(2023, 10, 06));

            var request = CreateHistoryRequest(symbol, resolution, tickType, TimeSpan.FromDays(100));

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            var history1 = historyProvider.GetHistory(request).ToList();
            stopwatch.Stop();
            var history1Duration = stopwatch.Elapsed;

            Assert.That(history1, Is.Not.Empty);

            Log.Debug($"Retrieved {historyProvider.DataPointCount} data points in {history1Duration}");

            // Set a low limit for the second request so more requests are made
            using var historyProvider2 = new ConfigurableResponseLimitPolygonHistoryProvider(_apiKey, 100);

            stopwatch.Restart();
            var history2 = historyProvider2.GetHistory(request).ToList();
            stopwatch.Stop();
            var history2Duration = stopwatch.Elapsed;

            Log.Debug($"Retrieved {historyProvider2.DataPointCount} data points in {history2Duration}");

            Assert.That(history2, Has.Count.EqualTo(history1.Count));
            Assert.That(history1, Is.Not.Empty);

            Assert.That(history2Duration, Is.GreaterThan(history1Duration));
        }

        private static TestCaseData[] UssuportedSecurityTypesResolutionsAndTickTypesTestCases => new[]
        {
            // Supported resolution and tick type, unsupported security type symbol
            new TestCaseData(Symbols.USDJPY, Resolution.Minute, TickType.Trade),
            new TestCaseData(Symbols.BTCUSD, Resolution.Minute, TickType.Trade),
            new TestCaseData(Symbols.DE10YBEUR, Resolution.Minute, TickType.Trade),
            new TestCaseData(Symbols.Future_ESZ18_Dec2018, Resolution.Minute, TickType.Trade),

            // Supported security type and resolution, unsupported tick type
            new TestCaseData(Symbols.SPY_C_192_Feb19_2016, Resolution.Minute, TickType.OpenInterest),

            // Supported security type unsupported resolution and tick type combination
            new TestCaseData(Symbols.SPY, Resolution.Minute, TickType.Quote),
            new TestCaseData(Symbols.SPY, Resolution.Hour, TickType.Quote),
            new TestCaseData(Symbols.SPY, Resolution.Daily, TickType.Quote),
        };

        [TestCaseSource(nameof(UssuportedSecurityTypesResolutionsAndTickTypesTestCases))]
        public void ReturnsNullForUnsupportedSecurityTypeResolutionOrTickType(Symbol symbol, Resolution resolution, TickType tickType)
        {
            using var historyProvider = new TestPolygonHistoryProvider();
            var request = CreateHistoryRequest(symbol, resolution, tickType, TimeSpan.FromDays(100));
            var history = historyProvider.GetHistory(request)?.ToList();

            Assert.IsNull(history);
            Assert.That(historyProvider.TestRestApiClient.ApiCallsCount, Is.EqualTo(0));
        }

        [TestCase(5)]
        [TestCase(10)]
        [Explicit("This tests require a Polygon.io api key, requires internet and are long.")]
        public void RateLimitsHistoryApiCalls(int historyRequestsCount)
        {
            var symbol = Symbol.CreateOption(Symbols.SPY, Market.USA, OptionStyle.American, OptionRight.Call, 429m, new DateTime(2023, 12, 15));
            var request = CreateHistoryRequest(symbol, Resolution.Minute, TickType.Trade, TimeSpan.FromDays(1));

            var rate = TimeSpan.FromSeconds(5);
            using var unlimitedGate = new RateGate(int.MaxValue, rate);
            using var unlimitedHistoryProvider = new ConfigurableRateLimitedPolygonHistoryProvider(_apiKey, unlimitedGate);
            List<BaseData> history1 = null;

            var timer = Stopwatch.StartNew();
            for (var i = 0; i < historyRequestsCount; i++)
            {
                history1 = unlimitedHistoryProvider.GetHistory(request).ToList();
            }
            timer.Stop();
            var unlimitedHistoryRequestsElapsedTime = timer.Elapsed;

            using var gate = new RateGate(1, rate);
            using var rateLimitedHistoryProvider = new ConfigurableRateLimitedPolygonHistoryProvider(_apiKey, gate);
            List<BaseData> history2 = null;

            timer = Stopwatch.StartNew();
            for (var i = 0; i < historyRequestsCount; i++)
            {
                history2 = rateLimitedHistoryProvider.GetHistory(request).ToList();
            }
            timer.Stop();
            var rateLimitedHistoryRequestsElapsedTime = timer.Elapsed;

            Assert.That(history1, Is.Not.Empty.And.Count.EqualTo(history2.Count));

            var delay = rateLimitedHistoryRequestsElapsedTime - unlimitedHistoryRequestsElapsedTime;
            var expectedDelay = rate * historyRequestsCount;
            var lowerBound = expectedDelay - expectedDelay * 0.30;
            var upperBound = expectedDelay + expectedDelay * 0.30;

            Assert.That(delay, Is.GreaterThanOrEqualTo(lowerBound), $"The rate gate was early: {lowerBound - delay}");
            Assert.That(delay, Is.LessThanOrEqualTo(upperBound), $"The rate gate was late: {delay - upperBound}");
        }

        /// <summary>
        /// Retrieves the historical data of an hardcoded index [SPX] based on specified parameters.
        /// </summary>
        /// <param name="resolution">The resolution of the historical data to retrieve.</param>
        /// <param name="period">The time period for which historical data is requested.</param>
        /// <param name="tickType">The type of ticks for the historical data.</param>
        /// <returns>A list of <see cref="Slice"/> containing historical data of the index.</returns>
        /// <remarks>
        /// The <paramref name="resolution"/> parameter determines the granularity of the historical data, 
        /// while the <paramref name="period"/> parameter specifies the duration of the historical data to be retrieved.
        /// The <paramref name="tickType"/> parameter specifies the type of ticks to be included in the historical data.
        /// </remarks>
        internal List<Slice> GetIndexHistory(Resolution resolution, TimeSpan period, TickType tickType)
        {
            var symbol = Symbol.Create("SPX", SecurityType.Index, Market.USA);
            var requests = new List<HistoryRequest> { CreateHistoryRequest(symbol, resolution, tickType, period) };
            return _historyProvider.GetHistory(requests, TimeZones.Utc)?.ToList();
        }

        internal static HistoryRequest CreateHistoryRequest(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period)
        {
            var end = new DateTime(2023, 12, 15, 16, 0, 0);

            if (resolution == Resolution.Daily)
            {
                end = end.Date.AddDays(1);
            }

            return CreateHistoryRequest(symbol, resolution, tickType, end.Subtract(period), end);
        }

        internal static HistoryRequest CreateHistoryRequest(Symbol symbol, Resolution resolution, TickType tickType, DateTime startDateTime, DateTime endDateTime,
            SecurityExchangeHours exchangeHours = null, DateTimeZone dataTimeZone = null)
        {
            if (exchangeHours == null)
            {
                exchangeHours = SecurityExchangeHours.AlwaysOpen(TimeZones.NewYork);
            }

            if (dataTimeZone == null)
            {
                dataTimeZone = TimeZones.NewYork;
            }

            var dataType = LeanData.GetDataType(resolution, tickType);
            return new HistoryRequest(
                startDateTime,
                endDateTime,
                dataType,
                symbol,
                resolution,
                exchangeHours,
                dataTimeZone,
                null,
                true,
                false,
                DataNormalizationMode.Adjusted,
                tickType
                );
        }

        private class TestPolygonRestApiClient : PolygonRestApiClient
        {
            public int ResponseLimit { get; set; }

            public int ApiCallsCount { get; private set; }

            public TestPolygonRestApiClient() : base(string.Empty) { }

            public override IEnumerable<T> DownloadAndParseData<T>(RestRequest request)
            {
                return new List<T>
                {
                    JsonConvert.DeserializeObject<T>(DownloadData())
                };
            }

            private string DownloadData()
            {
                ApiCallsCount++;
                var start = new DateTime(2023, 12, 15, 9, 30, 0);
                return JsonConvert.SerializeObject(new AggregatesResponse
                {
                    Results = Enumerable.Range(0, ResponseLimit).Select(i => new SingleResponseAggregate
                    {
                        Timestamp = Convert.ToInt64(Time.DateTimeToUnixTimeStampMilliseconds(start.AddMinutes(i))),
                        Open = 1.5m,
                        High = 1.5m,
                        Low = 1.5m,
                        Close = 1.5m,
                        Volume = 100m
                    }).ToList(),
                });
            }
        }

        private class TestPolygonHistoryProvider : PolygonDataProvider
        {
            public TestPolygonRestApiClient TestRestApiClient => RestApiClient as TestPolygonRestApiClient;

            public TestPolygonHistoryProvider()
                : base("test-api-key", streamingEnabled: false)
            {
                RestApiClient.DisposeSafely();
                RestApiClient = new TestPolygonRestApiClient();
            }

            protected override List<ExchangeMapping> FetchExchangeMappings()
            {
                return new List<ExchangeMapping>();
            }
        }

        private class ConfigurableRateLimitedPolygonHistoryProvider : PolygonDataProvider
        {
            public ConfigurableRateLimitedPolygonHistoryProvider(string apiKey, RateGate rateGate)
                : base(apiKey, streamingEnabled: false)
            {
                RestApiClient.DisposeSafely();
                RestApiClient = new TestApiClient(apiKey, rateGate);
            }

            private class TestApiClient : PolygonRestApiClient
            {
                private RateGate _rateGate;
                protected override RateGate RateLimiter => _rateGate;

                public TestApiClient(string apiKey, RateGate rateGate) : base(apiKey)
                {
                    _rateGate = rateGate;
                }
            }
        }

        private class ConfigurableResponseLimitPolygonHistoryProvider : PolygonDataProvider
        {
            public ConfigurableResponseLimitPolygonHistoryProvider(string apiKey, int responseLimit)
                : base(apiKey, streamingEnabled: false)
            {
                RestApiClient.DisposeSafely();
                RestApiClient = new TestApiClient(apiKey, responseLimit.ToString());
            }

            private class TestApiClient : PolygonRestApiClient
            {
                private string _responseLimit;
                protected override string ApiResponseLimit => _responseLimit;

                public TestApiClient(string apiKey, string responseLimit) : base(apiKey)
                {
                    _responseLimit = responseLimit;
                }
            }
        }
    }
}
