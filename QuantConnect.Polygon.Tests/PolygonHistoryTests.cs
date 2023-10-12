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
using QuantConnect.Polygon;
using QuantConnect.Securities;
using QuantConnect.Util;
using Microsoft.CodeAnalysis;
using Newtonsoft.Json;
using QuantConnect.Configuration;
using System.Diagnostics;
using System;
using System.Linq;

namespace QuantConnect.Tests.Polygon
{
    [TestFixture]
    public class PolygonHistoryTests
    {
        private string _apiKey = Config.Get("polygon-api-key");
        private PolygonDataQueueHandler _historyProvider;

        [SetUp]
        public void SetUp()
        {
            Log.LogHandler = new CompositeLogHandler();

            _historyProvider = new PolygonDataQueueHandler(_apiKey, false);
            _historyProvider.Initialize(new HistoryProviderInitializeParameters(null, null, null, null, null, null, null, false, null, null));

        }

        [TearDown]
        public void TeadDown()
        {
            _historyProvider.Dispose();
        }

        private static TestCaseData[] HistoricalTradeBarsTestCases()
        {
            var optionSymbol = Symbol.CreateOption(Symbols.SPY, Market.USA, OptionStyle.American, OptionRight.Call, 429m, new DateTime(2023, 10, 06));

            return new[]
            {
                new TestCaseData(optionSymbol, Resolution.Minute, TickType.Trade, TimeSpan.FromDays(100)),
                new TestCaseData(optionSymbol, Resolution.Minute, TickType.Trade, TimeSpan.FromDays(200)),
                new TestCaseData(optionSymbol, Resolution.Hour, TickType.Trade, TimeSpan.FromDays(365)),
                new TestCaseData(optionSymbol, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(3650))
            };
        }

        [TestCaseSource(nameof(HistoricalTradeBarsTestCases))]
        [Explicit("This tests require a Polygon.io api key, requires internet and are long.")]
        public void GetsHistoricalTradeBars(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period)
        {
            var request = CreateHistoryRequest(symbol, resolution, tickType, period);
            var history = _historyProvider.GetHistory(request).ToList();

            Log.Trace("Data points retrieved: " + _historyProvider.DataPointCount);

            Assert.That(history, Is.Not.Empty);

            foreach (var baseData in history)
            {
                var bar = (TradeBar)baseData;
                Log.Trace($"{bar.Time}: {bar.Symbol} - O={bar.Open}, H={bar.High}, L={bar.Low}, C={bar.Close}");
            }

            // Ordered by time
            Assert.That(history, Is.Ordered.By("Time"));

            // No repeating bars
            var timesArray = history.Select(x => x.Time).ToList();
            Assert.That(timesArray.Distinct().Count(), Is.EqualTo(timesArray.Count));
        }

        [Test]
        [Explicit("This tests require a Polygon.io api key, requires internet and are long.")]
        public void GetsSameBarCountForDifferentResponseLimits()
        {
            const Resolution resolution = Resolution.Minute;
            const TickType tickType = TickType.Trade;
            var symbol = Symbol.CreateOption(Symbols.SPY, Market.USA, OptionStyle.American, OptionRight.Call, 429m, new DateTime(2023, 10, 06));

            var request = CreateHistoryRequest(symbol, resolution, tickType, TimeSpan.FromDays(100));

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            var history1 = _historyProvider.GetHistory(request).ToList();
            stopwatch.Stop();
            var history1Duration = stopwatch.Elapsed;

            Assert.That(history1, Is.Not.Empty);

            Log.Trace($"Retrieved {_historyProvider.DataPointCount} data points in {history1Duration}");

            Config.Set("polygon-aggregate-response-limit", 500);
            using var newHistoryProvider = new PolygonDataQueueHandler(_apiKey, false);

            stopwatch.Restart();
            var history2 = newHistoryProvider.GetHistory(request).ToList();
            stopwatch.Stop();
            var history2Duration = stopwatch.Elapsed;

            Log.Trace($"Retrieved {newHistoryProvider.DataPointCount} data points in {history2Duration}");

            Assert.That(history2, Has.Count.EqualTo(history1.Count));
            Assert.That(history1, Is.Not.Empty);

            Assert.That(history2Duration, Is.GreaterThan(history1Duration));
        }

        [Test]
        public void MakesTheRightNumberOfApiCallsToGetHistory()
        {
            // 1 hour of data per api call
            const int responseLimit = 60;
            const Resolution resolution = Resolution.Minute;
            const TickType tickType = TickType.Trade;
            var symbol = Symbol.CreateOption(Symbols.SPY, Market.USA, OptionStyle.American, OptionRight.Call, 429m, new DateTime(2023, 10, 06));
            var start = new DateTime(2023, 01, 02);
            var end = start.AddDays(1);

            var request = new HistoryRequest(
                start,
                end,
                LeanData.GetDataType(resolution, tickType),
                symbol,
                resolution,
                SecurityExchangeHours.AlwaysOpen(TimeZones.NewYork),
                TimeZones.NewYork,
                null,
                true,
                false,
                DataNormalizationMode.Adjusted,
                tickType);

            using var historyProvider = new TestPolygonHistoryProvider();
            historyProvider.ResponseLimit = responseLimit;
            historyProvider.SetHistoryRequest(request);

            var history = historyProvider.GetHistory(request).ToList();

            Log.Trace("Data points retrieved: " + _historyProvider.DataPointCount);

            Assert.That(history, Has.Count.EqualTo((end - start).TotalMinutes));
            Assert.That(historyProvider.ApiCallsCount, Is.EqualTo((end - start).TotalMinutes / responseLimit));
        }

        private static TestCaseData[] UssuportedSecurityTypesResolutionsAndTickTypesTestCases => new[]
        {
            // Supported resolution and tick type, unsupported security type symbol
            new TestCaseData(Symbols.SPY, Resolution.Minute, TickType.Trade),
            new TestCaseData(Symbols.USDJPY, Resolution.Minute, TickType.Trade),
            new TestCaseData(Symbols.BTCUSD, Resolution.Minute, TickType.Trade),
            new TestCaseData(Symbols.DE10YBEUR, Resolution.Minute, TickType.Trade),
            new TestCaseData(Symbols.SPX, Resolution.Minute, TickType.Trade),
            new TestCaseData(Symbols.Future_ESZ18_Dec2018, Resolution.Minute, TickType.Trade),

            // Supported security type symbol and tick type, unsupported resolution
            new TestCaseData(Symbols.SPY_C_192_Feb19_2016, Resolution.Tick, TickType.Trade),
            new TestCaseData(Symbols.SPY_C_192_Feb19_2016, Resolution.Second, TickType.Trade),

            // Supported security type and resolution, unsupported tick type
            new TestCaseData(Symbols.SPY_C_192_Feb19_2016, Resolution.Minute, TickType.Quote),
            new TestCaseData(Symbols.SPY_C_192_Feb19_2016, Resolution.Minute, TickType.OpenInterest),
        };

        [TestCaseSource(nameof(UssuportedSecurityTypesResolutionsAndTickTypesTestCases))]
        public void ReturnsEmptyForUnsupportedSecurityTypeResolutionOrTickType(Symbol symbol, Resolution resolution, TickType tickType)
        {
            using var historyProvider = new TestPolygonHistoryProvider();
            var request = CreateHistoryRequest(symbol, resolution, tickType, TimeSpan.FromDays(100));
            var history = historyProvider.GetHistory(request).ToList();

            Assert.That(history, Is.Empty);
            Assert.That(historyProvider.ApiCallsCount, Is.EqualTo(0));
        }

        [TestCase(5)]
        [TestCase(10)]
        [Explicit("This tests require a Polygon.io api key, requires internet and are long.")]
        public void RateLimitsHistoryApiCalls(int historyRequestsCount)
        {
            var symbol = Symbol.CreateOption(Symbols.SPY, Market.USA, OptionStyle.American, OptionRight.Call, 429m, new DateTime(2023, 10, 06));
            var request = CreateHistoryRequest(symbol, Resolution.Minute, TickType.Trade, TimeSpan.FromDays(1));

            var rate = TimeSpan.FromSeconds(5);
            using var unlimitedGate = new RateGate(int.MaxValue, rate);
            using var unlimitedHistoryProvider = new ConfigurableRateLimitedPolygonHistoryProvider(_apiKey, unlimitedGate);

            var timer = Stopwatch.StartNew();
            for (var i = 0; i < historyRequestsCount; i++)
            {
                var history = unlimitedHistoryProvider.GetHistory(request).ToList();
            }
            timer.Stop();
            var unlimitedHistoryRequestsElapsedTime = timer.Elapsed;

            using var gate = new RateGate(1, rate);
            using var rateLimitedHistoryProvider = new ConfigurableRateLimitedPolygonHistoryProvider(_apiKey, gate);

            timer = Stopwatch.StartNew();
            for (var i = 0; i < historyRequestsCount; i++)
            {
                var history = rateLimitedHistoryProvider.GetHistory(request).ToList();
            }
            timer.Stop();
            var rateLimitedHistoryRequestsElapsedTime = timer.Elapsed;

            var delay = rateLimitedHistoryRequestsElapsedTime - unlimitedHistoryRequestsElapsedTime;
            var expectedDelay = rate * historyRequestsCount;
            var lowerBound = expectedDelay - expectedDelay * 0.30;
            var upperBound = expectedDelay + expectedDelay * 0.30;

            Assert.That(delay, Is.GreaterThanOrEqualTo(lowerBound), $"The rate gate was early: {lowerBound - delay}");
            Assert.That(delay, Is.LessThanOrEqualTo(upperBound), $"The rate gate was late: {delay - upperBound}");
        }

        private static HistoryRequest CreateHistoryRequest(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period)
        {
            var end = new DateTime(2023, 10, 6);
            var dataType = LeanData.GetDataType(resolution, tickType);

            return new HistoryRequest(end.Subtract(period),
                end,
                dataType,
                symbol,
                resolution,
                SecurityExchangeHours.AlwaysOpen(TimeZones.NewYork),
                TimeZones.NewYork,
                null,
                true,
                false,
                DataNormalizationMode.Adjusted,
                tickType);
        }

        private class TestPolygonHistoryProvider : PolygonDataQueueHandler
        {
            private DateTime _currentStart;

            public HistoryRequest CurrentHistoryRequest { get; private set; }

            public int ResponseLimit { get; set; }

            public int ApiCallsCount { get; private set; }

            public TestPolygonHistoryProvider() : base("test-api-key", false) { }

            public void SetHistoryRequest(HistoryRequest request)
            {
                _currentStart = request.StartTimeUtc;
                CurrentHistoryRequest = request;
            }

            protected override T DownloadAndParseData<T>(string url)
            {
                return JsonConvert.DeserializeObject<T>(DownloadData());
            }

            public string DownloadData()
            {
                var start = _currentStart;
                _currentStart = start.AddMinutes(ResponseLimit);
                ApiCallsCount++;

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
                    NextUrl = _currentStart < CurrentHistoryRequest.EndTimeUtc ? "https://www.someourl.com" : null
                });
            }
        }

        private class ConfigurableRateLimitedPolygonHistoryProvider : PolygonDataQueueHandler
        {
            protected override RateGate HistoryRateLimiter { get; }

            public ConfigurableRateLimitedPolygonHistoryProvider(string apiKey, RateGate rateGate)
                : base(apiKey, false)
            {
                HistoryRateLimiter = rateGate;
            }
        }
    }
}
