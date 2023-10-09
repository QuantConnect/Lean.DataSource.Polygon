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

namespace QuantConnect.Tests.Polygon
{
    [TestFixture]
    public class PolygonHistoryTests
    {
        private PolygonDataQueueHandler _historyProvider;

        [SetUp]
        public void SetUp()
        {
            Log.LogHandler = new CompositeLogHandler();

            _historyProvider = new PolygonDataQueueHandler();
            _historyProvider.Initialize(new HistoryProviderInitializeParameters(null, null, null, null, null, null, null, false, null, null));

        }

        private static TestCaseData[] HistoricalTradeBarsTestCases()
        {
            var optionSymbol = Symbol.CreateOption(Symbols.SPY, Market.USA, OptionStyle.American, OptionRight.Call, 429m, new DateTime(2023, 10, 06));

            return new[]
            {
                // long requests
                new TestCaseData(optionSymbol, Resolution.Minute, TickType.Trade, TimeSpan.FromDays(100)),
                new TestCaseData(optionSymbol, Resolution.Minute, TickType.Trade, TimeSpan.FromDays(200))
            };
        }

        [TestCaseSource(nameof(HistoricalTradeBarsTestCases))]
        [Explicit("This tests require a Polygon.io api key, requires internet and are long.")]
        public void GetHistoricalTradeBarsTest(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period)
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
            var newHistoryProvider = new PolygonDataQueueHandler();

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
            // And hour of data per api call
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

            var historyProvider = new TestPolygonHistoryProvider();
            historyProvider.ResponseLimit = responseLimit;
            historyProvider.SetHistoryRequest(request);

            var history = historyProvider.GetHistory(request).ToList();

            Log.Trace("Data points retrieved: " + _historyProvider.DataPointCount);

            Assert.That(history, Has.Count.EqualTo((end - start).TotalMinutes));
            Assert.That(historyProvider.ApiCallsCount, Is.EqualTo((end - start).TotalMinutes / responseLimit));
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
    }
}
