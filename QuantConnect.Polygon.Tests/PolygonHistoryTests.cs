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
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using QuantConnect.Polygon;
using QuantConnect.Securities;
using QuantConnect.Util;

namespace QuantConnect.Tests.Polygon
{
    [TestFixture]
    [Explicit("Tests require a Polygon.io api key.")]
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

        private static TestCaseData[] HistoricalTradeBarsTestCaseDatas => new[]
        {
            // long requests
            new TestCaseData(Symbol.CreateOption(Symbols.SPY, Market.USA, OptionStyle.American, OptionRight.Call, 429m, new DateTime(2023, 10, 06)),
                Resolution.Minute, TickType.Trade, TimeSpan.FromDays(100), true),
            new TestCaseData(Symbol.CreateOption(Symbols.SPY, Market.USA, OptionStyle.American, OptionRight.Call, 429m, new DateTime(2023, 10, 06)),
                Resolution.Minute, TickType.Trade, TimeSpan.FromDays(200), true)
        };

        [TestCaseSource(nameof(HistoricalTradeBarsTestCaseDatas))]
        public void GetHistoricalTradeBarsTest(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period, bool isNonEmptyResult)
        {
            var request = CreateHistoryRequest(symbol, resolution, tickType, period);
            var history = _historyProvider.GetHistory(request).ToList();

            Log.Trace("Data points retrieved: " + _historyProvider.DataPointCount);

            Assert.That(history, Is.Not.Empty);

            if (isNonEmptyResult)
            {
                var i = -1;
                foreach (var baseData in history)
                {
                    var bar = (TradeBar)baseData;
                    Log.Trace($"{++i} {bar.Time}: {bar.Symbol} - O={bar.Open}, H={bar.High}, L={bar.Low}, C={bar.Close}");
                }

                // Ordered by time
                Assert.That(history, Is.Ordered.By("Time"));

                // No repeating bars
                var timesArray = history.Select(x => x.Time).ToList();
                Assert.That(timesArray.Distinct().Count(), Is.EqualTo(timesArray.Count));
            }
        }

        private static HistoryRequest CreateHistoryRequest(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period)
        {
            var now = DateTime.UtcNow;
            var dataType = LeanData.GetDataType(resolution, tickType);

            return new HistoryRequest(now.Add(-period),
                now,
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
    }
}
