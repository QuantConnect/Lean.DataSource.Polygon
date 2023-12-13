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
using QuantConnect.Logging;
using QuantConnect.Polygon;
using QuantConnect.Securities;
using QuantConnect.Util;
using Microsoft.CodeAnalysis;
using System;
using System.Linq;

namespace QuantConnect.Tests.Polygon
{
    [TestFixture]
    public class PolygonDataDownloaderTests
    {
        private PolygonDataDownloader _downloader;

        [SetUp]
        public void SetUp()
        {
            Log.LogHandler = new CompositeLogHandler();

            _downloader = new PolygonDataDownloader();

        }

        private static TestCaseData[] HistoricalTradeBarsTestCases()
        {
            var equitySymbol = Symbols.SPY;
            var optionSymbol = Symbol.CreateOption(Symbols.SPY, Market.USA, OptionStyle.American, OptionRight.Call, 429m, new DateTime(2023, 10, 06));

            return new[]
            {
                // Equity
                new TestCaseData(equitySymbol, Resolution.Minute, TickType.Trade, TimeSpan.FromDays(100)),
                new TestCaseData(equitySymbol, Resolution.Minute, TickType.Trade, TimeSpan.FromDays(200)),
                new TestCaseData(equitySymbol, Resolution.Hour, TickType.Trade, TimeSpan.FromDays(365)),
                new TestCaseData(equitySymbol, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(3650)),

                // Options
                new TestCaseData(optionSymbol, Resolution.Minute, TickType.Trade, TimeSpan.FromDays(100)),
                new TestCaseData(optionSymbol, Resolution.Minute, TickType.Trade, TimeSpan.FromDays(200)),
                new TestCaseData(optionSymbol, Resolution.Hour, TickType.Trade, TimeSpan.FromDays(365)),
                new TestCaseData(optionSymbol, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(3650))
            };
        }

        [TestCaseSource(nameof(HistoricalTradeBarsTestCases))]
        [Explicit("This tests require a Polygon.io api key, requires internet and are long.")]
        public void DownloadsHistoricalData(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period)
        {
            var end = new DateTime(2023, 10, 6);
            var start = end.Subtract(period);
            var parameters = new DataDownloaderGetParameters(symbol, resolution, start, end, tickType);
            var data = _downloader.Get(parameters).ToList();

            Log.Trace("Data points retrieved: " + data.Count);

            Assert.That(data, Is.Not.Empty);

            // Ordered by time
            Assert.That(data, Is.Ordered.By("Time"));

            // No repeating bars
            var timesArray = data.Select(x => x.Time).ToList();
            Assert.That(timesArray.Distinct().Count(), Is.EqualTo(timesArray.Count));
        }
    }
}
