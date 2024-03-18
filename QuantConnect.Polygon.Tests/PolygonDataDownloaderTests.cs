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
using QuantConnect.Logging;
using QuantConnect.Util;
using System;
using System.Collections.Generic;
using System.Linq;

namespace QuantConnect.Lean.DataSource.Polygon.Tests
{
    [TestFixture]
    public class PolygonDataDownloaderTests
    {
        private PolygonDataDownloader _downloader;

        [SetUp]
        public void SetUp()
        {
            _downloader = new PolygonDataDownloader();
        }

        [TearDown]
        public void TearDown()
        {
            _downloader.DisposeSafely();
        }

        private static TestCaseData[] HistoricalDataTestCases => PolygonHistoryTests.HistoricalDataTestCases;

        [TestCaseSource(nameof(HistoricalDataTestCases))]
        [Explicit("This tests require a Polygon.io api key, requires internet and are long.")]
        public void DownloadsHistoricalData(Symbol symbol, Resolution resolution, TimeSpan period, TickType tickType)
        {
            var request = PolygonHistoryTests.CreateHistoryRequest(symbol, resolution, tickType, period);

            var parameters = new DataDownloaderGetParameters(symbol, resolution, request.StartTimeUtc, request.EndTimeUtc, tickType);
            var data = _downloader.Get(parameters).ToList();

            Log.Trace("Data points retrieved: " + data.Count);

            PolygonHistoryTests.AssertHistoricalDataResults(data, resolution);
        }

        private static TestCaseData[] IndexHistoricalDataTestCases => PolygonHistoryTests.IndexHistoricalDataTestCases;

        [TestCaseSource(nameof(IndexHistoricalDataTestCases))]
        [Explicit("This tests require a Polygon.io api key, requires internet and are long.")]
        public void DownloadsIndexHistoricalData(Resolution resolution, TimeSpan period, TickType tickType, bool shouldBeEmpty)
        {
            var data = DownloadIndexHistoryData(resolution, period, tickType);

            Log.Trace("Data points retrieved: " + data.Count);

            if (shouldBeEmpty)
            {
                Assert.That(data, Is.Empty);
            }
            else
            {
                PolygonHistoryTests.AssertHistoricalDataResults(data, resolution);
            }
        }

        private static TestCaseData[] IndexHistoricalInvalidDataTestCases => PolygonHistoryTests.IndexHistoricalInvalidDataTestCases;

        [TestCaseSource(nameof(IndexHistoricalInvalidDataTestCases))]
        [Explicit("This tests require a Polygon.io api key, requires internet and are long.")]
        public void DownloadsIndexInvalidHistoricalData(Resolution resolution, TimeSpan period, TickType tickType)
        {
            var data = DownloadIndexHistoryData(resolution, period, tickType);

            Assert.IsNull(data);
        }

        [Test]
        [Explicit("This tests require a Polygon.io api key, requires internet and are long.")]
        public void DownloadsDataFromCanonicalOptionSymbol()
        {
            var symbol = Symbol.CreateCanonicalOption(Symbol.Create("SPY", SecurityType.Equity, Market.USA));
            var parameters = new DataDownloaderGetParameters(symbol, Resolution.Hour,
                new DateTime(2024, 02, 22), new DateTime(2024, 02, 23), TickType.Trade);
            using var downloader = new TestablePolygonDataDownloader();
            var data = downloader.Get(parameters)?.ToList();

            Log.Trace("Data points retrieved: " + data.Count);

            Assert.That(data, Is.Not.Null.And.Not.Empty);

            // Multiple symbols
            var distinctSymbols = data.Select(x => x.Symbol).Distinct().ToList();
            Assert.That(distinctSymbols, Has.Count.GreaterThan(1).And.All.Matches<Symbol>(x => x.Canonical == symbol));
        }

        /// <summary>
        /// Downloads historical data of an hardcoded index [SPX] based on specified parameters.
        /// </summary>
        /// <param name="resolution">The resolution of the historical data to download.</param>
        /// <param name="period">The time period for which historical data is requested.</param>
        /// <param name="tickType">The type of ticks for the historical data.</param>
        /// <returns>A list of <see cref="BaseData"/> containing downloaded historical data of the index.</returns>
        /// <remarks>
        /// The <paramref name="resolution"/> parameter determines the granularity of the historical data, 
        /// while the <paramref name="period"/> parameter specifies the duration of the historical data to be downloaded.
        /// The <paramref name="tickType"/> parameter specifies the type of ticks to be included in the historical data.
        /// </remarks>
        private List<BaseData> DownloadIndexHistoryData(Resolution resolution, TimeSpan period, TickType tickType)
        {
            var symbol = Symbol.Create("SPX", SecurityType.Index, Market.USA);
            var request = PolygonHistoryTests.CreateHistoryRequest(symbol, resolution, tickType, period);

            var parameters = new DataDownloaderGetParameters(symbol, resolution, request.StartTimeUtc, request.EndTimeUtc, tickType);
            return _downloader.Get(parameters)?.ToList();
        }

        private class TestablePolygonDataDownloader : PolygonDataDownloader
        {
            protected override IEnumerable<Symbol> GetOptions(Symbol symbol, DateTime startUtc, DateTime endUtc)
            {
                // Let's only take a few contracts from a few days to speed up the test
                return base.GetOptions(symbol, startUtc, endUtc)
                    .GroupBy(x => x.ID.Date)
                    .OrderBy(x => x.Key)
                    .Select(x => x.Take(50))
                    .Take(5)
                    .SelectMany(x => x);
            }
        }
    }
}
