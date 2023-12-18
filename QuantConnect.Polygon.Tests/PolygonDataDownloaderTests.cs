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
using QuantConnect.Util;
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
    }
}
