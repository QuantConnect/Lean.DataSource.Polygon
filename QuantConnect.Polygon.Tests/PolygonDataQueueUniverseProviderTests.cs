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
using QuantConnect.Util;
using QuantConnect.Configuration;
using System;
using System.Linq;
using QuantConnect.Lean.Engine.DataFeeds;
using System.Collections.Generic;

namespace QuantConnect.Lean.DataSource.Polygon.Tests
{
    [TestFixture]
    public class PolygonDataQueueUniverseProviderTests
    {
        private readonly string _apiKey = Config.Get("polygon-api-key");
        private TestablePolygonDataProvider _polygon;

        [SetUp]
        public void SetUp()
        {
            _polygon = new TestablePolygonDataProvider(_apiKey);
        }

        [TearDown]
        public void TeadDown()
        {
            _polygon.DisposeSafely();
        }

        private static Symbol[] OptionChainTestCases =>
            new[]
            {
                Symbol.Create("SPY", SecurityType.Equity, Market.USA),
                Symbol.Create("SPX", SecurityType.Index, Market.USA),
            }
            .Select(underlying => new[] { underlying, Symbol.CreateCanonicalOption(underlying) })
            .SelectMany(x => x)
            .ToArray();

        [TestCaseSource(nameof(OptionChainTestCases))]
        [Explicit("This tests require a Polygon.io api key, requires internet and are long.")]
        public void GetsOptionChain(Symbol symbol)
        {
            var date = new DateTime(2014, 10, 7);
            _polygon.TimeProviderInstance.SetCurrentTimeUtc(date);
            var optionChain = _polygon.LookupSymbols(symbol, true).ToList();

            Assert.That(optionChain, Is.Not.Null.And.Not.Empty);

            var expectedOptionType = symbol.SecurityType;
            if (!expectedOptionType.IsOption())
            {
                expectedOptionType = expectedOptionType == SecurityType.Equity ? SecurityType.Option : SecurityType.IndexOption;
            }
            Assert.IsTrue(optionChain.All(x => x.SecurityType == expectedOptionType));
            Assert.IsTrue(optionChain.All(x => x.ID.Date.Date >= date));
        }

        private class TestablePolygonDataProvider : PolygonDataProvider
        {
            public ManualTimeProvider TimeProviderInstance = new ManualTimeProvider(DateTime.UtcNow);

            protected override ITimeProvider TimeProvider => TimeProviderInstance;

            public TestablePolygonDataProvider(string apiKey)
                : base(apiKey, streamingEnabled: false)
            {
            }

            protected override List<ExchangeMapping> FetchExchangeMappings()
            {
                return new List<ExchangeMapping>();
            }
        }
    }
}
