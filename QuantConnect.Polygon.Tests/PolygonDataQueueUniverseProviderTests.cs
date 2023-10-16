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
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Interfaces;

namespace QuantConnect.Tests.Polygon
{
    [TestFixture]
    public class PolygonDataQueueUniverseProviderTests
    {
        private readonly string _apiKey = Config.Get("polygon-api-key");
        private TestablePolygonDataQueueHandler _polygon;

        [SetUp]
        public void SetUp()
        {
            var optionChainProvider = new BacktestingOptionChainProvider(TestGlobals.DataCacheProvider, TestGlobals.MapFileProvider);
            Composer.Instance.AddPart<IOptionChainProvider>(optionChainProvider);

            _polygon = new TestablePolygonDataQueueHandler(_apiKey);
        }

        [TearDown]
        public void TeadDown()
        {
            _polygon.DisposeSafely();
        }

        [Test]
        public void UsesOptionChainProviderFromComposer()
        {
            var date = new DateTime(2014, 10, 7);
            _polygon.TimeProviderInstance.SetCurrentTimeUtc(date);
            var optionChain = _polygon.LookupSymbols(Symbol.CreateCanonicalOption(Symbols.AAPL), true).ToList();

            Assert.That(optionChain, Is.Not.Empty);
            Assert.IsTrue(optionChain.All(x => x.SecurityType == SecurityType.Option));
            Assert.IsTrue(optionChain.All(x => x.ID.Symbol == "AAPL"));
            Assert.IsTrue(optionChain.All(x => x.Underlying == Symbols.AAPL));
            Assert.IsTrue(optionChain.All(x => x.ID.Date.Date >= date));
        }

        private class TestablePolygonDataQueueHandler : PolygonDataQueueHandler
        {
            public ManualTimeProvider TimeProviderInstance = new ManualTimeProvider();

            protected override ITimeProvider TimeProvider => TimeProviderInstance;

            public TestablePolygonDataQueueHandler(string apiKey)
                : base(apiKey, streamingEnabled: false)
            {
            }
        }
    }
}