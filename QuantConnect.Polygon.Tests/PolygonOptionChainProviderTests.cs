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
 *
*/

using NUnit.Framework;
using QuantConnect.Configuration;
using QuantConnect.Logging;
using QuantConnect.Polygon;
using System;
using System.Collections.Generic;
using System.Linq;

namespace QuantConnect.Tests.Polygon
{
    [TestFixture]
    [Explicit("Requires Polygon API key and depends on internet connection")]
    public class PolygonOptionChainProviderTests
    {
        private readonly string _apiKey = Config.Get("polygon-api-key");

        private PolygonRestApiClient _restApiClient;
        private PolygonOptionChainProvider _optionChainProvider;

        [OneTimeSetUp]
        public void SetUp()
        {
            _restApiClient = new PolygonRestApiClient(_apiKey);
            _optionChainProvider = new PolygonOptionChainProvider(_restApiClient, new PolygonSymbolMapper());
        }

        [OneTimeTearDown]
        public void TearDown()
        {
            _restApiClient.Dispose();
        }

        private static Symbol[] Underlyings =>
            new (string Ticker, SecurityType SecurityType)[]
            {
                ("SPY", SecurityType.Equity),
                ("AAPL", SecurityType.Equity),
                ("IBM", SecurityType.Equity),
                ("GOOG", SecurityType.Equity),
                ("SPX", SecurityType.Index),
                ("VIX", SecurityType.Index),
                ("DAX", SecurityType.Index),
            }
            .Select(t => Symbol.Create(t.Ticker, t.SecurityType, Market.USA) )
            .ToArray();

        [TestCaseSource(nameof(Underlyings))]
        public void GetsOptionChainGivenTheUnderlyingSymbol(Symbol underlying)
        {
            GetOptionChain(underlying);
        }

        private List<Symbol> GetOptionChain(Symbol symbol)
        {
            var reference = new DateTime(2024, 01, 03);
            var optionChain = _optionChainProvider.GetOptionContractList(symbol, reference).ToList();

            Assert.That(optionChain, Is.Not.Null.And.Not.Empty);
            Assert.That(optionChain.Select(x => x.ID.StrikePrice), Is.All.GreaterThan(0));
            Assert.That(optionChain.Select(x => x.ID.Date), Is.All.GreaterThanOrEqualTo(reference.Date));

            var underlying = symbol.Underlying ?? symbol;
            Assert.That(optionChain.Select(x => x.Underlying), Is.All.EqualTo(underlying));

            Log.Trace($"Option chain for {symbol} contains {optionChain.Count} contracts");

            return optionChain;
        }

        private static Symbol[] Canonicals =>
            Underlyings
            .Select(underlying => Symbol.CreateCanonicalOption(underlying))
            .ToArray();

        [TestCaseSource(nameof(Canonicals))]
        public void GetsOptionChainGivenTheOptionSymbol(Symbol option)
        {
            GetOptionChain(option);
        }
    }
}