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

using Moq;
using System;
using RestSharp;
using System.Linq;
using NUnit.Framework;
using QuantConnect.Logging;
using QuantConnect.Configuration;
using System.Collections.Generic;

namespace QuantConnect.Lean.DataSource.Polygon.Tests
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
                ("GOOGL", SecurityType.Equity),
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

        private List<Symbol> GetOptionChain(Symbol symbol, DateTime? reference = null)
        {
            var referenceDate = reference ?? new DateTime(2024, 01, 03);
            var optionChain = _optionChainProvider.GetOptionContractList(symbol, referenceDate).ToList();

            Assert.That(optionChain, Is.Not.Null.And.Not.Empty);

            // Multiple strikes
            var strikes = optionChain.Select(x => x.ID.StrikePrice).Distinct().ToList();
            Assert.That(strikes, Has.Count.GreaterThan(1).And.All.GreaterThan(0));

            // Multiple expirations
            var expirations = optionChain.Select(x => x.ID.Date).Distinct().ToList();
            Assert.That(expirations, Has.Count.GreaterThan(1).And.All.GreaterThanOrEqualTo(referenceDate.Date));

            // All contracts have the same underlying
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

        [Test]
        public void GetsFullSPXOptionChain()
        {
            var chain = GetOptionChain(Symbol.Create("SPX", SecurityType.Index, Market.USA), new DateTime(2024, 03, 15));

            // SPX has a lot of options, let's make sure we get more than 1000 contracts (which is the pagination limit)
            // to assert that multiple requests are being made.
            // In fact, we expect to get more than 20000 contracts for this date.
            Assert.That(chain, Has.Count.GreaterThan(20000));

            // Make sure we have both SPX and SPXW contracts:

            var spxw = chain.Where(x => x.ID.Symbol == "SPXW").ToList();
            Assert.That(spxw, Is.Not.Empty);

            var spx = chain.Where(x => x.ID.Symbol == "SPX").ToList();
            Assert.That(spx, Is.Not.Empty);

            Assert.That(spxw.Count + spx.Count, Is.EqualTo(chain.Count));
        }

        [TestCaseSource(nameof(Underlyings))]
        public void ValidateQueryParameterToSpecificSymbolValue(Symbol underlyingSymbol)
        {
            IRestRequest request = default;
            var mock = new Mock<PolygonRestApiClient>("api-key");

            mock.Setup(m => m.DownloadAndParseData<OptionChainResponse>(It.IsAny<RestRequest>()))
                .Callback((RestRequest r) => request = r);

            var optionChainProvider = new PolygonOptionChainProvider(mock.Object, new PolygonSymbolMapper());

            var expiryDate = new DateTime(2024, 03, 15);
            var option = Symbol.CreateOption(underlyingSymbol, Market.USA, OptionStyle.American, OptionRight.Call, 1000m, expiryDate);

            var optionContracts = optionChainProvider.GetOptionContractList(option, expiryDate).ToList();

            Assert.IsNotNull(optionContracts);
            Assert.IsTrue(request.Parameters[0].Value.ToString().EndsWith(option.Underlying.Value));
        }

        [TestCaseSource(nameof(Underlyings))]
        public void ValidateGetOptionContractsReturnsAppropriateSymbol(Symbol underlyingSymbol)
        {
            var referenceDate = new DateTime(2024, 03, 15);
            var optionContracts = _optionChainProvider.GetOptionContractList(underlyingSymbol, referenceDate).ToList();

            foreach (var optionContract in optionContracts)
            {
                Assert.That(optionContract.Underlying, Is.EqualTo(underlyingSymbol));
            }
        }
    }
}