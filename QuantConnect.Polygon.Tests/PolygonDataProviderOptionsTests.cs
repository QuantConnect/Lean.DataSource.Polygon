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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Lean.Engine.DataFeeds.Enumerators;
using QuantConnect.Logging;
using QuantConnect.Tests;
using QuantConnect.Util;

namespace QuantConnect.Lean.DataSource.Polygon.Tests
{
    [TestFixture]
    [Explicit("Tests are dependent on network and take long")]
    public class PolygonDataProviderOptionsTests : PolygonDataProviderBaseTests
    {
        [Test]
        public void StressTest()
        {
            const int maxSubscriptions = 1000;

            using var polygon = new PolygonDataProvider(ApiKey, maxSubscriptions);
            var optionChainProvider = new LiveOptionChainProvider();

            var underlyingTickers = new[] { "SPY", "AAPL", "GOOG", "IBM" };
            var subscriptionsCount = 0;

            foreach (var underlyingTicker in underlyingTickers)
            {
                var underlyingSymbol = Symbol.Create(underlyingTicker, SecurityType.Equity, Market.USA);
                var optionChain = optionChainProvider.GetOptionContractList(underlyingSymbol, DateTime.UtcNow);

                foreach (var optionSymbol in optionChain)
                {
                    var config = GetSubscriptionDataConfig<TradeBar>(
                        Symbol.CreateOption(
                            underlyingSymbol,
                            Market.USA,
                            OptionStyle.American,
                            optionSymbol.ID.OptionRight,
                            optionSymbol.ID.StrikePrice,
                            optionSymbol.ID.Date),
                        Resolution.Minute);

                    Assert.DoesNotThrow(() => polygon.Subscribe(config, (sender, args) =>
                    {
                        var dataPoint = ((NewDataAvailableEventArgs)args).DataPoint;
                        Log.Trace(dataPoint.ToString());
                    }));

                    if (++subscriptionsCount >= maxSubscriptions)
                    {
                        Log.Trace($"Subscribed to {subscriptionsCount} options");
                        break;
                    }
                }

                if (subscriptionsCount >= maxSubscriptions)
                {
                    break;
                }
            }

            Thread.Sleep(1000 * 60 * 3);

            polygon.DisposeSafely();
        }

        [Test]
        public void WeeklyIndexOptionsStreamDataSymbolIsCorrectlyMapped()
        {
            using var polygon = new PolygonDataProvider(ApiKey);

            var configs = new[] { 4675m, 4680m, 4685m, 4690m, 4695m }
                .Select(strike =>
                {
                    var symbol = Symbol.CreateOption(Symbols.SPX, "SPXW", Market.USA, OptionStyle.American, OptionRight.Call, strike,
                        new DateTime(2024, 01, 05));
                    return new[]
                    {
                        GetSubscriptionDataConfig<TradeBar>(symbol, Resolution.Tick),
                        GetSubscriptionDataConfig<QuoteBar>(symbol, Resolution.Tick)
                    };
                })
                .SelectMany(x => x);

            using var receivedData = new BlockingCollection<BaseData>();
            foreach (var config in configs)
            {
                polygon.Subscribe(config, (sender, args) =>
                {
                    var dataPoint = (BaseData)((NewDataAvailableEventArgs)args).DataPoint;
                    Log.Trace($"{dataPoint}. Time span: {dataPoint.Time} - {dataPoint.EndTime}");

                    receivedData.Add(dataPoint);
                });
            }

            // Checking the symbol for one of the data points is enough
            var data = receivedData.Take();

            Log.Trace("Unsubscribing symbols");
            foreach (var config in configs)
            {
                polygon.Unsubscribe(config);
            }

            // Some messages could be inflight
            Thread.Sleep(2 * 1000);

            Assert.That(data.Symbol.SecurityType, Is.EqualTo(SecurityType.IndexOption));
            Assert.That(data.Symbol.Underlying, Is.EqualTo(Symbols.SPX));
            Assert.That(data.Symbol.ID.Symbol, Is.EqualTo("SPXW"));
        }

        /// <summary>
        /// The subscription data configs to be used in the tests. At least 2 configs are required
        /// </summary>
        /// <remarks>
        /// In order to successfully run the tests, valid contracts should be used. Update them
        /// </remarks>
        protected override List<SubscriptionDataConfig> GetConfigs(Resolution resolution = Resolution.Second)
        {
            var spyOptions = new[] { 465m, 466m, 467m, 468m, 469m }
                .Select(strike =>
                {
                    var symbol = Symbol.CreateOption(Symbols.SPY, Market.USA, OptionStyle.American, OptionRight.Call, strike,
                        new DateTime(2024, 09, 20));
                    return new[]
                    {
                        GetSubscriptionDataConfig<TradeBar>(symbol, resolution),
                        GetSubscriptionDataConfig<QuoteBar>(symbol, resolution),
                        GetSubscriptionDataConfig<OpenInterest>(symbol, resolution)
                    };
                })
                .SelectMany(x => x);

            var spxOptions = new[] { 4675m, 4680m, 4685m, 4690m, 4695m }
                .Select(strike =>
                {
                    var symbol = Symbol.CreateOption(Symbols.SPX, "SPXW", Market.USA, OptionStyle.American, OptionRight.Call, strike,
                        new DateTime(2024, 09, 20));
                    return new[]
                    {
                        GetSubscriptionDataConfig<TradeBar>(symbol, resolution),
                        GetSubscriptionDataConfig<QuoteBar>(symbol, resolution),
                        GetSubscriptionDataConfig<OpenInterest>(symbol, resolution)
                    };
                })
                .SelectMany(x => x);

            return spyOptions.Concat(spxOptions).ToList();
        }
    }
}