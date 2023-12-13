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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Lean.Engine.DataFeeds.Enumerators;
using QuantConnect.Logging;
using QuantConnect.Polygon;
using QuantConnect.Util;

namespace QuantConnect.Tests.Polygon
{
    [TestFixture]
    [Explicit("Tests are dependent on network and take long")]
    public class PolygonDataQueueHandlerOptionsTests : PolygonDataQueueHandlerBaseTests
    {
        [Test]
        public void StressTest()
        {
            const int maxSubscriptions = 1000;

            using var polygon = new PolygonDataQueueHandler(ApiKey, maxSubscriptions);
            var optionChainProvider = new LiveOptionChainProvider(TestGlobals.DataCacheProvider, TestGlobals.MapFileProvider);

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

        /// <summary>
        /// The subscription data configs to be used in the tests. At least 2 configs are required
        /// </summary>
        /// <remarks>
        /// In order to successfully run the tests, valid contracts should be used. Update them
        /// </remarks>
        protected override List<SubscriptionDataConfig> GetConfigs()
        {
            var spyOptions = new[] { 463m, 464m, 465m, 466m, 467m }.Select(strike => GetSubscriptionDataConfig<TradeBar>(
                Symbol.CreateOption(
                    Symbols.SPY,
                    Market.USA,
                    OptionStyle.American,
                    OptionRight.Call,
                    strike,
                    new DateTime(2023, 12, 18)),
                Resolution.Minute));

            var aaplOptions = new[] { 195m, 197.5m, 200m, 202.5m, 205m }.Select(strike => GetSubscriptionDataConfig<TradeBar>(
                Symbol.CreateOption(
                    Symbols.AAPL,
                    Market.USA,
                    OptionStyle.American,
                    OptionRight.Call,
                    strike,
                    new DateTime(2023, 12, 15)),
                Resolution.Minute));

            return spyOptions.Concat(aaplOptions).ToList();
        }
    }
}