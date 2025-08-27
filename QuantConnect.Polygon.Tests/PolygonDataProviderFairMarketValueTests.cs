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
using System.Linq;
using NUnit.Framework;
using System.Threading;
using QuantConnect.Data;
using QuantConnect.Tests;
using QuantConnect.Logging;
using QuantConnect.Data.Market;
using QuantConnect.Configuration;
using System.Collections.Generic;
using QuantConnect.Lean.Engine.DataFeeds.Enumerators;

namespace QuantConnect.Lean.DataSource.Polygon.Tests
{
    [TestFixture]
    [Explicit("Tests are dependent on network and take long")]
    public class PolygonDataProviderFairMarketValueTests
    {
        private List<SubscriptionDataConfig> GetConfigs(Resolution resolution)
        {
            var symbols = new Symbol[]
            {
                Symbols.AAPL,
                Symbols.MSFT,
                Symbol.CreateOption(Symbols.AAPL, Symbols.AAPL.ID.Market, SecurityType.Option.DefaultOptionStyle(), OptionRight.Call, 227.5m, new(2025, 08, 29)),
                Symbol.CreateOption(Symbols.MSFT, Symbols.MSFT.ID.Market, SecurityType.Option.DefaultOptionStyle(), OptionRight.Call, 502.5m, new(2025, 08, 29)),
                Symbols.SPX,
                Symbol.CreateOption(Symbols.SPX, Symbols.SPX.ID.Market, SecurityType.IndexOption.DefaultOptionStyle(), OptionRight.Call, 6475m, new(2025, 08, 27))
            };

            return [.. symbols.Select(symbol => GetSubscriptionDataConfigs(symbol, resolution))];
        }

        [TestCase(Resolution.Tick, 10)]
        [TestCase(Resolution.Second, 5)]
        [TestCase(Resolution.Minute, 1)]
        public void StreamDataOnDifferentSecuritiesTypes(Resolution resolution, int expectedReceiveAmountData)
        {
            Config.Set("polygon-ws-url", "wss://business.polygon.io");
            using var polygon = new PolygonDataProvider(Config.Get("polygon-api-key"));

            var configs = GetConfigs(resolution);

            var receivedData = new Dictionary<Symbol, List<BaseData>>();

            var resetEvent = new AutoResetEvent(false);
            var receivedAllData = default(bool);

            foreach (var config in configs)
            {
                receivedData[config.Symbol] = [];

                polygon.Subscribe(config, (sender, args) =>
                {
                    if (args is NewDataAvailableEventArgs newData && newData.DataPoint is BaseData bd)
                    {
                        Log.Trace($"{bd}. Time span: {bd.Time} - {bd.EndTime}");
                        lock (receivedData)
                        {
                            receivedData[bd.Symbol].Add(bd);

                            if (!receivedAllData && receivedData.Values.All(x => x.Count >= expectedReceiveAmountData))
                            {
                                receivedAllData = true;
                                resetEvent.Set();
                            }
                        }
                    }
                });
            }

            Assert.IsTrue(resetEvent.WaitOne(TimeSpan.FromSeconds(120)));

            Log.Trace("Unsubscribing symbols");
            foreach (var config in configs)
            {
                polygon.Unsubscribe(config);
            }

            resetEvent.WaitOne(TimeSpan.FromSeconds(10));

            var dataSummary = receivedData.ToDictionary(pair => pair.Key, pair => pair.Value.Count);
            Log.Trace($"Received {receivedData.Count} symbols with data points -> {string.Join(", ", dataSummary.Select(kvp => $"{kvp.Key}: {kvp.Value}"))}");

            Assert.IsTrue(receivedData.Values.All(d => d.Count >= expectedReceiveAmountData));

            foreach (var (symbol, baseData) in receivedData)
            {
                foreach (var data in baseData)
                {
                    Assert.IsInstanceOf(resolution == Resolution.Tick ? typeof(Tick) : typeof(TradeBar), data);
                    switch (data)
                    {
                        case Tick t:
                            Assert.Greater(t.LastPrice, 0m);
                            Assert.AreEqual(t.Quantity, 0m);
                            break;
                        case TradeBar tb:
                            Assert.Greater(tb.Open, 0m);
                            Assert.Greater(tb.High, 0m);
                            Assert.Greater(tb.Low, 0m);
                            Assert.Greater(tb.Close, 0m);
                            Assert.AreEqual(tb.Volume, 0m);
                            break;
                        default:
                            throw new NotSupportedException();
                    }
                }
            }
        }

        private static SubscriptionDataConfig GetSubscriptionDataConfigs(Symbol symbol, Resolution resolution)
        {
            return resolution == Resolution.Tick
                ? new SubscriptionDataConfig(PolygonDataProviderBaseTests.GetSubscriptionDataConfig<Tick>(symbol, resolution), tickType: TickType.Trade)
                : PolygonDataProviderBaseTests.GetSubscriptionDataConfig<TradeBar>(symbol, resolution);
        }
    }
}
