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
using QuantConnect.Lean.Engine.DataFeeds;
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
                //Symbols.SPX,
                Symbol.CreateOption(Symbols.SPX, "SPXW", Symbols.SPX.ID.Market, SecurityType.IndexOption.DefaultOptionStyle(), OptionRight.Call, 6485m, new(2025, 08, 29))
            };

            return [.. symbols.SelectMany(symbol => GetSubscriptionDataConfigs(symbol, resolution))];
        }

        [TestCase(Resolution.Tick, 10)]
        [TestCase(Resolution.Second, 5)]
        [TestCase(Resolution.Minute, 1)]
        public void StreamDataOnDifferentSecuritiesTypes(Resolution resolution, int expectedReceiveAmountData)
        {
            var mockDateTimeAfterOpenExchange = DateTime.UtcNow.Date.AddHours(9).AddMinutes(30).AddSeconds(59).ConvertToUtc(TimeZones.NewYork);
            TestablePolygonDataProvider.TimeProviderInstance = new ManualTimeProvider(mockDateTimeAfterOpenExchange);
            using var polygon = new TestablePolygonDataProvider(Config.Get("polygon-api-key"), Config.Get("polygon-license-type"));

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

                            if ((bd is Tick t && t.TickType == TickType.OpenInterest) || bd is OpenInterest)
                            {
                                // Prevent from repeating request
                                TestablePolygonDataProvider.TimeProviderInstance.SetCurrentTimeUtc(DateTime.UtcNow);
                            }

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

            foreach (var (symbol, baseData) in receivedData.Where(x => x.Key.SecurityType.IsOption()))
            {
                var openInterestDetected = default(bool);
                foreach (var data in baseData)
                {
                    if (data is Tick t && t.TickType == TickType.OpenInterest)
                    {
                        openInterestDetected = true;
                        break;
                    }
                }
                Assert.IsTrue(openInterestDetected);
            }

            foreach (var (symbol, baseData) in receivedData)
            {
                foreach (var data in baseData)
                {
                    switch (data)
                    {
                        case OpenInterest oi:
                            Assert.Greater(oi.Value, 0m);
                            break;
                        case Tick t:
                            switch (t.TickType)
                            {
                                case TickType.OpenInterest:
                                    Assert.Greater(t.Value, 0m);
                                    break;
                                case TickType.Trade:
                                    Assert.Greater(t.LastPrice, 0m);
                                    Assert.AreEqual(t.Quantity, 0m);
                                    break;
                                default:
                                    throw new NotSupportedException();
                            }
                            break;
                        case TradeBar tb:
                            Assert.Greater(tb.Open, 0m);
                            Assert.Greater(tb.High, 0m);
                            Assert.Greater(tb.Low, 0m);
                            Assert.Greater(tb.Close, 0m);
                            if (resolution >= Resolution.Minute)
                            {
                                // For 1-minute or higher aggregates, volume is expected (may be zero).
                                // Example: {"ev":"AM","sym":"O:MSFT250829C00502500","v":0,"av":358,...}
                                Assert.GreaterOrEqual(tb.Volume, 0m);
                            }
                            else
                            {
                                // For tick/fmv data, volume is not reported and should remain zero
                                Assert.AreEqual(tb.Volume, 0m);
                            }
                            break;
                        default:
                            throw new NotSupportedException();
                    }
                }
            }
        }

        private static List<SubscriptionDataConfig> GetSubscriptionDataConfigs(Symbol symbol, Resolution resolution)
        {
            var subs = new List<SubscriptionDataConfig>();
            if (resolution == Resolution.Tick)
            {
                subs.Add(new SubscriptionDataConfig(PolygonDataProviderBaseTests.GetSubscriptionDataConfig<Tick>(symbol, resolution), tickType: TickType.Trade));

                if (symbol.SecurityType.IsOption())
                {
                    subs.Add(new SubscriptionDataConfig(PolygonDataProviderBaseTests.GetSubscriptionDataConfig<Tick>(symbol, resolution), tickType: TickType.OpenInterest));
                }

                return subs;
            }

            subs.Add(PolygonDataProviderBaseTests.GetSubscriptionDataConfig<TradeBar>(symbol, resolution));

            if (symbol.SecurityType.IsOption())
            {
                subs.Add(PolygonDataProviderBaseTests.GetSubscriptionDataConfig<OpenInterest>(symbol, resolution));
            }

            return subs;
        }
    }
}
