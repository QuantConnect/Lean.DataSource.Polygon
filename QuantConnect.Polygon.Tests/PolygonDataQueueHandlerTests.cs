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
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Lean.Engine.DataFeeds.Enumerators;
using QuantConnect.Logging;
using QuantConnect.Polygon;
using QuantConnect.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace QuantConnect.Tests.Polygon
{
    [TestFixture]
    [Explicit("Tests are dependent on network and take long")]
    public class PolygonDataQueueHandlerTests
    {
        private string _apiKey = Config.Get("polygon-api-key");

        [SetUp]
        public void Setup()
        {
            Log.DebuggingEnabled = Config.GetBool("debug-mode");
            Log.LogHandler = new CompositeLogHandler();
        }

        [Test]
        public void CanSubscribeAndUnsubscribe()
        {
            using var polygon = new PolygonDataQueueHandler(_apiKey);
            var unsubscribed = false;

            var configs = GetConfigs().GroupBy(x => x.Symbol.Underlying, (key, group) => group.First());

            var dataFromEnumerator = new List<TradeBar>();
            var dataFromEventHandler = new List<TradeBar>();

            Action<BaseData> callback = (dataPoint) =>
            {
                if (dataPoint == null)
                {
                    return;
                }

                dataFromEnumerator.Add((TradeBar)dataPoint);

                if (unsubscribed && dataPoint.Symbol.Value == "AAPL")
                {
                    Assert.Fail("Should not receive data for unsubscribed symbol");
                }
            };

            foreach (var config in configs)
            {
                ProcessFeed(polygon.Subscribe(config, (sender, args) =>
                {
                    var dataPoint = ((NewDataAvailableEventArgs)args).DataPoint;
                    dataFromEventHandler.Add((TradeBar)dataPoint);
                    Log.Trace($"{dataPoint}. Time span: {dataPoint.Time} - {dataPoint.EndTime}");
                }), callback);
            }

            Thread.Sleep(3 * 60 * 1000);

            polygon.Unsubscribe(configs.First(config => config.Symbol.Underlying.Value == "AAPL"));

            Log.Trace("Unsubscribing AAPL");
            Thread.Sleep(2000);
            // some messages could be inflight, but after a pause all APPL messages must have been consumed
            unsubscribed = true;

            Thread.Sleep(3 * 60 * 1000);

            polygon.DisposeSafely();

            Assert.That(dataFromEnumerator, Has.Count.GreaterThan(0));
            Assert.That(dataFromEventHandler, Has.Count.EqualTo(dataFromEnumerator.Count));

            dataFromEnumerator = dataFromEnumerator.OrderBy(x => x.Symbol.Value).ThenBy(x => x.Time).ToList();
            dataFromEventHandler = dataFromEventHandler.OrderBy(x => x.Symbol.Value).ThenBy(x => x.Time).ToList();
            for (var i = 0; i < dataFromEnumerator.Count; i++)
            {
                var enumeratorDataPoint = dataFromEnumerator[i];
                var eventHandlerDataPoint = dataFromEventHandler[i];
                Assert.That(enumeratorDataPoint.Symbol, Is.EqualTo(eventHandlerDataPoint.Symbol));
                Assert.That(enumeratorDataPoint.Time, Is.EqualTo(eventHandlerDataPoint.Time));
                Assert.That(enumeratorDataPoint.EndTime, Is.EqualTo(eventHandlerDataPoint.EndTime));
                Assert.That(enumeratorDataPoint.Value, Is.EqualTo(eventHandlerDataPoint.Value));
                Assert.That(enumeratorDataPoint.Open, Is.EqualTo(eventHandlerDataPoint.Open));
                Assert.That(enumeratorDataPoint.High, Is.EqualTo(eventHandlerDataPoint.High));
                Assert.That(enumeratorDataPoint.Low, Is.EqualTo(eventHandlerDataPoint.Low));
                Assert.That(enumeratorDataPoint.Close, Is.EqualTo(eventHandlerDataPoint.Close));
                Assert.That(enumeratorDataPoint.Volume, Is.EqualTo(eventHandlerDataPoint.Volume));
            }
        }

        [Test]
        public void IsConnectedReturnsTrueOnlyAfterAWebSocketConnectionIsOpen()
        {
            using var polygon = new PolygonDataQueueHandler(_apiKey);

            Assert.IsFalse(polygon.IsConnected);

            var config = GetConfigs()[0];
            polygon.Subscribe(config, (sender, args) => { });
            Thread.Sleep(1000);

            Assert.IsTrue(polygon.IsConnected);

            polygon.Unsubscribe(config);
            Thread.Sleep(1000);

            Assert.IsTrue(polygon.IsConnected);
        }

        [Test]
        public void ThrowsOnFailedAuthentication()
        {
            using var polygon = new PolygonDataQueueHandler("invalidapikey");

            var config = GetConfigs()[0];
            Assert.Throws<PolygonFailedAuthenticationException>(() =>
            {
                polygon.Subscribe(config, (sender, args) => { });
            });
        }

        [Test]
        public void StreamsDataForResolutionsHigherThanMinute()
        {
            using var polygon = new PolygonDataQueueHandler(_apiKey);
            var unsubscribed = false;

            var configs = GetConfigs().GroupBy(x => x.Symbol.Underlying, (key, group) => group.First());
            var receivedData = new List<TradeBar>();

            foreach (var config in configs)
            {
                polygon.Subscribe(config, (sender, args) =>
                {
                    var dataPoint = (TradeBar)((NewDataAvailableEventArgs)args).DataPoint;
                    Log.Trace($"{dataPoint}. Time span: {dataPoint.Time} - {dataPoint.EndTime}");

                    receivedData.Add(dataPoint);
                });
            }

            // Run for 3 hours
            Thread.Sleep(3 * 60 * 60 * 1000);

            // Data is not fill forwarded by Polygon, so we expect at most 3 trade bars
            Assert.That(receivedData, Is.Not.Empty.And.Count.LessThanOrEqualTo(3));
        }

        [Test]
        public void RespectsMaximumWebSocketConnectionsAndSubscriptions(
            [Values(1, 2, 3, 4, 5)] int maxSubscriptionsPerWebSocket)
        {
            using var polygon = new TestablePolygonDataQueueHandler(Config.Get("polygon-api-key"), maxSubscriptionsPerWebSocket);
            var configs = GetConfigs();

            var i = 0;
            for (; i < maxSubscriptionsPerWebSocket; i++)
            {
                var config = configs[i];
                Assert.DoesNotThrow(() => polygon.Subscribe(config, (sender, args) => { }),
                    $"Could not subscribe symbol #{i + 1}. Subscription count: {polygon.SubscriptionManager.TotalSubscriptionsCount}");

                var expectedSubscriptionCount = i + 1;
                Assert.That(polygon.SubscriptionManager.TotalSubscriptionsCount, Is.EqualTo(expectedSubscriptionCount));
            }

            Assert.Throws<NotSupportedException>(() => polygon.Subscribe(configs[i], (sender, args) => { }));
        }

        [Test]
        public void StressTest()
        {
            const int maxSubscriptions = 1000;

            using var polygon = new PolygonDataQueueHandler(_apiKey, maxSubscriptions);
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

        private SubscriptionDataConfig GetSubscriptionDataConfig<T>(Symbol symbol, Resolution resolution)
        {
            return new SubscriptionDataConfig(
                typeof(T),
                symbol,
                resolution,
                TimeZones.Utc,
                TimeZones.Utc,
                true,
                true,
                false);
        }

        private void ProcessFeed(IEnumerator<BaseData> enumerator, Action<BaseData> callback = null)
        {
            Task.Run(() =>
            {
                try
                {
                    while (enumerator.MoveNext())
                    {
                        var tick = enumerator.Current;
                        callback?.Invoke(tick);
                    }
                }
                catch (AssertionException)
                {
                    throw;
                }
                catch (Exception err)
                {
                    Log.Error(err.Message);
                }
            });
        }

        /// <summary>
        /// In order to successfuly run the tests, the right contracts should be used. Update expiracy
        /// </summary>
        private SubscriptionDataConfig[] GetConfigs()
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

            return spyOptions.Concat(aaplOptions).ToArray();
        }

        private class TestablePolygonDataQueueHandler : PolygonDataQueueHandler
        {
            public PolygonSubscriptionManager SubscriptionManager => _subscriptionManager;

            public TestablePolygonDataQueueHandler(string apiKey, int maxSubscriptionsPerWebSocket)
                : base(apiKey, maxSubscriptionsPerWebSocket)
            {
            }
        }
    }
}