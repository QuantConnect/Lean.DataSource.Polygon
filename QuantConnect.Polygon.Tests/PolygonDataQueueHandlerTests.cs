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
using QuantConnect.Logging;
using QuantConnect.Polygon;

namespace QuantConnect.Tests.Polygon
{
    [TestFixture]
    [Explicit("Tests are dependent on network and take long")]
    public class PolygonDataQueueHandlerTests
    {
        [SetUp]
        public void Setup()
        {
            Log.DebuggingEnabled = Config.GetBool("debug-mode");
        }

        [Test]
        public void CanSubscribeAndUnsubscribe()
        {
            using var polygon = new PolygonDataQueueHandler();
            var unsubscribed = false;

            var configs = GetConfigs().GroupBy(x => x.Symbol.Underlying, (key, group) => group.First());

            Action<BaseData> callback = (dataPoint) =>
            {
                if (dataPoint == null)
                    return;

                Log.Trace($"{dataPoint} Time span: {dataPoint.Time} - {dataPoint.EndTime}");
                if (unsubscribed && dataPoint.Symbol.Value == "AAPL")
                {
                    Assert.Fail("Should not receive data for unsubscribed symbol");
                }
            };

            foreach (var config in configs)
            {
                ProcessFeed(polygon.Subscribe(config, (sender, args) => {}), callback);
            }

            Thread.Sleep(3 * 60 * 1000);

            polygon.Unsubscribe(configs.First(config => config.Symbol.Underlying.Value == "AAPL"));

            Log.Trace("Unsubscribing");
            Thread.Sleep(2000);
            // some messages could be inflight, but after a pause all MBLY messages must have beed consumed by ProcessFeed
            unsubscribed = true;

            Thread.Sleep(3 * 60 * 1000);

            polygon.Dispose();
        }

        [TestCase(1, 8)]
        [TestCase(8, 1)]
        [TestCase(2, 4)]
        [TestCase(4, 2)]
        [TestCase(2, 3)]
        [TestCase(3, 2)]
        [TestCase(1, 1)]
        [TestCase(2, 2)]
        [TestCase(3, 3)]
        public void RespectsMaximumWebSocketConnectionsAndSubscriptions(int MaxWebSocketConnections, int MaxSubscriptionsPerWebSocket)
        {
            Config.Set("polygon-max-websocket-connections", MaxWebSocketConnections);
            Config.Set("polygon-max-subscriptions-per-websocket", MaxSubscriptionsPerWebSocket);

            using var polygon = new PolygonDataQueueHandler();

            var configs = GetConfigs();

            for (var i = 0; i < MaxWebSocketConnections * MaxSubscriptionsPerWebSocket; i++)
            {
                var config = configs[i];
                Assert.DoesNotThrow(() => polygon.Subscribe(config, (sender, args) => { }),
                    $"Could not subscribe symbol #{i + 1}. WebSocket count: {polygon.WebSocketCount}. Subscription count: {polygon.SubscriptionCount}");

                var expectedSubscriptionCount = i + 1;
                Assert.That(polygon.SubscriptionCount, Is.EqualTo(expectedSubscriptionCount));

                var expectedWebSocketCount = i / MaxSubscriptionsPerWebSocket + 1;
                Assert.That(polygon.WebSocketCount, Is.EqualTo(expectedWebSocketCount));
            }

            Assert.That(polygon.WebSocketCount, Is.EqualTo(MaxWebSocketConnections));
            Assert.That(polygon.SubscriptionCount, Is.EqualTo(MaxWebSocketConnections * MaxSubscriptionsPerWebSocket));

            Assert.Throws<NotSupportedException>(() => polygon.Subscribe(configs.Last(), (sender, args) => { }));
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

        private SubscriptionDataConfig[] GetConfigs()
        {
            var spyOptions = new[] { 220m, 260m, 300m, 340m, 380m }.Select(strike => GetSubscriptionDataConfig<TradeBar>(
                Symbol.CreateOption(
                    Symbols.SPY,
                    Market.USA,
                    OptionStyle.American,
                    OptionRight.Call,
                    strike,
                    new DateTime(2023, 10, 20)),
                Resolution.Minute));

            var aaplOptions = new[] { 80m, 100m, 130m, 150m, 170m }.Select(strike => GetSubscriptionDataConfig<TradeBar>(
                Symbol.CreateOption(
                    Symbols.AAPL,
                    Market.USA,
                    OptionStyle.American,
                    OptionRight.Call,
                    strike,
                    new DateTime(2023, 10, 27)),
                Resolution.Minute));

            return spyOptions.Concat(aaplOptions).ToArray();
        }
    }
}