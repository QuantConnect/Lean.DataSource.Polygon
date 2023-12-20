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
using System.Threading;
using NUnit.Framework;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Polygon;
using QuantConnect.Util;

namespace QuantConnect.Tests.Polygon
{
    [TestFixture]
    [Explicit("Tests are dependent on network and take long")]
    public class PolygonDataQueueHandlerCommonTests
    {
        private readonly string _apiKey = Config.Get("polygon-api-key");

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
        public void RespectsMaximumWebSocketConnectionsAndSubscriptions(
            [Values(1, 2, 3, 4, 5)] int maxSubscriptionsPerWebSocket)
        {
            using var polygon = new TestablePolygonDataQueueHandler(Config.Get("polygon-api-key"),
                Config.GetValue("polygon-subscription-plan", PolygonSubscriptionPlan.Starter),
                maxSubscriptionsPerWebSocket);
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

        /// <summary>
        /// In order to successfully run the tests, the right contracts should be used. Update expiracy
        /// </summary>
        protected virtual SubscriptionDataConfig[] GetConfigs()
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
    }
}