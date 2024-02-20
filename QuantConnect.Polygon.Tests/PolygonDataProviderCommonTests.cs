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
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Packets;
using QuantConnect.Util;

namespace QuantConnect.Lean.DataSource.Polygon.Tests
{
    [TestFixture]
    [Explicit("Tests are dependent on network and take long")]
    public class PolygonDataProviderCommonTests
    {
        private readonly string _apiKey = Config.Get("polygon-api-key");

        [Test]
        public void IsConnectedReturnsTrueOnlyAfterAWebSocketConnectionIsOpen()
        {
            using var polygon = new PolygonDataProvider(_apiKey);

            AssertConnection(polygon);
        }

        private void AssertConnection(PolygonDataProvider polygon)
        {
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
            Assert.Throws<PolygonAuthenticationException>(() =>
            {
                using var polygon = new PolygonDataProvider("invalidapikey");
            });
        }

        [Test]
        public void RespectsMaximumWebSocketConnectionsAndSubscriptions(
            [Values(1, 2, 3, 4, 5)] int maxSubscriptionsPerWebSocket)
        {
            using var polygon = new TestablePolygonDataProvider(Config.Get("polygon-api-key"), maxSubscriptionsPerWebSocket);
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
        public void CanInitializeUsingJobPacket()
        {
            Config.Set("polygon-api-key", "");

            var job = new LiveNodePacket
            {
                BrokerageData = new Dictionary<string, string>() { { "polygon-api-key", _apiKey } }
            };

            using var polygon = new PolygonDataProvider();

            Assert.IsFalse(polygon.IsConnected);

            polygon.SetJob(job);

            AssertConnection(polygon);

            // Restore the API key after the test
            Config.Set("polygon-api-key", _apiKey);
        }

        [Test]
        public void JobPacketWontOverrideCredentials()
        {
            var job = new LiveNodePacket
            {
                BrokerageData = new Dictionary<string, string>() { { "polygon-api-key", "InvalidApiKeyThatWontBeUsed" } }
            };

            // This should pick up the API key from the configuration
            using var polygon = new PolygonDataProvider();

            Assert.IsFalse(polygon.IsConnected);

            // This should not override the API key
            polygon.SetJob(job);

            // Since API key was not overriden, this should work
            AssertConnection(polygon);
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
            return new[] { "SPY", "AAPL", "GOOG", "MSFT" }
                .Select(ticker =>
                {
                    var symbol = Symbol.Create(ticker, SecurityType.Equity, Market.USA);
                    return new[]
                    {
                        GetSubscriptionDataConfig<TradeBar>(symbol, Resolution.Second),
                        GetSubscriptionDataConfig<QuoteBar>(symbol, Resolution.Second),
                    };
                })
                .SelectMany(x => x)
                .ToArray();
        }
    }
}