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
using QuantConnect.Lean.Engine.DataFeeds.Enumerators;
using QuantConnect.Logging;
using QuantConnect.Polygon;
using QuantConnect.Util;

namespace QuantConnect.Tests.Polygon
{
    [TestFixture]
    [Explicit("Tests are dependent on network and take long")]
    public class PolygonDataQueueHandlerIndicesTests : PolygonDataQueueHandlerBaseTests
    {
        // Overriding because tick data is not supported for indices
        [TestCase(Resolution.Second, 15)]
        [TestCase(Resolution.Minute, 3)]
        [TestCase(Resolution.Hour, 3)]
        [Explicit("Tests are dependent on network and take long. " +
            "Also, this test will only pass if the subscribed securities are liquid enough to get data in the test run time.")]
        public override void StreamsDataForDifferentResolutions(Resolution resolution, int period)
        {
            base.StreamsDataForDifferentResolutions(resolution, period);
        }

        [Test]
        [Explicit("Tests are dependent on network and take long. " +
            "Also, this test will only pass if the subscribed securities are liquid enough to get data in the test run time.")]
        public void TickDataIsNotSupportedForStreaming()
        {
            using var polygon = new PolygonDataQueueHandler(ApiKey);

            var configs = GetConfigs(Resolution.Tick);
            var receivedData = new List<BaseData>();

            foreach (var config in configs)
            {
                polygon.Subscribe(config, (sender, args) =>
                {
                    var dataPoint = (BaseData)((NewDataAvailableEventArgs)args).DataPoint;
                    Log.Trace($"{dataPoint}. Time span: {dataPoint.Time} - {dataPoint.EndTime}");

                    receivedData.Add(dataPoint);
                });
            }

            Thread.Sleep(TimeSpan.FromSeconds(10));

            Log.Trace("Unsubscribing symbols");
            foreach (var config in configs)
            {
                polygon.Unsubscribe(config);
            }

            Assert.That(receivedData, Is.Empty);
        }

        /// <summary>
        /// The subscription data configs to be used in the tests. At least 2 configs are required
        /// </summary>
        /// <remarks>
        /// In order to successfully run the tests, valid contracts should be used. Update them
        /// </remarks>
        protected override List<SubscriptionDataConfig> GetConfigs(Resolution resolution = Resolution.Second)
        {
            return new[] { "SPX", "VIX", "DJI", "IXIC" }
                .Select(ticker =>
                {
                    var symbol = Symbol.Create(ticker, SecurityType.Index, Market.USA);
                    return new[]
                    {
                        GetSubscriptionDataConfig<TradeBar>(symbol, resolution),
                        GetSubscriptionDataConfig<QuoteBar>(symbol, resolution),
                    };
                })
                .SelectMany(x => x)
                .ToList();
        }
    }
}