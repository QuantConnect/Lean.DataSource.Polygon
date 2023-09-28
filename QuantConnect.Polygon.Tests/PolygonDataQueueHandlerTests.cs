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
    [Explicit("Tests are dependent on network and are long")]
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
            var polygon = new PolygonDataQueueHandler();
            var unsubscribed = false;

            var configs = new[]
            {
                GetSubscriptionDataConfig<TradeBar>(Symbol.CreateOption(Symbol.Create("SPY", SecurityType.Equity, Market.USA), Market.USA,
                    OptionStyle.American, OptionRight.Call, 150m, new DateTime(2023, 10, 20)), Resolution.Minute),
                GetSubscriptionDataConfig<TradeBar>(Symbol.CreateOption(Symbol.Create("AAPL", SecurityType.Equity, Market.USA), Market.USA,
                    OptionStyle.American, OptionRight.Call, 55m, new DateTime(2023, 10, 20)), Resolution.Minute)
            };

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
                ProcessFeed(polygon.Subscribe(config, (sender, args) =>{}), callback);
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
    }
}