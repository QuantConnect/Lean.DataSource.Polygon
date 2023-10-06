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
using QuantConnect.Lean.Engine.DataFeeds.Enumerators;
using QuantConnect.Logging;
using QuantConnect.Polygon;
using QuantConnect.Util;

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

            var dataFromEnumerator = new List<TradeBar>();
            var dataFromEventHandler = new List<TradeBar>();

            Action<BaseData> callback = (dataPoint) =>
            {
                if (dataPoint == null)
                    return;

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
                    Console.WriteLine($"{dataPoint}. Time span: {dataPoint.Time} - {dataPoint.EndTime}");
                }), callback);
            }

            Thread.Sleep(3 * 60 * 1000);

            polygon.Unsubscribe(configs.First(config => config.Symbol.Underlying.Value == "AAPL"));

            Console.WriteLine("Unsubscribing AAPL");
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
            using var polygon = new PolygonDataQueueHandler();

            Assert.IsFalse(polygon.IsConnected);

            var config = GetConfigs()[0];
            polygon.Subscribe(config, (sender, args) => { });
            Thread.Sleep(1000);

            Assert.IsTrue(polygon.IsConnected);

            polygon.Unsubscribe(config);
            Thread.Sleep(1000);

            Assert.IsTrue(polygon.IsConnected);
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