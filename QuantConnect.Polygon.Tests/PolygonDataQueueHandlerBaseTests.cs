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
using System.Threading.Tasks;
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
    public abstract class PolygonDataQueueHandlerBaseTests
    {
        private readonly string _apiKey = Config.Get("polygon-api-key");

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

            var configs = GetConfigs().Take(2).ToList();
            var configToUnsubscribe = configs[1];

            var dataFromEnumerator = new List<TradeBar>();
            var dataFromEventHandler = new List<TradeBar>();

            Action<BaseData> callback = (dataPoint) =>
            {
                if (dataPoint == null)
                {
                    return;
                }

                dataFromEnumerator.Add((TradeBar)dataPoint);

                if (unsubscribed && dataPoint.Symbol == configToUnsubscribe.Symbol)
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

            polygon.Unsubscribe(configToUnsubscribe);
            Log.Trace($"Unsubscribing {configToUnsubscribe.Symbol}");

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
        public void StreamsDataForResolutionsHigherThanMinute()
        {
            using var polygon = new PolygonDataQueueHandler(_apiKey);
            var unsubscribed = false;

            var configs = GetConfigs().Take(2).ToList();
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

        protected SubscriptionDataConfig GetSubscriptionDataConfig<T>(Symbol symbol, Resolution resolution)
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
        protected abstract SubscriptionDataConfig[] GetConfigs();
    }
}