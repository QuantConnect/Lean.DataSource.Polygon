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
using QuantConnect.Util;

namespace QuantConnect.Lean.DataSource.Polygon.Tests
{
    public abstract class PolygonDataProviderBaseTests
    {
        protected readonly string ApiKey = Config.Get("polygon-api-key");

        // With each subscription plan, different streaming methods will be used:
        //  - Starter: Only resolutions >= second and trade data will be streamed, using aggregated second bars
        //  - Developer: All resolutions and trade data only will be streamed, using trade ticks
        //  - Advanced: All resolutions and both trade and quote data will be streamed, using trade and quote ticks
        [Test]
        [Explicit("Tests are dependent on network and take long. " +
            "Also, this test will only pass if the subscribed securities are liquid enough to get data in the test run time.")]
        public void CanSubscribeAndUnsubscribe()
        {
            using var polygon = new PolygonDataProvider(ApiKey);
            var unsubscribed = false;

            var configs = GetConfigs(Resolution.Second);
            Assert.That(configs, Is.Not.Empty);

            var dataFromEnumerator = new List<TradeBar>();
            var dataFromEventHandler = new List<TradeBar>();

            Action<BaseData> callback = (dataPoint) =>
            {
                if (dataPoint == null)
                {
                    return;
                }

                dataFromEnumerator.Add((TradeBar)dataPoint);

                if (unsubscribed)
                {
                    Assert.Fail("Should not receive data for unsubscribed symbols");
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

            const int seconds = 10;
            Thread.Sleep(seconds * 1000);

            foreach (var config in configs)
            {
                polygon.Unsubscribe(config);
            }

            Log.Trace("Unsubscribing symbols");

            // some messages could be inflight, but after a pause all messages must have been consumed
            Thread.Sleep(2 * 1000);

            unsubscribed = true;
            var dataCount = dataFromEnumerator.Count;

            Assert.That(dataFromEnumerator, Is.Not.Empty.And.Count.LessThanOrEqualTo(seconds * configs.Count));
            Assert.That(dataFromEventHandler, Has.Count.EqualTo(dataFromEnumerator.Count));

            // Let's give some time to make sure we don't receive any more data
            Thread.Sleep(seconds * 1000);

            Assert.That(dataFromEnumerator.Count, Is.EqualTo(dataCount));
            Assert.That(dataFromEventHandler.Count, Is.EqualTo(dataCount));

            polygon.DisposeSafely();

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

        [TestCase(Resolution.Tick, 1)]
        [TestCase(Resolution.Second, 15)]
        [TestCase(Resolution.Minute, 3)]
        [TestCase(Resolution.Hour, 3)]
        [Explicit("Tests are dependent on network and take long. " +
            "Also, this test will only pass if the subscribed securities are liquid enough to get data in the test run time.")]
        public virtual void StreamsDataForDifferentResolutions(Resolution resolution, int period)
        {
            using var polygon = new PolygonDataProvider(ApiKey);

            var configs = GetConfigs(resolution);
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

            var timeSpan = resolution != Resolution.Tick ? resolution.ToTimeSpan() : TimeSpan.FromSeconds(5);

            // Run for the specified period
            Thread.Sleep(period * (int)timeSpan.TotalMilliseconds);

            Log.Trace("Unsubscribing symbols");
            foreach (var config in configs)
            {
                polygon.Unsubscribe(config);
            }

            // Some messages could be inflight
            Thread.Sleep(2 * 1000);

            Log.Trace($"Received {receivedData.Count} data points");
            Assert.That(receivedData, Is.Not.Empty);

            if (resolution == Resolution.Tick)
            {
                foreach (var data in receivedData)
                {
                    Assert.That(data.EndTime - data.Time, Is.LessThan(TimeSpan.FromSeconds(1)));
                }
            }
            else
            {
                foreach (var data in receivedData)
                {
                    Assert.That(data.EndTime - data.Time, Is.EqualTo(timeSpan));
                }
            }
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

        protected static Task StartEnumeratorProcessing(
            Func<IEnumerable<IEnumerator<BaseData>>> enumeratorProvider,
            Action<BaseData> callback,
            int cancellationTokenDelayMilliseconds = 100,
            Action throwExceptionCallback = null,
            CancellationToken cancellationToken = default)
        {
            return Task.Factory.StartNew(() =>
            {
                try
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        foreach (var enumerator in enumeratorProvider())
                        {
                            if (enumerator.MoveNext())
                            {
                                if (enumerator.Current is BaseData tick && tick != null)
                                {
                                    callback?.Invoke(tick);
                                }
                            }
                        }

                        cancellationToken.WaitHandle.WaitOne(TimeSpan.FromMilliseconds(cancellationTokenDelayMilliseconds));
                    }
                }
                catch (Exception ex)
                {
                    Log.Debug($"Enumerator Processing Exception: {ex.Message}");
                    throw;
                }
            }, cancellationToken).ContinueWith(task =>
            {
                throwExceptionCallback?.Invoke();
                Log.Debug("Enumerator processing encountered an exception.");
            }, TaskContinuationOptions.OnlyOnFaulted);
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
        /// The subscription data configs to be used in the tests. At least 2 configs are required
        /// </summary>
        /// <remarks>
        /// In order to successfully run the tests, valid contracts should be used. Update them
        /// </remarks>
        protected abstract List<SubscriptionDataConfig> GetConfigs(Resolution resolution = Resolution.Second);
    }
}