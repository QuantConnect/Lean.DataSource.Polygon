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
 */

using System;
using System.Linq;
using NUnit.Framework;
using System.Threading;
using QuantConnect.Data;
using QuantConnect.Tests;
using QuantConnect.Logging;
using System.Threading.Tasks;
using QuantConnect.Data.Market;
using System.Collections.Generic;
using QuantConnect.Configuration;
using System.Collections.Concurrent;
using QuantConnect.Lean.Engine.DataFeeds;

namespace QuantConnect.Lean.DataSource.Polygon.Tests
{
    public class PolygonOpenInterestProcessorManagerTests : PolygonDataProviderBaseTests
    {
        private readonly PolygonRestApiClient _restApiClient = new(Config.Get("polygon-api-key"));

        private readonly PolygonSymbolMapper symbolMapper = new();

        private readonly PolygonAggregationManager dataAggregator = new();

        private readonly ManualTimeProvider _timeProviderInstance = new();

        private readonly EventBasedDataQueueHandlerSubscriptionManager _subscriptionManager = new()
        {
            SubscribeImpl = (symbols, _) => { return true; },
            UnsubscribeImpl = (symbols, _) => { return true; }
        };

        private object _locker = new();

        [TestCase("2024-09-16T09:30:59", true, Description = "Market: After Opening")]
        [TestCase("2024-09-16T15:28:59", true, Description = "Market: Before Closing")]
        [TestCase("2024-09-16T16:28:59", false, Description = "Market: Closed")]
        [Explicit("This tests require a Polygon.io api key, requires internet and are long.")]
        public void GetOpenInterestInDifferentTimeExchangeTime(string mockDateTime, bool isShouldReturnData)
        {
            var waitOneDelay = isShouldReturnData ? TimeSpan.FromSeconds(30) : TimeSpan.FromSeconds(5);
            var resetEvent = new AutoResetEvent(false);
            var cancellationTokenSource = new CancellationTokenSource();
            var optionContractsConfigs = GetConfigs();

            var symbolOpenInterest = new ConcurrentDictionary<Symbol, decimal>();
            Action<BaseData> callback = (baseData) =>
            {
                if (baseData == null)
                {
                    return;
                }

                lock (_locker)
                {
                    symbolOpenInterest[baseData.Symbol] = baseData.Value;

                    if (symbolOpenInterest.Count > 5)
                    {
                        resetEvent.Set();
                    }
                }
            };

            foreach (var config in optionContractsConfigs)
            {
                _subscriptionManager.Subscribe(config);
                ProcessFeed(
                    Subscribe(dataAggregator, config, (sender, args) => { }),
                    cancellationTokenSource.Token,
                    callback: callback
                    );
            }

            var mockDateTimeAfterOpenExchange = DateTime.Parse(mockDateTime).ConvertTo(TimeZones.NewYork, TimeZones.Utc);
            _timeProviderInstance.SetCurrentTimeUtc(mockDateTimeAfterOpenExchange);
            var processor = new PolygonOpenInterestProcessorManager(_timeProviderInstance, _restApiClient, symbolMapper, _subscriptionManager, dataAggregator, GetTickTime);
            processor.ScheduleNextRun();
            resetEvent.WaitOne(waitOneDelay, cancellationTokenSource.Token);

            if (isShouldReturnData)
            {
                Assert.Greater(symbolOpenInterest.Count, 0);
                foreach (var (symbol, openInterest) in symbolOpenInterest)
                {
                    Assert.Greater(openInterest, 0);
                }
            }
            else
            {
                Assert.Zero(symbolOpenInterest.Count);
            }

            cancellationTokenSource.Cancel();
            cancellationTokenSource.Dispose();
            processor.Dispose();
            symbolOpenInterest.Clear();
        }

        [Test]
        public void GetOpenInterestWithHugeAmountSymbols()
        {
            var symbol = Symbol.CreateCanonicalOption(Symbols.AAPL);
            var resetEvent = new AutoResetEvent(false);
            var cancellationTokenSource = new CancellationTokenSource();

            using var polygon = new PolygonDataProvider(ApiKey);

            var optionChain = polygon.LookupSymbols(symbol, true).ToList();

            var expectedAmountOptinContractWithOpenInterest = optionChain.Count;

            var symbolOpenInterest = new ConcurrentDictionary<Symbol, decimal>();
            Action<BaseData> callback = (baseData) =>
            {
                if (baseData == null)
                {
                    return;
                }

                lock (_locker)
                {
                    symbolOpenInterest[baseData.Symbol] = baseData.Value;

                    if (symbolOpenInterest.Count == expectedAmountOptinContractWithOpenInterest)
                    {
                        resetEvent.Set();
                    }
                }
            };

            foreach (var optionContract in optionChain)
            {
                var config = GetSubscriptionDataConfig<OpenInterest>(optionContract, Resolution.Second);
                _subscriptionManager.Subscribe(config);
                StartDataFeed(
                    optionContract,
                    Subscribe(dataAggregator, config, (sender, args) => { }),
                    cancellationTokenSource.Token,
                    cancellationTokenDelayMilliseconds: (int)TimeSpan.FromSeconds(1).TotalMilliseconds,
                    callback: callback
                );
            }

            var mockDateTimeAfterOpenExchange = DateTime.Parse("2024-09-16T09:30:59").ConvertTo(TimeZones.NewYork, TimeZones.Utc);
            _timeProviderInstance.SetCurrentTimeUtc(mockDateTimeAfterOpenExchange);
            var processor = new PolygonOpenInterestProcessorManager(_timeProviderInstance, _restApiClient, symbolMapper, _subscriptionManager, dataAggregator, GetTickTime);

            processor.ScheduleNextRun();

            resetEvent.WaitOne(TimeSpan.FromSeconds(20), cancellationTokenSource.Token);

            cancellationTokenSource.Cancel();
            cancellationTokenSource.Dispose();
            processor.Dispose();

            Assert.AreEqual(expectedAmountOptinContractWithOpenInterest, symbolOpenInterest.Count);
            symbolOpenInterest.Clear();
        }

        private Task _processingTask;
        private readonly ConcurrentDictionary<Symbol, IEnumerator<BaseData>> _activeEnumerators = new();

        private void StartDataFeed(
            Symbol symbol,
            IEnumerator<BaseData> enumerator,
            CancellationToken cancellationToken,
            int cancellationTokenDelayMilliseconds = 100,
            Action<BaseData> callback = null,
            Action throwExceptionCallback = null)
        {
            _activeEnumerators[symbol] = enumerator;

            if (_processingTask != null)
            {
                return;
            }

            _processingTask = Task.Factory.StartNew(() =>
            {
                try
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        foreach (var (_, enumerator) in _activeEnumerators)
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
                    Log.Debug($"{nameof(PolygonOpenInterestProcessorManagerTests)}.{nameof(StartDataFeed)}.Exception: {ex.Message}");
                    throw;
                }
            }, cancellationToken).ContinueWith(task =>
            {
                if (throwExceptionCallback != null)
                {
                    throwExceptionCallback();
                }
                Log.Debug("The throwExceptionCallback is null.");
            }, TaskContinuationOptions.OnlyOnFaulted);
        }

        protected override List<SubscriptionDataConfig> GetConfigs(Resolution resolution = Resolution.Second)
        {
            var configs = new List<SubscriptionDataConfig>();

            var expiryContractDate = new DateTime(2024, 09, 20);
            var strikesAAPL = new decimal[] { 100m, 105m, 110m, 115m, 120m, 125m, 130m, 135m, 140m, 145m };

            foreach (var strike in strikesAAPL)
            {
                var optionContract = Symbol.CreateOption(Symbols.AAPL, Market.USA, OptionStyle.American, OptionRight.Call, strike, expiryContractDate);
                configs.Add(GetSubscriptionDataConfig<OpenInterest>(optionContract, resolution));
            }

            var strikesSPY = new decimal[] { 300m, 320m, 360m, 365m, 380m, 400m, 415m, 420m, 430m, 435m };

            foreach (var strike in strikesSPY)
            {
                var optionContract = Symbol.CreateOption(Symbols.SPY, Market.USA, OptionStyle.American, OptionRight.Call, strike, expiryContractDate);
                configs.Add(GetSubscriptionDataConfig<OpenInterest>(optionContract, resolution));
            }

            return configs;
        }

        private DateTime GetTickTime(Symbol symbol, DateTime utcTime) => utcTime.ConvertFromUtc(TimeZones.NewYork);

        private IEnumerator<BaseData> Subscribe(PolygonAggregationManager dataAggregator, SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
            => dataAggregator.Add(dataConfig, newDataAvailableHandler);
    }
}
