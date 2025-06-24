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

        private readonly Lock _locker = new();

        [Test]
        [Explicit("This tests require a Polygon.io api key")]
        public void GetOpenInterestWithHugeAmountSymbols()
        {
            var symbol = Symbol.CreateCanonicalOption(Symbols.AAPL);
            var resetEvent = new AutoResetEvent(false);
            var cancellationTokenSource = new CancellationTokenSource();
            var _activeEnumerators = new ConcurrentDictionary<Symbol, IEnumerator<BaseData>>();

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
                _activeEnumerators[optionContract] = Subscribe(dataAggregator, config, (sender, args) => { });
            }

            StartEnumeratorProcessing(() => _activeEnumerators.Values, callback, (int)TimeSpan.FromSeconds(1).TotalMilliseconds, cancellationToken: cancellationTokenSource.Token);

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

        protected override List<SubscriptionDataConfig> GetConfigs(Resolution resolution = Resolution.Second) => throw new NotImplementedException();

        private DateTime GetTickTime(Symbol symbol, DateTime utcTime) => utcTime.ConvertFromUtc(TimeZones.NewYork);

        private IEnumerator<BaseData> Subscribe(PolygonAggregationManager dataAggregator, SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
            => dataAggregator.Add(dataConfig, newDataAvailableHandler);
    }
}
