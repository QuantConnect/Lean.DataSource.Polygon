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

        private object _locker = new();

        [Test]
        public void GetOpenInterestOfOptionSymbolsByPolygonOpenInterestProcessorManager()
        {
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

            _timeProviderInstance.SetCurrentTimeUtc(DateTime.UtcNow);
            var processor = new PolygonOpenInterestProcessorManager(_timeProviderInstance, _restApiClient, symbolMapper, dataAggregator, GetTickTime);

            processor.AddSymbols([.. optionContractsConfigs.Select(x => x.Symbol)]);

            foreach (var config in optionContractsConfigs)
            {
                ProcessFeed(
                    Subscribe(dataAggregator, config, (sender, args) => { }),
                    cancellationTokenSource.Token,
                    callback: callback
                    );
            }

            // Internal delay to respect the 1-minute request interval for OpenInterest data.
            resetEvent.WaitOne(TimeSpan.FromSeconds(70), cancellationTokenSource.Token);

            Assert.Greater(symbolOpenInterest.Count, 0);

            cancellationTokenSource.Cancel();
            cancellationTokenSource.Dispose();
            processor.Dispose();
            symbolOpenInterest.Clear();
        }

        protected override List<SubscriptionDataConfig> GetConfigs(Resolution resolution = Resolution.Second)
        {
            var configs = new List<SubscriptionDataConfig>();

            var expiryContractDate = new DateTime(2025, 05, 16);
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
