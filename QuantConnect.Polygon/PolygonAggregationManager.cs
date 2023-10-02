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

using System.Collections.Concurrent;

using QuantConnect.Data;
using QuantConnect.Data.Consolidators;
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Lean.Engine.DataFeeds.Enumerators;
using QuantConnect.Logging;

namespace QuantConnect.Polygon
{
    /// <summary>
    /// Aggregates ticks and bars based on given subscriptions.
    /// Current implementation is based on <see cref="AggregationManager"/> and adds special handling to aggregate <see cref="TradeBar"/> data.
    /// </summary>
    public class PolygonAggregationManager : AggregationManager
    {
        private readonly ConcurrentDictionary<SecurityIdentifier, List<KeyValuePair<SubscriptionDataConfig, ScannableEnumerator<BaseData>>>> _enumerators
            = new();

        /// <summary>
        /// Add new subscription to current <see cref="IDataAggregator"/> instance
        /// </summary>
        /// <param name="dataConfig">defines the parameters to subscribe to a data feed</param>
        /// <param name="newDataAvailableHandler">handler to be fired on new data available</param>
        /// <returns>The new enumerator for this subscription request</returns>
        public IEnumerator<BaseData> Add(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {
            if (dataConfig.Type != typeof(TradeBar))
            {
                return base.Add(dataConfig, newDataAvailableHandler);
            }

            var consolidator = new TradeBarConsolidator(dataConfig.Resolution.ToTimeSpan());
            var isPeriodBased = dataConfig.Resolution != Resolution.Tick;
            var enumerator = new ScannableEnumerator<BaseData>(consolidator, dataConfig.ExchangeTimeZone, TimeProvider, newDataAvailableHandler, isPeriodBased);

            _enumerators.AddOrUpdate(
                dataConfig.Symbol.ID,
                new List<KeyValuePair<SubscriptionDataConfig, ScannableEnumerator<BaseData>>> { new KeyValuePair<SubscriptionDataConfig, ScannableEnumerator<BaseData>>(dataConfig, enumerator) },
                (k, v) => { return v.Concat(new[] { new KeyValuePair<SubscriptionDataConfig, ScannableEnumerator<BaseData>>(dataConfig, enumerator) }).ToList(); });

            return enumerator;
        }

        /// <summary>
        /// Removes the handler with the specified identifier
        /// </summary>
        /// <param name="dataConfig">Subscription data configuration to be removed</param>
        public bool Remove(SubscriptionDataConfig dataConfig)
        {
            if (dataConfig.Type != typeof(TradeBar))
            {
                return base.Remove(dataConfig);
            }

            List<KeyValuePair<SubscriptionDataConfig, ScannableEnumerator<BaseData>>> enumerators;
            if (_enumerators.TryGetValue(dataConfig.Symbol.ID, out enumerators))
            {
                if (enumerators.Count == 1)
                {
                    List<KeyValuePair<SubscriptionDataConfig, ScannableEnumerator<BaseData>>> output;
                    return _enumerators.TryRemove(dataConfig.Symbol.ID, out output);
                }
                else
                {
                    _enumerators[dataConfig.Symbol.ID] = enumerators.Where(pair => pair.Key != dataConfig).ToList();
                    return true;
                }
            }
            else
            {
                Log.Debug($"PolygonAggregationManager.Update(): IDataConsolidator for symbol ({dataConfig.Symbol.Value}) was not found.");
                return false;
            }
        }

        /// <summary>
        /// Add new data to aggregator
        /// </summary>
        /// <param name="input">The new data</param>
        public void Update(BaseData input)
        {
            try
            {
                if (!(input is TradeBar))
                {
                    base.Update(input);
                    return;
                }

                if (_enumerators.TryGetValue(input.Symbol.ID, out var enumerators))
                {
                    for (var i = 0; i < enumerators.Count; i++)
                    {
                        var kvp = enumerators[i];
                        kvp.Value.Update(input);
                    }
                }
            }
            catch (Exception exception)
            {
                Log.Error(exception);
            }
        }
    }
}
