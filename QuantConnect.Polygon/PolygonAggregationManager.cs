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

using QuantConnect.Data;
using QuantConnect.Data.Consolidators;
using QuantConnect.Lean.Engine.DataFeeds;

namespace QuantConnect.Polygon
{
    /// <summary>
    /// Aggregates Polygon.io trade bars into same or higher resolution bars
    /// </summary>
    public class PolygonAggregationManager : AggregationManager
    {
        private PolygonSubscriptionPlan _subscriptionPlan;

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonAggregationManager"/> class
        /// </summary>
        /// <param name="subscriptionPlan">Polygon subscription plan</param>
        public PolygonAggregationManager(PolygonSubscriptionPlan subscriptionPlan)
        {
            _subscriptionPlan = subscriptionPlan;
        }

        /// <summary>
        /// Gets the consolidator to aggregate data for the given config
        /// </summary>
        protected override IDataConsolidator GetConsolidator(SubscriptionDataConfig config)
        {
            if (config.TickType == TickType.OpenInterest)
            {
                throw new ArgumentException($"Unsupported subscription tick type {config.TickType}");
            }

            if (_subscriptionPlan < PolygonSubscriptionPlan.Advanced)
            {
                if (config.TickType != TickType.Trade)
                {
                    throw new ArgumentException($"Unsupported subscription data config type {config.TickType} " +
                        $"for {_subscriptionPlan} Polygon.io subscription plan");
                }

                if (_subscriptionPlan < PolygonSubscriptionPlan.Developer)
                {
                    // Starter plan only supports streaming aggregated data.
                    // We use the TradeBarConsolidator for TradeBar data given that we are aggregating trade bars
                    // (that are already aggregated by Polygon) instead of ticks.
                    return new TradeBarConsolidator(config.Resolution.ToTimeSpan());
                }
            }

            // Use base's method, since we can fetch ticks with Developer and Advanced plans
            return base.GetConsolidator(config);
        }
    }
}
