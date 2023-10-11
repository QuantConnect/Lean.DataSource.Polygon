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
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.DataFeeds;

namespace QuantConnect.Polygon
{
    /// <summary>
    /// Aggregates ticks and bars based on given subscriptions.
    /// Current implementation is based on <see cref="AggregationManager"/> and adds special handling to aggregate <see cref="TradeBar"/> data.
    /// </summary>
    public class PolygonAggregationManager : AggregationManager
    {
        /// <summary>
        /// Gets the consolidator to aggregate data for the given config
        /// </summary>
        protected override IDataConsolidator GetConsolidator(SubscriptionDataConfig config)
        {
            if (config.Type != typeof(TradeBar))
            {
                throw new ArgumentException($"Unsupported subscription data config type {config.Type}");
            }

            // We use the TradeBarConsolidator for TradeBar data given that we are aggregating trade bars
            // (that are already aggregated by Polygon) instead of ticks.
            return new TradeBarConsolidator(config.Resolution.ToTimeSpan());
        }
    }
}
