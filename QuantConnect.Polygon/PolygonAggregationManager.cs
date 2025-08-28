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

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Aggregates Polygon.io trade bars into same or higher resolution bars
    /// </summary>
    public class PolygonAggregationManager : AggregationManager
    {
        private EventType _usingEventType;

        /// <summary>
        /// Signals whether aggregated bars are being streamed instead of ticks
        /// so the consolidator to use can get trade bars as inputs instead of ticks.
        /// </summary>
        /// <param name="useEventType">Whether aggregated bars are being streamed instead of ticks</param>
        public void SetUsingAggregates(EventType useEventType)
        {
            _usingEventType = useEventType;
        }

        /// <summary>
        /// Gets the consolidator to aggregate data for the given config
        /// </summary>
        protected override IDataConsolidator GetConsolidator(SubscriptionDataConfig config)
        {
            switch (_usingEventType)
            {
                case EventType.A when config.Resolution == Resolution.Second && config.TickType == TickType.Trade:
                case EventType.AM when config.Resolution == Resolution.Minute && config.TickType == TickType.Trade:
                    return new FilteredIdentityDataConsolidator<BaseData>(data => data.GetType() == config.Type);
                case EventType.A when config.Resolution >= Resolution.Minute && config.TickType == TickType.Trade:
                case EventType.AM when config.Resolution >= Resolution.Minute && config.TickType == TickType.Trade:
                    // Starter plan only supports streaming aggregated data.
                    // We use the TradeBarConsolidator for TradeBar data given that we are aggregating trade bars
                    // (that are already aggregated by Polygon) instead of ticks.
                    return new TradeBarConsolidator(config.Resolution.ToTimeSpan());
                default:
                    // Use base's method, since we can fetch ticks with Developer and Advanced plans
                    return base.GetConsolidator(config);
            }
        }
    }
}
