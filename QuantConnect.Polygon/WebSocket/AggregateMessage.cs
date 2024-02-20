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

using Newtonsoft.Json;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Models a Polygon.io WebSocket aggregates API message
    /// </summary>
    public class AggregateMessage : BaseMessage
    {
        /// <summary>
        /// The symbol these aggregates are for
        /// </summary>
        [JsonProperty("sym")]
        public string Symbol { get; set; }

        /// <summary>
        /// The tick volume
        /// </summary>
        [JsonProperty("v")]
        public long Volume { get; set; }

        /// <summary>
        /// Today's accumulated volume
        /// </summary>
        [JsonProperty("av")]
        public long DayAccumulatedVolume { get; set; }

        /// <summary>
        /// Today's official opening price
        /// </summary>
        [JsonProperty("op")]
        public decimal DayOpen { get; set; }

        /// <summary>
        /// The volume-weighted average value for the aggregate window
        /// </summary>
        [JsonProperty("vw")]
        public decimal VolumeWeightedAveragePrice { get; set; }

        /// <summary>
        /// The open price for the symbol in this aggregate
        /// </summary>
        [JsonProperty("o")]
        public decimal Open { get; set; }

        /// <summary>
        /// The highest price for the symbol in this aggregate
        /// </summary>
        [JsonProperty("h")]
        public decimal High { get; set; }

        /// <summary>
        /// The lowest price for the symbol in this aggregate
        /// </summary>
        [JsonProperty("l")]
        public decimal Low { get; set; }

        /// <summary>
        /// The close price for the symbol in this aggregate
        [JsonProperty("c")]
        public decimal Close { get; set; }

        /// <summary>
        /// Today's volume weighted average price
        /// </summary>
        [JsonProperty("a")]
        public decimal DayVolumeWeightedAveragePrice { get; set; }

        /// <summary>
        /// The average trade size for this aggregate window
        /// </summary>
        [JsonProperty("z")]
        public long AverageTradeSize { get; set; }

        /// <summary>
        /// The timestamp of the starting tick for this aggregate window in Unix Milliseconds
        /// </summary>
        [JsonProperty("s")]
        public long StartingTickTimestamp { get; set; }

        /// <summary>
        /// The timestamp of the ending tick for this aggregate window in Unix Milliseconds
        /// </summary>
        [JsonProperty("e")]
        public long EndingTickTimestamp { get; set; }
    }
}
