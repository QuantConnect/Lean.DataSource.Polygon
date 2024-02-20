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
    /// Models a Polygon.io WebSocket trade API message
    /// </summary>
    public class TradeMessage : BaseMessage
    {
        /// <summary>
        /// The symbol these aggregates are for
        /// </summary>
        [JsonProperty("sym")]
        public string Symbol { get; set; }

        /// <summary>
        /// The exchange ID
        /// </summary>
        [JsonProperty("x")]
        public int ExchangeID { get; set; }

        /// <summary>
        /// The trade price
        /// </summary>
        [JsonProperty("p")]
        public decimal Price { get; set; }

        /// <summary>
        /// The trade size
        /// </summary>
        [JsonProperty("s")]
        public long Size { get; set; }

        /// <summary>
        /// The trade conditions
        /// </summary>
        [JsonProperty("c")]
        public long[] Conditions { get; set; }

        /// <summary>
        /// The trade timestamp in UNIX milliseconds
        /// </summary>
        [JsonProperty("t")]
        public long Timestamp { get; set; }
    }
}
