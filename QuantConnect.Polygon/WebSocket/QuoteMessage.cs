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

namespace QuantConnect.Polygon
{
    /// <summary>
    /// Models a Polygon.io WebSocket quote API message
    /// </summary>
    public class QuoteMessage : BaseMessage
    {
        /// <summary>
        /// The symbol these aggregates are for
        /// </summary>
        [JsonProperty("sym")]
        public string Symbol { get; set; }

        /// <summary>
        /// The bid exchange ID
        /// </summary>
        [JsonProperty("bx")]
        public int BidExchangeID { get; set; }

        /// <summary>
        /// The bid price price
        /// </summary>
        [JsonProperty("bp")]
        public decimal BidPrice { get; set; }

        /// <summary>
        /// The bid size
        /// </summary>
        [JsonProperty("bs")]
        public long BidSize { get; set; }

        /// <summary>
        /// The ask exchange ID
        /// </summary>
        [JsonProperty("ax")]
        public int AskExchangeID { get; set; }

        /// <summary>
        /// The ask price
        /// </summary>
        [JsonProperty("ap")]
        public decimal AskPrice { get; set; }

        /// <summary>
        /// The ask size
        /// </summary>
        [JsonProperty("as")]
        public long AskSize { get; set; }

        /// <summary>
        /// The quote condition
        /// </summary>
        [JsonProperty("c")]
        public long Condition { get; set; }

        /// <summary>
        /// The quote timestamp in UNIX milliseconds
        /// </summary>
        [JsonProperty("t")]
        public long Timestamp { get; set; }
    }
}
