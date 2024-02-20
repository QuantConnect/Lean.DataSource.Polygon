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
    /// Models a single quote tick from a Polygon.io REST API response
    /// </summary>
    public class Quote : ResponseTick
    {
        /// <summary>
        /// Quote timestamp in nanoseconds
        /// </summary>
        [JsonProperty("participant_timestamp")]
        public override long Timestamp { get; set; }

        /// <summary>
        /// The exchange ID
        /// </summary>
        [JsonIgnore]
        public override int ExchangeID
        {
            get { return BidExchangeID; }
            set { BidExchangeID = value; }
        }

        /// <summary>
        /// The bid exchange ID
        /// </summary>
        [JsonProperty("bid_exchange")]
        public int BidExchangeID { get; set; }

        /// <summary>
        /// The bid price price
        /// </summary>
        [JsonProperty("bid_price")]
        public decimal BidPrice { get; set; }

        /// <summary>
        /// The bid size
        /// </summary>
        [JsonProperty("bid_size")]
        public long BidSize { get; set; }

        /// <summary>
        /// The ask exchange ID
        /// </summary>
        [JsonProperty("ask_exchange")]
        public int AskExchangeID { get; set; }

        /// <summary>
        /// The ask price
        /// </summary>
        [JsonProperty("ask_price")]
        public decimal AskPrice { get; set; }

        /// <summary>
        /// The ask size
        /// </summary>
        [JsonProperty("ask_size")]
        public long AskSize { get; set; }
    }
}
