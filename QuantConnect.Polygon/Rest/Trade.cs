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
    /// Models a single trade tick from a Polygon.io REST API response
    /// </summary>
    public class Trade : ResponseTick
    {
        /// <summary>
        /// Trade timestamp in nanoseconds
        /// </summary>
        [JsonProperty("participant_timestamp")]
        public override long Timestamp { get; set; }

        /// <summary>
        /// The exchange ID
        /// </summary>
        [JsonProperty("exchange")]
        public override int ExchangeID { get; set; }

        /// <summary>
        /// The price of the trade
        /// </summary>
        [JsonProperty("price")]
        public decimal Price { get; set; }

        /// <summary>
        /// The size (volume) of the trade
        /// </summary>
        [JsonProperty("size")]
        public decimal Volume { get; set; }
    }
}
