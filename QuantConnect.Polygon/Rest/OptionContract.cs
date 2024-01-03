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
using QuantConnect.Util;

namespace QuantConnect.Polygon
{
    /// <summary>
    /// Models a option contract as returned by the Polygon.io REST API
    /// </summary>
    public class OptionContract
    {
        /// <summary>
        /// The contract right (call, put or "other" in rare cases)
        /// </summary>
        [JsonProperty("contract_type")]
        public string Right { get; set; }

        /// <summary>
        /// The option style (American, European, Bermudan)
        /// </summary>
        [JsonProperty("exercise_style")]
        public string Style { get; set; }

        /// <summary>
        /// The option expiration date
        /// </summary>
        [JsonProperty("expiration_date"), JsonConverter(typeof(DateTimeJsonConverter), "yyyy-MM-dd")]
        public DateTime ExpirationDate { get; set; }

        /// <summary>
        /// The option strike price
        /// </summary>
        [JsonProperty("strike_price")]
        public decimal StrikePrice { get; set; }

        /// <summary>
        /// The option ticker in Polygon's format
        /// </summary>
        [JsonProperty("ticker")]
        public string Ticker { get; set; }
    }
}
