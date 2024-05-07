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

namespace QuantConnect.Lean.DataSource.Polygon
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

        /// <summary>
        /// The option open interest (number of open contracts)
        /// </summary>
        [JsonProperty("open_interest")]
        public int OpenInterest { get; set; }

        /// <summary>
        /// The option implied volatility
        /// </summary>
        [JsonProperty("implied_volatility")]
        public double ImpliedVolatility { get; set; }

        /// <summary>
        /// The option Greeks
        /// </summary>
        [JsonProperty("greeks")]
        public Greeks Greeks { get; set; }

        /// <summary>
        /// The option last trade
        /// </summary>
        [JsonProperty("previous_close")]
        public int Close { get; set; }

        /// <summary>
        /// The option volume
        /// </summary>
        [JsonProperty("volume")]
        public int Volume { get; set; }
    }

    public class Greeks
    {
        /// <summary>
        /// The option delta
        /// </summary>
        [JsonProperty("delta")]
        public double Delta { get; set; }

        /// <summary>
        /// The option gamma
        /// </summary>
        [JsonProperty("gamma")]
        public double Gamma { get; set; }
        
        /// <summary>
        /// \The option theta
        /// </summary>
        [JsonProperty("theta")]
        public double Theta { get; set; }

        /// <summary>
        /// The option vega
        /// </summary>
        [JsonProperty("vega")]
        public double Vega { get; set; }
    }
}
