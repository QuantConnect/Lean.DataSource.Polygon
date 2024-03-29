﻿/*
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
    /// Models a single aggregate bar from a Polygon.io REST API response
    /// </summary>
    public class SingleResponseAggregate
    {
        /// <summary>
        /// Open price of the aggregate window
        /// </summary>
        [JsonProperty("o")]
        public decimal Open { get; set; }

        /// <summary>
        /// High price of the aggregate window
        /// </summary>
        [JsonProperty("h")]
        public decimal High { get; set; }

        /// <summary>
        /// Low price of the aggregate window
        /// </summary>
        [JsonProperty("l")]
        public decimal Low { get; set; }

        /// <summary>
        /// Close price of the aggregate window
        /// </summary>
        [JsonProperty("c")]
        public decimal Close { get; set; }

        /// <summary>
        /// Volume of the aggregate window
        /// </summary>
        [JsonProperty("v")]
        public decimal Volume { get; set; }

        /// <summary>
        /// Starting Timestamp of the aggregate window
        /// </summary>
        [JsonProperty("t")]
        public long Timestamp { get; set; }
    }
}
