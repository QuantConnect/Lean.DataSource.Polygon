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
    /// Models a Polygon.io REST API message containing a list of aggregates
    /// </summary>
    public class AggregatesResponse : BaseResultsResponse<SingleResponseAggregate>
    {
        /// <summary>
        /// The symbol that the aggregates are for
        /// </summary>
        [JsonProperty("ticker")]
        public string Ticker { get; set; }

        /// <summary>
        /// The number of aggregates used to generate the response
        /// </summary>
        [JsonProperty("queryCount")]
        public int QueryCount { get; set; }

        /// <summary>
        /// The total number of results for the request
        /// </summary>
        [JsonProperty("resultsCount")]
        public int ResultsCount { get; set; }

        /// <summary>
        /// Whether or not the data was adjusted for splits.
        /// </summary>
        [JsonProperty("adjusted")]
        public bool Adjusted { get; set; }

        /// <summary>
        /// A request id assigned by the Polygon.io server.
        /// </summary>
        [JsonProperty("request_id")]
        public string RequestId { get; set; }
    }
}
