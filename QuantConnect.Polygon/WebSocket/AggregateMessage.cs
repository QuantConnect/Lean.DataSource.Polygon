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
    public class AggregateMessage : BaseMessage
    {
        [JsonProperty("sym")]
        public string Symbol { get; set; }

        [JsonProperty("v")]
        public long Volume { get; set; }

        [JsonProperty("av")]
        public long DayAccumulatedVolume { get; set; }

        [JsonProperty("op")]
        public decimal DayOpen { get; set; }

        [JsonProperty("vw")]
        public decimal VolumeWeightedAveragePrice { get; set; }

        [JsonProperty("o")]
        public decimal Open { get; set; }

        [JsonProperty("c")]
        public decimal Close { get; set; }

        [JsonProperty("h")]
        public decimal High { get; set; }

        [JsonProperty("l")]
        public decimal Low { get; set; }

        [JsonProperty("a")]
        public decimal DayVolumeWeightedAveragePrice { get; set; }

        [JsonProperty("z")]
        public long AverageTradeSize { get; set; }

        [JsonProperty("s")]
        public long StartingTickTimestamp { get; set; }

        [JsonProperty("e")]
        public long EndingTickTimestamp { get; set; }

        [JsonProperty("otc")]
        public bool IsOTC { get; set; }
    }
}
