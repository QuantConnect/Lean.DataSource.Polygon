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
 *
*/

using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Util;

namespace QuantConnect.Tests.Polygon
{
    [TestFixture]
    [Explicit("Tests are dependent on network and take long")]
    public class PolygonDataQueueHandlerStocksTests : PolygonDataQueueHandlerBaseTests
    {
        /// <summary>
        /// The subscription data configs to be used in the tests. At least 2 configs are required
        /// </summary>
        /// <remarks>
        /// In order to successfully run the tests, valid contracts should be used. Update them
        /// </remarks>
        protected override List<SubscriptionDataConfig> GetConfigs(Resolution resolution = Resolution.Second)
        {
            return new[] { "SPY", "AAPL", "GOOG", "MSFT" }
                .Select(ticker =>
                {
                    var symbol = Symbol.Create(ticker, SecurityType.Equity, Market.USA);
                    return new[]
                    {
                        GetSubscriptionDataConfig<TradeBar>(symbol, resolution),
                        GetSubscriptionDataConfig<QuoteBar>(symbol, resolution),
                    };
                })
                .SelectMany(x => x)
                .ToList();
        }
    }
}