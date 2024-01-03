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

using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Util;

namespace QuantConnect.Polygon
{
    public partial class PolygonDataQueueHandler : IDataQueueUniverseProvider
    {
        private IOptionChainProvider _optionChainProvider;

        /// <summary>
        /// Gets the <see cref="IOptionChainProvider"/> instance used to get the option chain for a given symbol
        /// </summary>
        private IOptionChainProvider OptionChainProvider
        {
            get
            {
                if (_optionChainProvider == null)
                {
                    _optionChainProvider = Composer.Instance.GetPart<IOptionChainProvider>();
                }
                return _optionChainProvider;
            }
        }

        /// <summary>
        /// Method returns a collection of symbols that are available at the broker.
        /// </summary>
        /// <param name="symbol">Symbol to search option chain for</param>
        /// <param name="includeExpired">Include expired contracts</param>
        /// <param name="securityCurrency">Expected security currency(if any)</param>
        /// <returns>Future/Option chain associated with the Symbol provided</returns>
        public IEnumerable<Symbol> LookupSymbols(Symbol symbol, bool includeExpired, string securityCurrency = null)
        {
            if (OptionChainProvider == null)
            {
                return Enumerable.Empty<Symbol>();
            }

            if (!IsSecurityTypeSupported(symbol.SecurityType))
            {
                throw new ArgumentException($"Unsupported security type {symbol.SecurityType}");
            }

            Log.Trace($"PolygonDataQueueHandler.LookupSymbols(): Requesting symbol list for {symbol}");

            var symbols = OptionChainProvider.GetOptionContractList(symbol, TimeProvider.GetUtcNow().Date).ToList();

            // Try to remove options contracts that have expired
            if (!includeExpired)
            {
                var removedSymbols = new List<Symbol>();
                symbols.RemoveAll(x =>
                {
                    var expired = x.ID.Date < GetTickTime(x, TimeProvider.GetUtcNow()).Date;
                    if (expired)
                    {
                        removedSymbols.Add(x);
                    }
                    return expired;
                });

                if (removedSymbols.Count > 0)
                {
                    Log.Trace("PolygonDataQueueHandler.LookupSymbols(): Removed contract(s) for having expiry in the past: " +
                        $"{string.Join(",", removedSymbols.Select(x => x.Value))}");
                }
            }

            Log.Trace($"PolygonDataQueueHandler.LookupSymbols(): Returning {symbols.Count} contract(s) for {symbol}");

            return symbols;
        }

        /// <summary>
        /// Returns whether selection can take place or not.
        /// </summary>
        /// <returns>True if selection can take place</returns>
        public bool CanPerformSelection()
        {
            return IsConnected;
        }
    }
}
