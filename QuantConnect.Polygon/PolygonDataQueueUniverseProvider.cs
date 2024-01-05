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
        /// Method returns a collection of symbols that are available at the broker.
        /// </summary>
        /// <param name="symbol">Symbol to search option chain for</param>
        /// <param name="includeExpired">Include expired contracts</param>
        /// <param name="securityCurrency">Expected security currency(if any)</param>
        /// <returns>Future/Option chain associated with the Symbol provided</returns>
        public IEnumerable<Symbol> LookupSymbols(Symbol symbol, bool includeExpired, string securityCurrency = null)
        {
            var utcNow = TimeProvider.GetUtcNow();
            var symbols = GetOptionChain(symbol, utcNow.Date);

            // Try to remove options contracts that have expired
            if (!includeExpired)
            {
                var removedSymbols = new List<Symbol>();
                foreach (var optionSymbol in symbols)
                {
                    if (optionSymbol.ID.Date < GetTickTime(optionSymbol, utcNow).Date)
                    {
                        removedSymbols.Add(optionSymbol);
                        continue;
                    }

                    yield return optionSymbol;
                }

                if (removedSymbols.Count > 0)
                {
                    Log.Trace("PolygonDataQueueHandler.LookupSymbols(): Removed contract(s) for having expiry in the past: " +
                        $"{string.Join(",", removedSymbols.Select(x => x.Value))}");
                }
            }

            foreach (var optionSymbol in symbols)
            {
                yield return optionSymbol;
            }
        }

        /// <summary>
        /// Method returns a collection of symbols that are available at the broker.
        /// </summary>
        /// <param name="symbol">Symbol to search option chain for</param>
        /// <param name="date">Reference date</param>
        /// <returns>Option chain associated with the provided symbol</returns>
        public IEnumerable<Symbol> GetOptionChain(Symbol symbol, DateTime date)
        {
            if ((symbol.SecurityType.IsOption() && symbol.SecurityType == SecurityType.FutureOption) ||
                (symbol.HasUnderlying && symbol.Underlying.SecurityType != SecurityType.Equity && symbol.Underlying.SecurityType != SecurityType.Index))
            {
                throw new ArgumentException($"Unsupported security type {symbol.SecurityType}");
            }

            Log.Trace($"PolygonDataQueueHandler.GetOptionChain(): Requesting symbol list for {symbol}");

            return _optionChainProvider.GetOptionContractList(symbol, date);
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
