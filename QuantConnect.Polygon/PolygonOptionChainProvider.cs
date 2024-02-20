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
using RestSharp;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Polygon.io implementation of <see cref="IOptionChainProvider"/>
    /// </summary>
    /// <remarks>
    /// Reference: https://polygon.io/docs/options/get_v3_reference_options_contracts
    /// </remarks>
    public class PolygonOptionChainProvider : IOptionChainProvider
    {
        private PolygonRestApiClient _restApiClient;
        private PolygonSymbolMapper _symbolMapper;

        private bool _unsupportedSecurityTypeLogSent;

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonOptionChainProvider"/> class
        /// </summary>
        /// <param name="restApiClient">The Polygon REST API client</param>
        /// <param name="symbolMapper">The Polygon symbol mapper</param>
        public PolygonOptionChainProvider(PolygonRestApiClient restApiClient, PolygonSymbolMapper symbolMapper)
        {
            _restApiClient = restApiClient;
            _symbolMapper = symbolMapper;
        }

        /// <summary>
        /// Gets the list of option contracts for a given underlying symbol from Polygon's REST API
        /// </summary>
        /// <param name="symbol">The option or the underlying symbol to get the option chain for.
        /// Providing the option allows targeting an option ticker different than the default e.g. SPXW</param>
        /// <param name="date">The date for which to request the option chain (only used in backtesting)</param>
        /// <returns>The list of option contracts</returns>
        public IEnumerable<Symbol> GetOptionContractList(Symbol symbol, DateTime date)
        {
            // Only equity and index options are supported
            if (symbol.SecurityType == SecurityType.Future || symbol.SecurityType == SecurityType.FutureOption)
            {
                if (!_unsupportedSecurityTypeLogSent)
                {
                    Log.Trace($"PolygonOptionChainProvider.GetOptionContractList(): Unsupported security type {symbol.SecurityType}");
                    _unsupportedSecurityTypeLogSent = true;
                }

                yield break;
            }

            var underlying = symbol.SecurityType.IsOption() ? symbol.Underlying : symbol;
            var optionsSecurityType = underlying.SecurityType == SecurityType.Index ? SecurityType.IndexOption : SecurityType.Option;

            var request = new RestRequest("/v3/reference/options/contracts", Method.GET);
            request.AddQueryParameter("underlying_ticker", underlying.ID.Symbol);
            request.AddQueryParameter("as_of", date.ToStringInvariant("yyyy-MM-dd"));
            request.AddQueryParameter("limit", "1000");

            foreach (var contract in _restApiClient.DownloadAndParseData<OptionChainResponse>(request).SelectMany(response => response.Results))
            {
                // Unsupported option style (e.g. bermudan) or right (e.g. "other" in rare cases according to the endpoint's docs)
                if (!Enum.TryParse<OptionStyle>(contract.Style, ignoreCase: true, out var optionStyle) ||
                    !Enum.TryParse<OptionRight>(contract.Right, ignoreCase: true, out var optionRight))
                {
                    continue;
                }

                var contractSymbol = _symbolMapper.GetLeanSymbol(contract.Ticker, optionsSecurityType, underlying.ID.Market, optionStyle,
                    contract.ExpirationDate, contract.StrikePrice, optionRight, underlying);
                yield return contractSymbol;
            }
        }
    }
}
