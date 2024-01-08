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

using QuantConnect.Brokerages;
using QuantConnect.Securities.IndexOption;
using System.Globalization;

namespace QuantConnect.Polygon
{
    /// <summary>
    /// Provides the mapping between Lean symbols and Polygon.io symbols.
    /// </summary>
    public class PolygonSymbolMapper : ISymbolMapper
    {
        private readonly Dictionary<string, Symbol> _leanSymbolsCache = new();
        private readonly Dictionary<Symbol, string> _brokerageSymbolsCache = new();
        private readonly object _locker = new();

        /// <summary>
        /// Converts a Lean symbol instance to a brokerage symbol
        /// </summary>
        /// <param name="symbol">A Lean symbol instance</param>
        /// <returns>The brokerage symbol</returns>
        public string GetBrokerageSymbol(Symbol symbol)
        {
            if (symbol == null || string.IsNullOrWhiteSpace(symbol.Value))
            {
                throw new ArgumentException($"Invalid symbol: {(symbol == null ? "null" : symbol.ToString())}");
            }

            lock (_locker)
            {
                if (!_brokerageSymbolsCache.TryGetValue(symbol, out var brokerageSymbol))
                {
                    var ticker = symbol.Value.Replace(" ", "");
                    switch (symbol.SecurityType)
                    {
                        case SecurityType.Equity:
                            brokerageSymbol = ticker;
                            break;

                        case SecurityType.Index:
                            brokerageSymbol = $"I:{ticker}";
                            break;

                        case SecurityType.Option:
                        case SecurityType.IndexOption:
                            brokerageSymbol = $"O:{ticker}";
                            break;

                        default:
                            throw new Exception($"PolygonSymbolMapper.GetBrokerageSymbol(): unsupported security type: {symbol.SecurityType}");
                    }

                    // Lean-to-Polygon symbol conversion is accurate, so we can cache it both ways
                    _brokerageSymbolsCache[symbol] = brokerageSymbol;
                    _leanSymbolsCache[brokerageSymbol] = symbol;
                }

                return brokerageSymbol;
            }
        }

        /// <summary>
        /// Converts a brokerage symbol to a Lean symbol instance
        /// </summary>
        /// <param name="brokerageSymbol">The brokerage symbol</param>
        /// <param name="securityType">The security type</param>
        /// <param name="market">The market</param>
        /// <param name="expirationDate">Expiration date of the security(if applicable)</param>
        /// <param name="strike">The strike of the security (if applicable)</param>
        /// <param name="optionRight">The option right of the security (if applicable)</param>
        /// <returns>A new Lean Symbol instance</returns>
        public Symbol GetLeanSymbol(string brokerageSymbol, SecurityType securityType, string market,
            DateTime expirationDate = new DateTime(), decimal strike = 0, OptionRight optionRight = OptionRight.Call)
        {
            return GetLeanSymbol(brokerageSymbol, securityType, market, OptionStyle.American, expirationDate, strike, optionRight);
        }

        /// <summary>
        /// Converts a brokerage symbol to a Lean symbol instance
        /// </summary>
        /// <param name="brokerageSymbol">The brokerage symbol</param>
        /// <param name="securityType">The security type</param>
        /// <param name="market">The market</param>
        /// <param name="optionStyle">The option style</param>
        /// <param name="expirationDate">Expiration date of the security(if applicable)</param>
        /// <param name="strike">The strike of the security (if applicable)</param>
        /// <param name="optionRight">The option right of the security (if applicable)</param>
        /// <param name="underlying">
        /// The underlying symbol of the security (if applicable). This is useful in cases like weekly index options (e.g. SPXW)
        /// where the brokerage symbol (e.g. O:SPXW240104C04685000) doesn't have information about the actual underlying symbol (SPX in this case).
        /// </param>
        /// <returns>A new Lean Symbol instance</returns>
        public Symbol GetLeanSymbol(string brokerageSymbol, SecurityType securityType, string market, OptionStyle optionStyle,
            DateTime expirationDate = new DateTime(), decimal strike = 0, OptionRight optionRight = OptionRight.Call,
            Symbol? underlying = null)
        {
            if (string.IsNullOrWhiteSpace(brokerageSymbol))
            {
                throw new ArgumentException("Invalid symbol: " + brokerageSymbol);
            }

            lock (_locker)
            {
                if (!_leanSymbolsCache.TryGetValue(brokerageSymbol, out var leanSymbol))
                {
                    var leanBaseSymbol = securityType == SecurityType.Equity ? null : GetLeanSymbol(brokerageSymbol);
                    var underlyingSymbolStr = underlying?.ID.Symbol ?? leanBaseSymbol?.Underlying.ID.Symbol;

                    switch (securityType)
                    {
                        case SecurityType.Option:
                            leanSymbol = Symbol.CreateOption(underlyingSymbolStr, market, optionStyle, optionRight, strike, expirationDate);
                            break;

                        case SecurityType.IndexOption:
                            underlying ??= Symbol.Create(leanBaseSymbol?.Underlying.ID.Symbol, SecurityType.Index, market);
                            leanSymbol = Symbol.CreateOption(underlying, leanBaseSymbol?.ID.Symbol, market, optionStyle, optionRight, strike, expirationDate);
                            break;

                        case SecurityType.Equity:
                            leanSymbol = Symbol.Create(brokerageSymbol, securityType, market);
                            break;

                        case SecurityType.Index:
                            leanSymbol = Symbol.Create(leanBaseSymbol?.ID.Symbol, securityType, market);
                            break;

                        default:
                            throw new Exception($"PolygonSymbolMapper.GetLeanSymbol(): unsupported security type: {securityType}");
                    }

                    _leanSymbolsCache[brokerageSymbol] = leanSymbol;
                    _brokerageSymbolsCache[leanSymbol] = brokerageSymbol;
                }

                return leanSymbol;
            }
        }

        /// <summary>
        /// Gets the Lean symbol for the specified Polygon symbol
        /// </summary>
        /// <param name="polygonSymbol">The polygon symbol</param>
        /// <returns>The corresponding Lean symbol</returns>
        /// <remarks>
        /// The mapped symbol might not be 100% accurate, since Polygon.io doesn't provide the full symbol information.
        /// For instance, for weekly index options (e.g. SPXW), Polygon.io doesn't provide the underlying symbol (SPX in this case).
        /// See <see cref="GetLeanSymbol(string, SecurityType, string, OptionStyle, DateTime, decimal, OptionRight, Symbol?)"/> for more details.
        /// </remarks>
        public Symbol GetLeanSymbol(string polygonSymbol)
        {
            lock (_locker)
            {
                if (!_leanSymbolsCache.TryGetValue(polygonSymbol, out var symbol))
                {
                    symbol = GetLeanSymbolInternal(polygonSymbol);
                }

                return symbol;
            }
        }

        private Symbol GetLeanSymbolInternal(string polygonSymbol)
        {
            if (polygonSymbol.StartsWith("O:"))
            {
                return GetLeanOptionSymbol(polygonSymbol);
            }
            else if (polygonSymbol.StartsWith("I:"))
            {
                return GetLeanIndexSymbol(polygonSymbol);
            }

            return GetLeanSymbol(polygonSymbol, SecurityType.Equity, Market.USA);
        }

        /// <summary>
        /// Gets the Lean option symbol for the specified Polygon symbol
        /// </summary>
        /// <param name="polygonSymbol">The polygon symbol</param>
        /// <returns>The corresponding Lean option symbol</returns>
        private Symbol GetLeanOptionSymbol(string polygonSymbol)
        {
            // Polygon option symbol format, without the "O:" prefix, is similar to OSI option symbol format
            // But they don't have a fixed number of characters for the underlying ticker, so we need to parse it
            // starting from the end of the string: strike -> option right -> expiration date -> underlying ticker.
            // Reference: https://polygon.io/blog/how-to-read-a-stock-options-ticker
            var strike = long.Parse(polygonSymbol.Substring(polygonSymbol.Length - 8)) / 1000m;
            var optionRight = polygonSymbol.Substring(polygonSymbol.Length - 9, 1) == "C" ? OptionRight.Call : OptionRight.Put;
            var expirationDate = DateTime.ParseExact(polygonSymbol.Substring(polygonSymbol.Length - 15, 6), "yyMMdd", CultureInfo.InvariantCulture);
            var ticker = polygonSymbol.Substring(2, polygonSymbol.Length - 15 - 2);

            var underlying = IndexOptionSymbol.IsIndexOption(ticker)
                ? Symbol.Create(IndexOptionSymbol.MapToUnderlying(ticker), SecurityType.Index, Market.USA)
                : Symbol.Create(ticker, SecurityType.Equity, Market.USA);

            return Symbol.CreateOption(underlying, ticker, Market.USA, OptionStyle.American, optionRight, strike, expirationDate);
        }

        /// <summary>
        /// Gets the Lean index symbol for the specified Polygon symbol
        /// </summary>
        private Symbol GetLeanIndexSymbol(string polygonSymbol)
        {
            return Symbol.Create(polygonSymbol.Substring(2), SecurityType.Index, Market.USA);
        }
    }
}
