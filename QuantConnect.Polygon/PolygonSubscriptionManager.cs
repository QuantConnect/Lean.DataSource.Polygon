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
using QuantConnect.Logging;
using QuantConnect.Util;

namespace QuantConnect.Polygon
{
    /// <summary>
    /// Multi-WebSocket Subscription Manager implementation for Polygon.io integration,
    /// based on <see cref="BrokerageMultiWebSocketSubscriptionManager"/>, which allows creating websockets
    /// categorized by security type since Polygon.io WebSocket API has a different URL for each security
    /// type.
    /// It also handles the number connections limits, having a maximum number of allowed websocket
    /// connections and a maximum number of allowed subscriptions per websocket.
    /// </summary>
    public partial class PolygonSubscriptionManager : BrokerageMultiWebSocketSubscriptionManager
    {
        private readonly int _maxWebSocketConnections;
        private readonly Dictionary<SecurityType, int> _maxSubscriptionsPerSecurityTypeWebSocket;

        private readonly Func<SecurityType, PolygonWebSocketClientWrapper> _webSocketFactory;

        /// <summary>
        /// Whether or not there is at least one open socket
        /// </summary>
        public bool IsConnected => _webSocketEntries.Any(x => x.WebSocket.IsOpen);

        public int WebSocketConnectionsCount => _webSocketEntries.Count(x => x.WebSocket.IsOpen);

        public int TotalSubscriptionsCount => _webSocketEntries.Sum(x => x.SymbolCount);

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonSubscriptionManager"/> class
        /// </summary>
        /// <param name="maxWebSocketConnections">The maximum number of subscriptions per websocket connection</param>
        /// <param name="maxSubscriptionsPerSecurityTypeWebSocket">
        /// The maximum number of subscriptions per websocket connection, which depend on the security type the websocket streams data for
        /// </param>
        /// <param name="webSocketFactory">A function which returns a new websocket instance</param>
        /// <param name="subscribeFunc">A function which subscribes a symbol</param>
        /// <param name="unsubscribeFunc">A function which unsubscribes a symbol</param>
        /// <param name="messageHandler">The websocket message handler</param>
        /// <param name="webSocketConnectionDuration">The maximum duration of the websocket connection, TimeSpan.Zero for no duration limit</param>
        /// <param name="connectionRateLimiter">The rate limiter for creating new websocket connections</param>
        public PolygonSubscriptionManager(
            int maxWebSocketConnections,
            Dictionary<SecurityType, int> maxSubscriptionsPerSecurityTypeWebSocket,
            Func<SecurityType, PolygonWebSocketClientWrapper> webSocketFactory,
            Func<IWebSocket, Symbol, bool> subscribeFunc,
            Func<IWebSocket, Symbol, bool> unsubscribeFunc,
            Action<WebSocketMessage> messageHandler,
            TimeSpan webSocketConnectionDuration,
            RateGate connectionRateLimiter = null)
            : base(null, 0, 0, null, null, subscribeFunc, unsubscribeFunc, messageHandler, webSocketConnectionDuration,
                  connectionRateLimiter)
        {
            _maxWebSocketConnections = maxWebSocketConnections;
            _maxSubscriptionsPerSecurityTypeWebSocket = maxSubscriptionsPerSecurityTypeWebSocket;
            _webSocketFactory = webSocketFactory;

            // Make sure no websockets are created, we will do it on demand
            _webSocketEntries.Clear();
        }

        /// <summary>
        /// Gets a websocket where a symbol can be subscribed in.
        /// </summary>
        protected override IWebSocket GetWebSocketForSymbol(Symbol symbol)
        {
            BrokerageMultiWebSocketEntry entry;

            lock (_locker)
            {
                var entriesForSecurityType = _webSocketEntries.Where(x => x.Symbols.Any(s => s.SecurityType == symbol.SecurityType)).ToList();
                // TODO: Throw if !_maxSubscriptionsPerSecurityTypeWebSocket.TryGetValue????
                var maxSubscriptions = _maxSubscriptionsPerSecurityTypeWebSocket[symbol.SecurityType];

                entry = entriesForSecurityType.FirstOrDefault(x => x.SymbolCount < maxSubscriptions);

                if (entry == null)
                {
                    if (_webSocketEntries.Count >= _maxWebSocketConnections)
                    {
                        throw new NotSupportedException($"Maximum symbol count reached for the current configuration [MaxSymbolsPerWebSocket={maxSubscriptions}, MaxWebSocketConnections:{_maxWebSocketConnections}]");
                    }

                    entry = new BrokerageMultiWebSocketEntry(new(), CreateWebSocket(symbol.SecurityType));
                    _webSocketEntries.Add(entry);
                }
            }

            if (!entry.WebSocket.IsOpen)
            {
                Connect(entry.WebSocket);
            }

            entry.AddSymbol(symbol);

            Log.Trace($"PolygonSubscriptionManager.GetWebSocketForSymbol(): added symbol: {symbol} to websocket: {entry.WebSocket.GetHashCode()} - Count: {entry.SymbolCount}");

            return entry.WebSocket;
        }

        /// <summary>
        /// Creates a new websocket using the provided factory method and subscribing some of its event handlers
        /// </summary>
        private IWebSocket CreateWebSocket(SecurityType securityType)
        {
            var webSocket = _webSocketFactory(securityType);
            webSocket.Open += OnOpen;
            webSocket.Message += EventHandler;

            return webSocket;
        }
    }
}
