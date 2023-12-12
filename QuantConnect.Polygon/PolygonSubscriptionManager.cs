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

using System.Runtime.CompilerServices;

using QuantConnect.Brokerages;
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
        // Each Polygon websocket endpoint has a different subscriptions limit
        private readonly Func<SecurityType, int> _maxSubscriptionsPerWebSocketFunc;

        /// <summary>
        /// Whether or not there is at least one open socket
        /// </summary>
        public bool IsConnected
        {
            get
            {
                lock (_locker)
                {
                    return _webSocketEntries.Any(x => x.WebSocket.IsOpen);
                }
            }
        }

        /// <summary>
        /// Gets the number of open websockets
        /// </summary>
        public int WebSocketConnectionsCount
        {
            get
            {
                lock (_locker)
                {
                    return _webSocketEntries.Count(x => x.WebSocket.IsOpen);
                }
            }
        }

        /// <summary>
        /// Gets the total number of subscriptions
        /// </summary>
        public int TotalSubscriptionsCount
        {
            get
            {
                lock (_locker)
                {
                    return _webSocketEntries.Sum(x => x.SymbolCount);
                }
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonSubscriptionManager"/> class
        /// </summary>
        /// <param name="maxWebSocketConnections">The maximum number of subscriptions per websocket connection</param>
        /// <param name="maxSubscriptionsPerWebSocketFunc">
        /// Function that gets the maximum number of subscriptions allowed for a security type websocket connection
        /// </param>
        /// <param name="webSocketFactory">A function which returns a new websocket instance</param>
        /// <param name="subscribeFunc">A function which subscribes a symbol</param>
        /// <param name="unsubscribeFunc">A function which unsubscribes a symbol</param>
        /// <param name="messageHandler">The websocket message handler</param>
        /// <param name="webSocketConnectionDuration">The maximum duration of the websocket connection, TimeSpan.Zero for no duration limit</param>
        /// <param name="connectionRateLimiter">The rate limiter for creating new websocket connections</param>
        public PolygonSubscriptionManager(
            int maxWebSocketConnections,
            Func<SecurityType, int> maxSubscriptionsPerWebSocketFunc,
            Func<Symbol, PolygonWebSocketClientWrapper> webSocketFactory,
            Func<IWebSocket, Symbol, bool> subscribeFunc,
            Func<IWebSocket, Symbol, bool> unsubscribeFunc,
            Action<WebSocketMessage> messageHandler,
            TimeSpan webSocketConnectionDuration,
            RateGate connectionRateLimiter = null)
            : base(null, 0, maxWebSocketConnections, null, webSocketFactory, subscribeFunc, unsubscribeFunc, messageHandler,
                  webSocketConnectionDuration, connectionRateLimiter)
        {
            _maxSubscriptionsPerWebSocketFunc = maxSubscriptionsPerWebSocketFunc;
        }

        /// <summary>
        /// Checks whether or not the websocket entry is full
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override bool IsWebSocketEntryFull(BrokerageMultiWebSocketEntry entry)
        {
            var securityTypes = (entry.WebSocket as PolygonWebSocketClientWrapper)?.SecurityTypes;
            return securityTypes != null && entry.SymbolCount >= _maxSubscriptionsPerWebSocketFunc(securityTypes[0]);
        }

        /// <summary>
        /// Checks whether or not the symbol can be added to the websocket entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override bool IsWebSocketEntryForSymbol(BrokerageMultiWebSocketEntry entry, Symbol symbol)
        {
            var securityTypes = (entry.WebSocket as PolygonWebSocketClientWrapper)?.SecurityTypes;
            return securityTypes != null && securityTypes.Contains(symbol.SecurityType);
        }
    }
}
