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

using QuantConnect.Data;
using QuantConnect.Logging;
using QuantConnect.Util;

namespace QuantConnect.Polygon
{
    /// <summary>
    /// Subscription manager to handle the subscriptions for the Polygon data queue handler.
    /// It can handle one websocket connection per supported security type, with an optional
    /// limit on the number of subscriptions per websocket connection.
    /// </summary>
    public class PolygonSubscriptionManager : EventBasedDataQueueHandlerSubscriptionManager
    {
        private int _maxSubscriptionsPerWebSocket;
        private List<PolygonWebSocketClientWrapper> _webSockets;
        private object _lock = new();

        /// <summary>
        /// Whether or not there is at least one open socket
        /// </summary>
        public bool IsConnected
        {
            get
            {
                lock (_lock)
                {
                    return _webSockets.Any(x => x.IsOpen);
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
                lock (_lock)
                {
                    return _webSockets.Sum(x => x.SubscriptionsCount);
                }
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonSubscriptionManager"/> class
        /// </summary>
        /// <param name="securityTypes">The supported security types</param>
        /// <param name="subscriptionPlan">Polygon subscription plan</param>
        /// <param name="websSocketFactory">Function to create websockets</param>
        public PolygonSubscriptionManager(
            IEnumerable<SecurityType> securityTypes,
            int maxSubscriptionsPerWebSocket,
            Func<SecurityType, PolygonWebSocketClientWrapper> websSocketFactory)
        {
            _maxSubscriptionsPerWebSocket = maxSubscriptionsPerWebSocket;
            _webSockets = new List<PolygonWebSocketClientWrapper>();
            foreach (var securityType in securityTypes)
            {
                var webSocket = GetWebSocket(securityType);
                // Some security types are supported by a single websocket connection, like options and index options
                if (webSocket == null)
                {
                    _webSockets.Add(websSocketFactory(securityType));
                }
            }
        }

        /// <summary>
        /// Subscribes to the symbols
        /// </summary>
        /// <param name="symbols">Symbols to subscribe</param>
        /// <param name="tickType">Type of tick data</param>
        protected override bool Subscribe(IEnumerable<Symbol> symbols, TickType tickType)
        {
            Log.Trace($"PolygonSubscriptionManager.Subscribe(): {string.Join(",", symbols.Select(x => x.Value))}");

            foreach (var symbol in symbols)
            {
                var webSocket = GetWebSocket(symbol.SecurityType);
                if (IsWebSocketFull(webSocket))
                {
                    throw new NotSupportedException("Maximum symbol count reached for the current configuration " +
                        $"[MaxSymbolsPerWebSocket={_maxSubscriptionsPerWebSocket}");
                }
                if (!webSocket.IsOpen)
                {
                    webSocket.Connect();
                }
                webSocket.Subscribe(symbol, TickType.Trade);
            }

            return true;
        }

        /// <summary>
        /// Unsubscribes from the symbols
        /// </summary>
        /// <param name="symbols">Symbols to subscribe</param>
        /// <param name="tickType">Type of tick data</param>
        protected override bool Unsubscribe(IEnumerable<Symbol> symbols, TickType tickType)
        {
            Log.Trace($"PolygonSubscriptionManager.Unsubscribe(): {string.Join(",", symbols.Select(x => x.Value))}");

            foreach (var symbol in symbols)
            {
                var webSocket = GetWebSocket(symbol.SecurityType);
                if (webSocket != null)
                {
                    webSocket.Unsubscribe(symbol, TickType.Trade);
                }
            }

            return true;
        }

        /// <summary>
        /// Adds a symbol to an existing or new websocket connection
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private PolygonWebSocketClientWrapper GetWebSocket(SecurityType securityType)
        {
            return _webSockets.FirstOrDefault(x => x.SupportedSecurityTypes.Contains(securityType));
        }

        /// <summary>
        /// Checks whether or not the websocket entry is full
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsWebSocketFull(PolygonWebSocketClientWrapper websocket)
        {
            var securityTypes = websocket.SupportedSecurityTypes;
            return _maxSubscriptionsPerWebSocket > 0 && websocket.SubscriptionsCount >= _maxSubscriptionsPerWebSocket;
        }
    }
}
