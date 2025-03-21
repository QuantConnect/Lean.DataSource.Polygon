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

using System.Text;
using System.Runtime.CompilerServices;
using Newtonsoft.Json.Linq;
using QuantConnect.Brokerages;
using QuantConnect.Data;
using QuantConnect.Logging;
using QuantConnect.Util;
using static QuantConnect.Brokerages.WebSocketClientWrapper;

namespace QuantConnect.Lean.DataSource.Polygon
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

        private List<SubscriptionDataConfig> _subscriptionsDataConfigs = new();

        /// <summary>
        /// Indicates whether data is being streamed using aggregates or ticks
        /// </summary>
        internal bool UsingAggregates { get; private set; }

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
        internal int TotalSubscriptionsCount
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
        /// <param name="maxSubscriptionsPerWebSocket">Maximum number of subscriptions allowed per websocket</param>
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
        /// Subscribes the specified configuration to the data feed
        /// </summary>
        /// <param name="config">The subscription data configuration to subscribe</param>
        public new void Subscribe(SubscriptionDataConfig config)
        {
            // We only store the subscription data config here to make it available
            // for the Subscribe(IEnumerable<Symbol> symbols, TickType tickType) method
            _subscriptionsDataConfigs.Add(config);
            base.Subscribe(config);
            _subscriptionsDataConfigs.Remove(config);
        }

        /// <summary>
        /// Subscribes to the symbols
        /// </summary>
        /// <param name="symbols">Symbols to subscribe</param>
        /// <param name="tickType">Type of tick data</param>
        protected override bool Subscribe(IEnumerable<Symbol> symbols, TickType tickType)
        {
            if (tickType == TickType.OpenInterest)
            {
                // Skip subscribing to OpenInterest and consider using PolygonOpenInterestProcessorManager instead, as Polygon doesn't support live updating of OpenInterest.
                return true;
            }

            Log.Trace($"PolygonSubscriptionManager.Subscribe(): {string.Join(",", symbols.Select(x => x.Value))}");

            foreach (var symbol in symbols)
            {
                var webSocket = GetWebSocket(symbol.SecurityType);

                if (webSocket == null)
                {
                    return false;
                }

                if (IsWebSocketFull(webSocket))
                {
                    throw new NotSupportedException("Maximum symbol count reached for the current configuration " +
                        $"[MaxSymbolsPerWebSocket={_maxSubscriptionsPerWebSocket}");
                }

                if (!webSocket.IsOpen)
                {
                    ConnectWebSocket(webSocket);
                }

                var config = _subscriptionsDataConfigs.Single(x => x.Symbol == symbol && x.TickType == tickType);
                webSocket.Subscribe(config, out var usingAggregates);
                UsingAggregates = usingAggregates;
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
                    webSocket.Unsubscribe(symbol, tickType);
                }
            }

            return true;
        }

        /// <summary>
        /// Adds a symbol to an existing or new websocket connection
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PolygonWebSocketClientWrapper GetWebSocket(SecurityType securityType)
        {
            return _webSockets.FirstOrDefault(x => x.SupportedSecurityTypes.Contains(securityType));
        }

        private void ConnectWebSocket(PolygonWebSocketClientWrapper webSocket)
        {
            using var authenticatedEvent = new AutoResetEvent(false);
            using var failedAuthenticationEvent = new AutoResetEvent(false);

            var error = new StringBuilder();
            EventHandler<WebSocketMessage> callback = (sender, e) =>
            {
                var data = (TextMessage)e.Data;
                var jsonMessage = JArray.Parse(data.Message)[0];
                var eventType = jsonMessage["ev"].ToString();
                if (eventType != "status")
                {
                    return;
                }

                var status = jsonMessage["status"]?.ToString() ?? string.Empty;
                var message = jsonMessage["message"]?.ToString() ?? string.Empty;
                if (status.Contains("auth_failed", StringComparison.InvariantCultureIgnoreCase))
                {
                    error.AppendLine($"Failed authentication: {message}");
                    failedAuthenticationEvent.Set();
                }
                else if (status.Contains("auth_success", StringComparison.InvariantCultureIgnoreCase))
                {
                    Log.Trace($"PolygonSubscriptionManager.ConnectWebSocket(): successful authentication.");
                    authenticatedEvent.Set();
                }
            };

            webSocket.Message += callback;
            webSocket.Connect();

            var result = WaitHandle.WaitAny(new[]{ failedAuthenticationEvent, authenticatedEvent  }, TimeSpan.FromSeconds(60));
            webSocket.Message -= callback;

            if (result == WaitHandle.WaitTimeout)
            {
                throw new TimeoutException("Timeout waiting for websocket to connect.");
            }

            if (result == 0)
            {
                webSocket.Close();
                _webSockets.Remove(webSocket);
                throw new PolygonAuthenticationException($"WebSocket authentication failed for security types: {string.Join(", ", webSocket.SupportedSecurityTypes)}. {error?.ToString()}");
            }

            Log.Trace($"PolygonSubscriptionManager.ConnectWebSocket(): Successfully connected websocket.");
        }

        /// <summary>
        /// Channel name
        /// </summary>
        /// <param name="tickType">Type of tick data</param>
        /// <returns>Returns Socket channel name corresponding <paramref name="tickType"/></returns>
        protected override string ChannelNameFromTickType(TickType tickType)
        {
            return tickType.ToString();
        }

        /// <summary>
        /// Checks whether or not the websocket entry is full
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsWebSocketFull(PolygonWebSocketClientWrapper websocket)
        {
            return _maxSubscriptionsPerWebSocket > 0 && websocket.SubscriptionsCount >= _maxSubscriptionsPerWebSocket;
        }
    }
}
