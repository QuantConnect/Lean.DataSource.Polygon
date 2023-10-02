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
using QuantConnect.Util;
using QuantConnect.Logging;
using QuantConnect.Configuration;
using QuantConnect.Brokerages;
using static QuantConnect.Brokerages.WebSocketClientWrapper;

namespace QuantConnect.Polygon
{
    /// <summary>
    /// Handles Polygon data subscriptions with multiple websocket connections
    /// </summary>
    public class PolygonMultiWebSocketSubscriptionManager : EventBasedDataQueueHandlerSubscriptionManager, IDisposable
    {
        private const int ConnectionTimeout = 30000;

        private readonly Func<SecurityType, PolygonWebSocketClientWrapper> _webSocketFactory;
        private readonly Action<string> _messageHandler;

        private readonly int _maximumWebSocketConnections = Config.GetInt("polygon-max-websocket-connections", 7);
        private readonly int _maximumSubscriptionsPerWebSocket = Config.GetInt("polygon-max-subscriptions-per-websocket", 1000);
        private readonly TimeSpan _webSocketConnectionDuration = TimeSpan.Zero;
        private readonly System.Timers.Timer _reconnectTimer;

        private readonly object _lock = new();
        private readonly Dictionary<SecurityType, List<PolygonMultiWebSocketEntry>> _webSockets = new();

        /// <summary>
        /// Returns whether the data provider is connected
        /// </summary>
        /// <returns>True if the data provider is connected</returns>
        public bool IsConnected => _webSockets.Values.Any(entries => entries.Count > 0) &&
            _webSockets.Values.All(clients => clients.All(client => client.WebSocket.IsOpen));

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonMultiWebSocketSubscriptionManager"/> class
        /// </summary>
        /// <param name="webSocketUrl">The URL for websocket connections</param>
        /// <param name="maximumSymbolsPerWebSocket">The maximum number of symbols per websocket connection</param>
        /// <param name="maximumWebSocketConnections">The maximum number of websocket connections allowed (if zero, symbol weighting is disabled)</param>
        /// <param name="symbolWeights">A dictionary for the symbol weights</param>
        /// <param name="webSocketFactory">A function which returns a new websocket instance</param>
        /// <param name="subscribeFunc">A function which subscribes a symbol</param>
        /// <param name="unsubscribeFunc">A function which unsubscribes a symbol</param>
        /// <param name="messageHandler">The websocket message handler</param>
        /// <param name="webSocketConnectionDuration">The maximum duration of the websocket connection, TimeSpan.Zero for no duration limit</param>
        /// <param name="connectionRateLimiter">The rate limiter for creating new websocket connections</param>
        public PolygonMultiWebSocketSubscriptionManager(
            List<SecurityType> supportedSecurityTypes,
            Func<SecurityType, PolygonWebSocketClientWrapper> webSocketFactory,
            Action<string> messageHandler,
            int maximumSymbolsPerWebSocket,
            int maximumWebSocketConnections,
            TimeSpan webSocketConnectionDuration)
        {
            _maximumSubscriptionsPerWebSocket = maximumSymbolsPerWebSocket;
            _maximumWebSocketConnections = maximumWebSocketConnections;
            _webSocketConnectionDuration = webSocketConnectionDuration;

            _webSocketFactory = webSocketFactory;
            _messageHandler = messageHandler;

            for (var i = 0; i < supportedSecurityTypes.Count; i++)
            {
                var securityType = supportedSecurityTypes[i];
                var entries = new List<PolygonMultiWebSocketEntry>();

                for (var j = 0; j < _maximumWebSocketConnections; j++)
                {
                    entries.Add(new PolygonMultiWebSocketEntry(CreateWebSocket(securityType)));
                }

                _webSockets[securityType] = entries;
            }

            // Some exchanges (e.g. Binance) require a daily restart for websocket connections
            if (webSocketConnectionDuration != TimeSpan.Zero)
            {
                _reconnectTimer = new System.Timers.Timer
                {
                    Interval = webSocketConnectionDuration.TotalMilliseconds
                };
                _reconnectTimer.Elapsed += (_, _) =>
                {
                    Log.Trace("PolygonMultiWebSocketSubscriptionManager(): Restarting websocket connections");

                    lock (_lock)
                    {
                        foreach (var kvp in _webSockets)
                        {
                            foreach (var entry in kvp.Value)
                            {
                                if (entry.WebSocket.IsOpen)
                                {
                                    Task.Factory.StartNew(() =>
                                    {
                                        Log.Trace($"PolygonMultiWebSocketSubscriptionManager(): Websocket restart - disconnect: ({entry.WebSocket.GetHashCode()})");
                                        Disconnect(entry.WebSocket);

                                        Log.Trace($"PolygonMultiWebSocketSubscriptionManager(): Websocket restart - connect: ({entry.WebSocket.GetHashCode()})");
                                        Connect(entry.WebSocket);
                                    });
                                }
                            }
                        }
                    }
                };
                _reconnectTimer.Start();

                Log.Trace($"PolygonMultiWebSocketSubscriptionManager(): WebSocket connections will be restarted every: {webSocketConnectionDuration}");
            }
        }

        /// <summary>
        /// Subscribes to the symbols
        /// </summary>
        /// <param name="symbols">Symbols to subscribe</param>
        /// <param name="tickType">Type of tick data</param>
        protected override bool Subscribe(IEnumerable<Symbol> symbols, TickType tickType)
        {
            Log.Trace($"PolygonMultiWebSocketSubscriptionManager.Subscribe(): {string.Join(",", symbols.Select(x => x.Value))}");

            foreach (var symbol in symbols)
            {
                var webSocket = GetWebSocket(symbol, tickType);
                webSocket.Subscribe(symbol, tickType);
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
            Log.Trace($"PolygonMultiWebSocketSubscriptionManager.Unsubscribe(): {string.Join(",", symbols.Select(x => x.Value))}");

            foreach (var symbol in symbols)
            {
                var entry = GetWebSocketEntry(symbol, tickType);
                if (entry != null)
                {
                    entry.RemoveSubscription(symbol, tickType);
                    entry.WebSocket.Unsubscribe(symbol, tickType);
                }
            }

            return true;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public override void Dispose()
        {
            _reconnectTimer?.Stop();
            _reconnectTimer.DisposeSafely();
            lock (_lock)
            {
                foreach (var kvp in _webSockets)
                {
                    foreach (var entry in kvp.Value)
                    {
                        try
                        {
                            entry.WebSocket.Open -= OnOpen;
                            entry.WebSocket.Message -= OnMessage;
                            entry.WebSocket.Close();
                        }
                        catch (Exception ex)
                        {
                            Log.Error(ex);
                        }
                    }
                }
                _webSockets.Clear();
            }
        }

        private PolygonMultiWebSocketEntry GetWebSocketEntry(Symbol symbol, TickType tickType)
        {
            lock (_lock)
            {
                if (_webSockets.TryGetValue(symbol.SecurityType, out var webSocketEntries))
                {
                    return webSocketEntries.FirstOrDefault(entry => entry.Contains(symbol, tickType));
                }
            }

            return null;
        }

        /// <summary>
        /// Adds a symbol to an existing or new websocket connection
        /// </summary>
        private PolygonWebSocketClientWrapper GetWebSocket(Symbol symbol, TickType tickType)
        {
            PolygonMultiWebSocketEntry entry;

            lock (_lock)
            {
                if (!_webSockets.TryGetValue(symbol.SecurityType, out var webSocketEntries))
                {
                    throw new NotSupportedException($"Security type not supported: {symbol.SecurityType}");
                }

                entry = webSocketEntries.FirstOrDefault(webSocketEntry => webSocketEntry.SubscriptionCount < _maximumSubscriptionsPerWebSocket);
                if (entry == null)
                {
                    if (_maximumWebSocketConnections > 0)
                    {
                        throw new NotSupportedException($@"Maximum symbol count reached for the current configuration [MaxSymbolsPerWebSocket={
                            _maximumSubscriptionsPerWebSocket}, MaxWebSocketConnections:{_maximumWebSocketConnections}]");
                    }

                    // symbol limit reached on all, create new websocket instance
                    entry = new PolygonMultiWebSocketEntry(CreateWebSocket(symbol.SecurityType));
                    webSocketEntries.Add(entry);
                }
            }

            if (!entry.WebSocket.IsOpen)
            {
                Connect(entry.WebSocket);
            }

            entry.AddSubscription(symbol, tickType);

            Log.Trace($"PolygonMultiWebSocketSubscriptionManager.GetWebSocketForSymbol(): added symbol: {symbol} to websocket: {entry.WebSocket.GetHashCode()} - Count: {entry.SubscriptionCount}");

            return entry.WebSocket;
        }

        /// <summary>
        /// When we create a websocket we will subscribe to it's events once and initialize it
        /// </summary>
        /// <remarks>Note that the websocket is no connected yet <see cref="Connect(IWebSocket)"/></remarks>
        private PolygonWebSocketClientWrapper CreateWebSocket(SecurityType securityType)
        {
            var webSocket = _webSocketFactory(securityType);
            webSocket.Open += OnOpen;
            webSocket.Message += OnMessage;

            return webSocket;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void OnMessage(object? _, WebSocketMessage message)
        {
            var e = (TextMessage)message.Data;
            _messageHandler?.Invoke(e.Message);
        }

        private void Connect(IWebSocket webSocket)
        {
            var connectedEvent = new ManualResetEvent(false);
            EventHandler onOpenAction = (_, _) => connectedEvent.Set();

            webSocket.Open += onOpenAction;

            try
            {
                webSocket.Connect();

                if (!connectedEvent.WaitOne(ConnectionTimeout))
                {
                    throw new TimeoutException($"PolygonMultiWebSocketSubscriptionManager.Connect(): WebSocket connection timeout: {webSocket.GetHashCode()}");
                }
            }
            finally
            {
                webSocket.Open -= onOpenAction;
                connectedEvent.DisposeSafely();
            }
        }

        private void Disconnect(IWebSocket webSocket)
        {
            webSocket.Close();
        }

        private void OnOpen(object? sender, EventArgs e)
        {
            var webSocket = (IWebSocket)sender;

            lock (_lock)
            {
                foreach (var kvp in _webSockets)
                {
                    foreach (var entry in kvp.Value)
                    {
                        if (entry.WebSocket == webSocket && entry.SubscriptionCount > 0)
                        {
                            var subscriptions = entry.Subscriptions;
                            Log.Trace($@"PolygonMultiWebSocketSubscriptionManager.Connect(): WebSocket opened: {webSocket.GetHashCode()
                                } - Resubscribing existing symbols: {subscriptions.Count}");

                            Task.Factory.StartNew(() =>
                            {
                                foreach (var subscription in subscriptions)
                                {
                                    entry.WebSocket.Subscribe(subscription.Symbol, subscription.TickType);
                                }
                            });
                        }
                    }
                }
            }
        }

        internal int WebSocketsCount => _webSockets.Values.SelectMany(x => x).Count(x => x.SubscriptionCount > 0);

        internal int SubscriptionsCount => _webSockets.Values.Sum(x => x.Sum(y => y.SubscriptionCount));
    }
}
