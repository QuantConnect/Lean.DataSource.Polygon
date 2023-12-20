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

using System.Collections.ObjectModel;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Logging;

namespace QuantConnect.Polygon
{
    /// <summary>
    /// WebSocket client wrapper for Polygon.io
    /// </summary>
    public class PolygonWebSocketClientWrapper : WebSocketClientWrapper
    {
        private static string BaseUrl = Config.Get("polygon-ws-url", "wss://socket.polygon.io");

        private readonly string _apiKey;
        private PolygonSubscriptionPlan _subscriptionPlan;
        private readonly ISymbolMapper _symbolMapper;
        private readonly Action<string> _messageHandler;
        private volatile bool _authenticated;

        private object _lock = new();

        private readonly List<SecurityType> _supportedSecurityTypes;

        private readonly Dictionary<Symbol, List<TickType>> _subscriptions;

        /// <summary>
        /// On Authenticated event
        /// </summary>
        public event EventHandler Authenticated;

        /// <summary>
        /// Gets the security types supported by this websocket client
        /// </summary>
        public ReadOnlyCollection<SecurityType> SupportedSecurityTypes => _supportedSecurityTypes.AsReadOnly();

        /// <summary>
        /// The number of current subscriptions for this websocket
        /// </summary>
        public int SubscriptionsCount
        {
            get
            {
                lock (_lock)
                {
                    return _subscriptions.Sum(kvp => kvp.Value.Count);
                }
            }
        }

        /// <summary>
        /// Creates a new instance of the <see cref="PolygonWebSocketClientWrapper"/> class
        /// </summary>
        /// <param name="apiKey">The Polygon.io API key</param>
        /// <param name="subscriptionPlan">Polygon subscription plan</param>
        /// <param name="symbolMapper">The symbol mapper</param>
        /// <param name="securityType">The security type</param>
        /// <param name="messageHandler">The message handler</param>
        public PolygonWebSocketClientWrapper(string apiKey,
            PolygonSubscriptionPlan subscriptionPlan,
            ISymbolMapper symbolMapper,
            SecurityType securityType,
            Action<string> messageHandler)
        {
            _apiKey = apiKey;
            _subscriptionPlan = subscriptionPlan;
            _symbolMapper = symbolMapper;
            _supportedSecurityTypes = GetSupportedSecurityTypes(securityType);
            _messageHandler = messageHandler;
            _subscriptions = new Dictionary<Symbol, List<TickType>>();

            var url = GetWebSocketUrl(securityType);
            Initialize(url);

            Open += OnOpen;
            Closed += OnClosed;
            Message += OnMessage;
            Error += OnError;
            Authenticated += OnAuthenticated;
        }

        /// <summary>
        /// Subscribes the given symbol
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <param name="tickType">Type of tick data</param>
        public void Subscribe(Symbol symbol, TickType tickType)
        {
            if (_subscriptionPlan == PolygonSubscriptionPlan.Basic)
            {
                throw new NotSupportedException("Basic plan does not support streaming data");
            }

            Subscribe(symbol, tickType, true);
            AddSubscription(symbol, tickType);
        }

        /// <summary>
        /// Unsubscribes the given symbol
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <param name="tickType">Type of tick data</param>
        public void Unsubscribe(Symbol symbol, TickType tickType)
        {
            Subscribe(symbol, tickType, false);
            RemoveSubscription(symbol, tickType);
        }

        private void Subscribe(Symbol symbol, TickType tickType, bool subscribe)
        {
            var ticker = _symbolMapper.GetBrokerageSymbol(symbol);
            Send(JsonConvert.SerializeObject(new
            {
                action = subscribe ? "subscribe" : "unsubscribe",
                @params = $"{GetSubscriptionPrefix(symbol.SecurityType, tickType)}.{ticker}"
            }));
        }

        private void AddSubscription(Symbol symbol, TickType tickType)
        {
            lock (_lock)
            {
                if (!_subscriptions.TryGetValue(symbol, out var tickTypes))
                {
                    tickTypes = new List<TickType>();
                    _subscriptions[symbol] = tickTypes;
                }
                tickTypes.Add(tickType);
            }
        }

        private void RemoveSubscription(Symbol symbol, TickType tickType)
        {
            lock (_lock)
            {
                if (_subscriptions.TryGetValue(symbol, out var tickTypes))
                {
                    tickTypes.Remove(tickType);
                    if (tickTypes.Count == 0)
                    {
                        _subscriptions.Remove(symbol);
                    }
                }
            }
        }

        private void OnError(object? sender, WebSocketError e)
        {
            Log.Error($"PolygonWebSocketClientWrapper.OnError(): {e.Message}");
        }

        private void OnMessage(object? sender, WebSocketMessage webSocketMessage)
        {
            var e = (TextMessage)webSocketMessage.Data;

            if (!_authenticated)
            {
                // Find the authentication message
                var authenticationMessage = JArray.Parse(e.Message)
                    .FirstOrDefault(message => message["ev"].ToString() == "status" && message["status"].ToString() == "auth_success");
                if (authenticationMessage != null)
                {
                    Authenticated?.Invoke(this, EventArgs.Empty);
                }
            }

            _messageHandler?.Invoke(e.Message);
        }

        private void OnClosed(object? sender, WebSocketCloseData e)
        {
            _authenticated = false;
            Log.Trace($"PolygonWebSocketClientWrapper.OnClosed(): {string.Join(", ", _supportedSecurityTypes)} - {e.Reason}");
        }

        private void OnOpen(object? sender, EventArgs e)
        {
            Log.Trace($"PolygonWebSocketClientWrapper.OnOpen(): {string.Join(", ", _supportedSecurityTypes)} - connection open");

            Send(JsonConvert.SerializeObject(new
            {
                action = "auth",
                @params = _apiKey
            }));
        }

        private void OnAuthenticated(object? sender, EventArgs e)
        {
            _authenticated = true;
            Log.Trace($"PolygonWebSocketClientWrapper.OnAuthenticated(): {string.Join(", ", _supportedSecurityTypes)} - authenticated");

            lock (_lock)
            {
                foreach (var kvp in _subscriptions)
                {
                    foreach (var tickType in kvp.Value)
                    {
                        Log.Trace($"PolygonWebSocketClientWrapper.OnAuthenticated(): {string.Join(", ", _supportedSecurityTypes)} - resubscribing {kvp.Key} - {tickType}");
                        Subscribe(kvp.Key, tickType, true);
                    }
                }
            }
        }

        private string GetSubscriptionPrefix(SecurityType securityType, TickType tickType)
        {
            switch (securityType)
            {
                case SecurityType.Equity:
                case SecurityType.Option:
                case SecurityType.IndexOption:
                    switch (_subscriptionPlan)
                    {
                        case PolygonSubscriptionPlan.Starter:
                            return "A";
                        case PolygonSubscriptionPlan.Developer:
                            return "T";
                        case PolygonSubscriptionPlan.Advanced:
                            return tickType == TickType.Trade ? "T" : "Q";
                        default:
                            throw new Exception($"Unsupported subscription plan: {_subscriptionPlan}");
                    }

                default:
                    throw new NotSupportedException($"Unsupported security type: {securityType}");
            }
        }

        public static string GetWebSocketUrl(SecurityType securityType)
        {
            switch (securityType)
            {
                case SecurityType.Equity:
                    return BaseUrl + "/stocks";

                case SecurityType.Option:
                case SecurityType.IndexOption:
                    return BaseUrl + "/options";

                default:
                    throw new Exception($"Unsupported security type: {securityType}");
            }
        }

        private static List<SecurityType> GetSupportedSecurityTypes(SecurityType securityType)
        {
            switch (securityType)
            {
                case SecurityType.Equity:
                    return new List<SecurityType> { securityType };

                case SecurityType.Option:
                case SecurityType.IndexOption:
                    return new List<SecurityType> { SecurityType.Option, SecurityType.IndexOption };

                default:
                    throw new Exception($"Unsupported security type: {securityType}");
            }
        }
    }
}
