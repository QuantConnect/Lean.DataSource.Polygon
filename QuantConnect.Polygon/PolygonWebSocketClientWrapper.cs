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

        private List<SecurityType> _supportedSecurityTypes;

        /// <summary>
        /// Gets the security types supported by this websocket client
        /// </summary>
        public ReadOnlyCollection<SecurityType> SupportedSecurityTypes => _supportedSecurityTypes.AsReadOnly();

        /// <summary>
        /// The number of current subscriptions for this websocket
        /// </summary>
        public int SubscriptionsCount { get; private set; }

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

            var url = GetWebSocketUrl(securityType);
            Initialize(url);

            Open += OnOpen;
            Closed += OnClosed;
            Message += OnMessage;
            Error += OnError;
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
            SubscriptionsCount++;
        }

        /// <summary>
        /// Unsubscribes the given symbol
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <param name="tickType">Type of tick data</param>
        public void Unsubscribe(Symbol symbol, TickType tickType)
        {
            Subscribe(symbol, tickType, false);
            SubscriptionsCount--;
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

        private void OnError(object? sender, WebSocketError e)
        {
            Log.Error($"PolygonWebSocketClientWrapper.OnError(): {e.Message}");
        }

        private void OnMessage(object? sender, WebSocketMessage webSocketMessage)
        {
            var e = (TextMessage)webSocketMessage.Data;
            _messageHandler?.Invoke(e.Message);
        }

        private void OnClosed(object? sender, WebSocketCloseData e)
        {
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

        private string GetSubscriptionPrefix(SecurityType securityType, TickType tickType)
        {
            if (tickType == TickType.OpenInterest)
            {
                throw new NotSupportedException("Open interest data is not supported");
            }

            if (_subscriptionPlan < PolygonSubscriptionPlan.Advanced && tickType != TickType.Trade)
            {
                throw new NotSupportedException("Plans below Advanced only support streaming trade data");
            }

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
