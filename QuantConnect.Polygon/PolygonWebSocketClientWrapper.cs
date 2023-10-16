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

using Newtonsoft.Json;
using QuantConnect.Brokerages;
using QuantConnect.Logging;

namespace QuantConnect.Polygon
{
    /// <summary>
    /// WebSocket client wrapper for Polygon.io
    /// </summary>
    public class PolygonWebSocketClientWrapper : WebSocketClientWrapper
    {
        private const string BaseUrl = "wss://launchpad.polygon.io";

        private readonly string _apiKey;
        private readonly ISymbolMapper _symbolMapper;
        private readonly Action<string> _messageHandler;

        public SecurityType SecurityType { get; }

        /// <summary>
        /// Creates a new instance of the <see cref="PolygonWebSocketClientWrapper"/> class
        /// </summary>
        /// <param name="apiKey">The Polygon.io API key</param>
        /// <param name="symbolMapper">The symbol mapper</param>
        /// <param name="securityType">The security type</param>
        /// <param name="messageHandler">The message handler</param>
        public PolygonWebSocketClientWrapper(string apiKey, ISymbolMapper symbolMapper, SecurityType securityType, Action<string> messageHandler)
        {
            _apiKey = apiKey;
            _symbolMapper = symbolMapper;
            SecurityType = securityType;
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
            if (tickType != TickType.Trade)
            {
                throw new Exception($"Unsupported tick type: {tickType}");
            }

            Subscribe(symbol, tickType, true);
        }

        /// <summary>
        /// Unsubscribes the given symbol
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <param name="tickType">Type of tick data</param>
        public void Unsubscribe(Symbol symbol, TickType tickType)
        {
            Subscribe(symbol, tickType, false);
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
            Log.Trace($"PolygonWebSocketClientWrapper.OnClosed(): {SecurityType} - {e.Reason}");
        }

        private void OnOpen(object? sender, EventArgs e)
        {
            Log.Trace($"PolygonWebSocketClientWrapper.OnOpen(): {SecurityType} - connection open");

            Send(JsonConvert.SerializeObject(new
            {
                action = "auth",
                @params = _apiKey
            }));
        }

        private string GetSubscriptionPrefix(SecurityType securityType, TickType tickType)
        {
            switch (securityType)
            {
                case SecurityType.Option:
                    // Only support aggregated minute data for options
                    return "AM";

                default:
                    throw new Exception($"Unsupported security type: {securityType}");
            }
        }

        public static string GetWebSocketUrl(SecurityType securityType)
        {
            switch (securityType)
            {
                case SecurityType.Option:
                    return BaseUrl + "/options";

                default:
                    throw new Exception($"Unsupported security type: {securityType}");
            }
        }
    }
}
