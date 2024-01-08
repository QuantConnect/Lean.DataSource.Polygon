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
using Newtonsoft.Json.Linq;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Logging;
using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;

namespace QuantConnect.Polygon
{
    /// <summary>
    /// WebSocket client wrapper for Polygon.io
    /// </summary>
    public class PolygonWebSocketClientWrapper : WebSocketClientWrapper
    {
        private static string BaseUrl = Config.Get("polygon-ws-url", "wss://socket.polygon.io");

        private readonly string _apiKey;
        private readonly ISymbolMapper _symbolMapper;
        private readonly Action<string> _messageHandler;

        private object _lock = new();

        private readonly List<SecurityType> _supportedSecurityTypes;

        private readonly List<string> _subscriptions;

        private Dictionary<(SecurityType, TickType), string> _prefixes;

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
                    return _subscriptions.Count;
                }
            }
        }

        /// <summary>
        /// Creates a new instance of the <see cref="PolygonWebSocketClientWrapper"/> class
        /// </summary>
        /// <param name="apiKey">The Polygon.io API key</param>
        /// <param name="symbolMapper">The symbol mapper</param>
        /// <param name="securityType">The security type</param>
        /// <param name="messageHandler">The message handler</param>
        public PolygonWebSocketClientWrapper(string apiKey,
            ISymbolMapper symbolMapper,
            SecurityType securityType,
            Action<string> messageHandler)
        {
            _apiKey = apiKey;
            _symbolMapper = symbolMapper;
            _supportedSecurityTypes = GetSupportedSecurityTypes(securityType);
            _messageHandler = messageHandler;
            _subscriptions = new();
            _prefixes = new();

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
        public void Subscribe(SubscriptionDataConfig config, out bool usingAggregates)
        {
            var ticker = _symbolMapper.GetBrokerageSymbol(config.Symbol);

            // If prefix is already known, use it
            if (_prefixes.TryGetValue((config.SecurityType, config.TickType), out var prefix))
            {
                var subscriptionTicker = MakeSubscriptionTicker(prefix, ticker);
                Subscribe(subscriptionTicker, true);
                AddSubscription(subscriptionTicker);
                usingAggregates = prefix == "A";

                Log.Trace($"PolygonWebSocketClientWrapper.Subscribe(): Subscribed to {subscriptionTicker}");
            }
            else
            {
                TrySubscribe(ticker, config, out usingAggregates);
            }
        }

        /// <summary>
        /// Tries to subscribe to the given symbol and tick type by trying all possible prefixes
        /// </summary>
        private void TrySubscribe(string ticker, SubscriptionDataConfig config, out bool usingAggregates)
        {
            usingAggregates = false;

            // We'll try subscribing assuming the highest subscription plan and work our way down if we get an error
            using var subscribedEvent = new ManualResetEventSlim(false);
            using var errorEvent = new ManualResetEventSlim(false);

            void ProcessMessage(object? _, WebSocketMessage wsMessage)
            {
                var jsonMessage = JArray.Parse(((TextMessage)wsMessage.Data).Message)[0];
                var eventType = jsonMessage["ev"]?.ToString() ?? string.Empty;
                if (eventType != "status")
                {
                    return;
                }

                var status = jsonMessage["status"]?.ToString() ?? string.Empty;
                var message = jsonMessage["message"]?.ToString() ?? string.Empty;
                if (status.Contains("success", StringComparison.InvariantCultureIgnoreCase) &&
                    message.Contains("subscribed to", StringComparison.InvariantCultureIgnoreCase))
                {
                    subscribedEvent.Set();
                }
                else
                {
                    Log.Debug($"PolygonWebSocketClientWrapper.Subscribe(): error: '{message}'.");
                    errorEvent.Set();
                }
            }

            Message += ProcessMessage;

            var waitHandles = new WaitHandle[] { subscribedEvent.WaitHandle, errorEvent.WaitHandle };
            var subscribed = false;
            var triedSubscription = false;

            foreach (var protentialPrefix in GetSubscriptionPefixes(config.SecurityType, config.TickType, config.Resolution))
            {
                triedSubscription = true;
                subscribedEvent.Reset();
                errorEvent.Reset();

                var subscriptionTicker = MakeSubscriptionTicker(protentialPrefix, ticker);
                Subscribe(subscriptionTicker, true);

                // Wait for the subscribed event or error event
                var index = WaitHandle.WaitAny(waitHandles, TimeSpan.FromSeconds(30));

                // Quickly try the next prefix if we get an error or timeout
                if (index != 0)
                {
                    continue;
                }

                Log.Trace($"PolygonWebSocketClientWrapper.Subscribe(): Subscribed to {subscriptionTicker}");
                // Subscription was successful
                AddSubscription(subscriptionTicker);
                _prefixes[(config.SecurityType, config.TickType)] = protentialPrefix;
                usingAggregates = protentialPrefix == "A";
                subscribed = true;
                break;
            }

            Message -= ProcessMessage;

            if (triedSubscription && !subscribed)
            {
                throw new Exception($"PolygonWebSocketClientWrapper.Subscribe(): Failed to subscribe to {ticker}. " +
                    $"Make sure your subscription plan allows streaming {config.TickType.ToString().ToLowerInvariant()} data.");
            }
        }

        /// <summary>
        /// Unsubscribes the given symbol
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <param name="tickType">Type of tick data</param>
        public void Unsubscribe(Symbol symbol, TickType tickType)
        {
            var baseTicker = _symbolMapper.GetBrokerageSymbol(symbol);
            foreach (var prefix in GetSubscriptionPefixes(symbol.SecurityType, tickType))
            {
                var ticker = MakeSubscriptionTicker(prefix, baseTicker);
                lock (_lock)
                {
                    if (RemoveSubscription(ticker))
                    {
                        Subscribe(ticker, false);
                        break;
                    }
                }
            }
        }

        private void Subscribe(string ticker, bool subscribe)
        {
            Send(JsonConvert.SerializeObject(new
            {
                action = subscribe ? "subscribe" : "unsubscribe",
                @params = ticker
            }));
        }

        /// <summary>
        /// Gets a list of Polygon WebSocket prefixes supported for the given tick type and resolution
        /// </summary>
        private IEnumerable<string> GetSubscriptionPefixes(SecurityType securityType ,TickType tickType, Resolution resolution = Resolution.Minute)
        {
            // If we already know the prefix, return it and don't try any others
            if (_prefixes.TryGetValue((securityType, tickType), out var prefix))
            {
                yield return prefix;
                yield break;
            }

            if (securityType == SecurityType.Index)
            {
                if (tickType != TickType.Trade || resolution == Resolution.Tick)
                {
                    yield break;
                }

                yield return "A";
                yield break;
            }

            if (tickType == TickType.Trade)
            {
                yield return "T";
                // Only use aggregates if resolution is not tick
                if (resolution > Resolution.Tick)
                {
                    yield return "A";
                }
            }
            else
            {
                yield return "Q";
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static string MakeSubscriptionTicker(string prefix, string ticker)
        {
            return $"{prefix}.{ticker}";
        }

        private void AddSubscription(string ticker)
        {
            lock (_lock)
            {
                if (!_subscriptions.Contains(ticker))
                {
                    _subscriptions.Add(ticker);
                }
            }
        }

        private bool RemoveSubscription(string ticker)
        {
            lock (_lock)
            {
               return _subscriptions.Remove(ticker);
            }
        }

        private void OnError(object? sender, WebSocketError e)
        {
            Log.Error($"PolygonWebSocketClientWrapper.OnError(): {e.Message}");
        }

        private void OnMessage(object? sender, WebSocketMessage webSocketMessage)
        {
            var e = (TextMessage)webSocketMessage.Data;

            // Find the authentication message
            var authenticationMessage = JArray.Parse(e.Message)
                .FirstOrDefault(message => message["ev"].ToString() == "status" && message["status"].ToString() == "auth_success");
            if (authenticationMessage != null)
            {
                Authenticated?.Invoke(this, EventArgs.Empty);
            }

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

        private void OnAuthenticated(object? sender, EventArgs e)
        {
            Log.Trace($"PolygonWebSocketClientWrapper.OnAuthenticated(): {string.Join(", ", _supportedSecurityTypes)} - authenticated");

            lock (_lock)
            {
                foreach (var ticker in _subscriptions)
                {
                    Log.Trace($"PolygonWebSocketClientWrapper.OnAuthenticated(): {string.Join(", ", _supportedSecurityTypes)} - resubscribing {ticker}");
                    Subscribe(ticker, true);
                }
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

                case SecurityType.Index:
                    return BaseUrl + "/indices";

                default:
                    throw new Exception($"Unsupported security type: {securityType}");
            }
        }

        private static List<SecurityType> GetSupportedSecurityTypes(SecurityType securityType)
        {
            switch (securityType)
            {
                case SecurityType.Equity:
                case SecurityType.Index:
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
