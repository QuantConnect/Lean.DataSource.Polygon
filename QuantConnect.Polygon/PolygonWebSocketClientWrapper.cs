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

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// WebSocket client wrapper for Polygon.io
    /// </summary>
    public class PolygonWebSocketClientWrapper : WebSocketClientWrapper
    {
        /// <summary>
        /// The license type for the current Polygon subscription (Individual or Business).
        /// </summary>
        public readonly LicenseType licenseType;

        private readonly string _apiKey;
        private readonly ISymbolMapper _symbolMapper;
        private readonly Action<string> _messageHandler;

        private object _lock = new();

        private readonly List<SecurityType> _supportedSecurityTypes;

        private readonly List<string> _subscriptions;

        /// <summary>
        /// Maps a combination of <see cref="SecurityType"/> and <see cref="TickType"/> 
        /// to the corresponding <see cref="EventType"/> used in WebSocket subscriptions.
        /// </summary>
        private readonly Dictionary<(SecurityType, TickType), EventType> _eventTypes = [];

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
        /// <param name="licenseType">The subscription license type, used to determine the base WebSocket URL.</param>
        public PolygonWebSocketClientWrapper(string apiKey,
            ISymbolMapper symbolMapper,
            SecurityType securityType,
            Action<string> messageHandler,
            LicenseType licenseType)
        {
            _apiKey = apiKey;
            _symbolMapper = symbolMapper;
            _supportedSecurityTypes = GetSupportedSecurityTypes(securityType);
            _messageHandler = messageHandler;
            _subscriptions = new();

            this.licenseType = licenseType;
            var baseUrl = licenseType switch
            {
                LicenseType.Individual => "wss://socket.polygon.io",
                LicenseType.Business when securityType == SecurityType.Index => "wss://socket.polygon.io",
                LicenseType.Business =>"wss://business.polygon.io",
                _ => throw new NotSupportedException($"{nameof(PolygonWebSocketClientWrapper)}: Unsupported license type '{licenseType}'. Expected either 'Individual' or 'Business'.")
            };

            Initialize(GetWebSocketUrl(baseUrl, securityType));

            Open += OnOpen;
            Closed += OnClosed;
            Message += OnMessage;
            Error += OnError;
            Authenticated += OnAuthenticated;
        }

        /// <summary>
        /// Subscribes to a Polygon data feed for the specified symbol, tick type, and resolution.
        /// </summary>
        /// <param name="symbol">The Lean symbol to subscribe to.</param>
        /// <param name="tickType">The type of market data to subscribe to (e.g., Trade, Quote, OpenInterest).</param>
        /// <param name="resolution">The resolution of the subscription (e.g., Tick, Minute, Hour, Daily).</param>
        /// <returns>
        /// The <see cref="EventType"/> that represents the underlying Polygon channel for this subscription.
        /// If the event type is already known, it is resolved from the internal cache; otherwise, the method
        /// establishes a new subscription and determines the event type dynamically.
        /// </returns>
        public EventType Subscribe(Symbol symbol, TickType tickType, Resolution resolution)
        {
            var ticker = _symbolMapper.GetBrokerageSymbol(symbol);

            // If eventType is already known, use it
            if (_eventTypes.TryGetValue((symbol.SecurityType, tickType), out var eventType))
            {
                var subscriptionTicker = MakeSubscriptionTicker(eventType, ticker);
                Subscribe(subscriptionTicker, true);
                AddSubscription(subscriptionTicker);
                Log.Trace($"PolygonWebSocketClientWrapper.Subscribe(): Subscribed to {subscriptionTicker}");

                return eventType;
            }
            else
            {
                return TrySubscribe(ticker, symbol.SecurityType, tickType, resolution);
            }
        }

        /// <summary>
        /// Attempts to subscribe to a data feed for the specified ticker when the 
        /// corresponding <see cref="EventType"/> is not already known.
        /// </summary>
        /// <param name="ticker">The brokerage-specific ticker symbol to subscribe to.</param>
        /// <param name="securityType">The <see cref="SecurityType"/> of the instrument (e.g., Equity, Option, Forex).</param>
        /// <param name="tickType">
        /// The <see cref="TickType"/> (e.g., Trade, Quote) that specifies the type of market data.</param>
        /// <param name="resolution">The <see cref="Resolution"/> at which the data should be sampled or aggregated.</param>
        /// <returns>
        /// The resolved <see cref="EventType"/> assigned to the subscription.
        /// If the subscription cannot be established, <see cref="EventType.None"/> is returned.
        /// </returns>
        private EventType TrySubscribe(string ticker, SecurityType securityType, TickType tickType, Resolution resolution)
        {
            // We'll try subscribing assuming the highest subscription plan and work our way down if we get an error
            using var subscribedEvent = new ManualResetEventSlim(false);
            using var errorEvent = new ManualResetEventSlim(false);

            void ProcessMessage(object? _, WebSocketMessage wsMessage)
            {
                var jsonMessage = JArray.Parse(((TextMessage)wsMessage.Data).Message)[0];
                if (Log.DebuggingEnabled)
                {
                    Log.Debug($"{nameof(TrySubscribe)}.{nameof(ProcessMessage)}.JSON: " + jsonMessage.ToString(Formatting.None));
                }
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

            var subscribeOnEventType = default(EventType);
            foreach (var protentialEventType in GetSubscriptionEventType(securityType, tickType, resolution))
            {
                triedSubscription = true;
                subscribedEvent.Reset();
                errorEvent.Reset();

                var subscriptionTicker = MakeSubscriptionTicker(protentialEventType, ticker);
                Subscribe(subscriptionTicker, true);

                // Wait for the subscribed event or error event
                var index = WaitHandle.WaitAny(waitHandles, TimeSpan.FromSeconds(30));

                // Quickly try the next eventType if we get an error or timeout
                if (index != 0)
                {
                    continue;
                }

                Log.Trace($"PolygonWebSocketClientWrapper.Subscribe(): Subscribed to {subscriptionTicker}");
                // Subscription was successful
                AddSubscription(subscriptionTicker);
                _eventTypes[(securityType, tickType)] = protentialEventType;
                subscribeOnEventType = protentialEventType;
                subscribed = true;
                break;
            }

            Message -= ProcessMessage;

            if (triedSubscription && !subscribed)
            {
                throw new Exception($"PolygonWebSocketClientWrapper.Subscribe(): Failed to subscribe to {ticker}. " +
                    $"Make sure your subscription plan allows streaming {tickType.ToString().ToLowerInvariant()} data.");
            }

            return subscribeOnEventType;
        }

        /// <summary>
        /// Unsubscribes the given symbol
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <param name="tickType">Type of tick data</param>
        public void Unsubscribe(Symbol symbol, TickType tickType)
        {
            var baseTicker = _symbolMapper.GetBrokerageSymbol(symbol);
            foreach (var eventType in GetSubscriptionEventType(symbol.SecurityType, tickType))
            {
                var ticker = MakeSubscriptionTicker(eventType, baseTicker);
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
            var msg = JsonConvert.SerializeObject(new
            {
                action = subscribe ? "subscribe" : "unsubscribe",
                @params = ticker
            });
            if (Log.DebuggingEnabled)
            {
                Log.Trace($"{nameof(PolygonWebSocketClientWrapper)}.{nameof(Subscribe)}.JSON: " + msg);
            }
            Send(msg);
        }

        /// <summary>
        /// Gets a list of Polygon WebSocket eventTypes supported for the given tick type and resolution
        /// </summary>
        private IEnumerable<EventType> GetSubscriptionEventType(SecurityType securityType ,TickType tickType, Resolution resolution = Resolution.Minute)
        {
            // If we already know the eventType, return it and don't try any others
            if (_eventTypes.TryGetValue((securityType, tickType), out var eventType))
            {
                yield return eventType;
                yield break;
            }

            if (securityType == SecurityType.Index)
            {
                if (licenseType == LicenseType.Business)
                {
                    yield return EventType.V;
                    yield break;
                }

                if (tickType != TickType.Trade || resolution == Resolution.Tick)
                {
                    yield break;
                }

                yield return EventType.A; // docs: minimum Advanced plan

                if (resolution >= Resolution.Minute)
                {
                    yield return EventType.AM; // docs: minimum Starter plan
                }

                yield break;
            }

            if (tickType == TickType.Trade)
            {
                if (licenseType == LicenseType.Business)
                {
                    yield return EventType.FMV;
                    yield break;
                }

                yield return EventType.T;
                // Only use aggregates if subscription doesn't support more accurate data
                if (resolution > Resolution.Tick)
                {
                    yield return EventType.A; // docs: minimum Developer plan

                    if (resolution >= Resolution.Minute)
                    {
                        yield return EventType.AM; // docs: minimum Starter plan
                    }
                }
            }
            else
            {
                yield return EventType.Q;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static string MakeSubscriptionTicker(EventType eventType, string ticker)
        {
            return $"{eventType}.{ticker}";
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
            if (Log.DebuggingEnabled)
            {
                Log.Debug($"{nameof(PolygonWebSocketClientWrapper)}.{nameof(OnMessage)}.JSON: {e.Message}");
            }
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

        private string GetWebSocketUrl(string baseUrl, SecurityType securityType)
        {
            switch (securityType)
            {
                case SecurityType.Equity:
                    return baseUrl + "/stocks";

                case SecurityType.Option:
                case SecurityType.IndexOption:
                    return baseUrl + "/options";

                case SecurityType.Index:
                    return baseUrl + "/indices";

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
