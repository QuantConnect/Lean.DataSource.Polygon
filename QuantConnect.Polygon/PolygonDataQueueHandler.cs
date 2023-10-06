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

using Newtonsoft.Json.Linq;
using NodaTime;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Util;
using System.Collections.ObjectModel;
using static QuantConnect.Brokerages.WebSocketClientWrapper;

namespace QuantConnect.Polygon
{
    public class PolygonDataQueueHandler : IDataQueueHandler
    {
        private readonly int MaximumWebSocketConnections = Config.GetInt("polygon-max-websocket-connections", 7);
        private readonly int MaximumSubscriptionsPerWebSocket = Config.GetInt("polygon-max-subscriptions-per-websocket", 1000);

        private readonly string ApiKey = Config.Get("polygon-api-key");

        private readonly ReadOnlyCollection<SecurityType> SupportedSecurityTypes = new List<SecurityType>() { SecurityType.Option }.AsReadOnly();

        private readonly PolygonAggregationManager _dataAggregator = new();

        private readonly Dictionary<SecurityType, BrokerageMultiWebSocketSubscriptionManager> _subscriptionManagers;
        private readonly List<PolygonWebSocketClientWrapper> _webSockets = new();

        private readonly PolygonSymbolMapper _symbolMapper = new();
        private readonly MarketHoursDatabase _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();
        private readonly Dictionary<Symbol, DateTimeZone> _symbolExchangeTimeZones = new();

        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonDataQueueHandler"/> class
        /// </summary>
        public PolygonDataQueueHandler()
        {
            _subscriptionManagers = SupportedSecurityTypes.ToDictionary(securityType => securityType,
                securityType => new BrokerageMultiWebSocketSubscriptionManager(
                    PolygonWebSocketClientWrapper.GetWebSocketUrl(securityType),
                    MaximumSubscriptionsPerWebSocket,
                    MaximumWebSocketConnections,
                    new Dictionary<Symbol, int>(),
                    () =>
                    {
                        var webSocket = new PolygonWebSocketClientWrapper(ApiKey, _symbolMapper, securityType, null);

                        webSocket.Open += (_, _) =>
                        {
                            _webSockets.Add(webSocket);
                        };
                        webSocket.Closed += (_, _) =>
                        {
                            _webSockets.Remove(webSocket);
                        };

                        return webSocket;
                    },
                    (webSocket, symbol) =>
                    {
                        ((PolygonWebSocketClientWrapper)webSocket).Subscribe(symbol, TickType.Trade);
                        return true;
                    },
                    (webSocket, symbol) =>
                    {
                        ((PolygonWebSocketClientWrapper)webSocket).Unsubscribe(symbol, TickType.Trade);
                        return true;
                    },
                    (webSocketMessage) =>
                    {
                        var e = (TextMessage)webSocketMessage.Data;
                        OnMessage(e.Message);
                    },
                    TimeSpan.Zero));
        }

        #region IDataQueueHandler implementation

        /// <summary>
        /// Returns whether the data provider is connected
        /// </summary>
        /// <returns>True if the data provider is connected</returns>
        public bool IsConnected => _webSockets.Count > 0 && _webSockets.All(webSocket => webSocket.IsOpen);

        /// <summary>
        /// Sets the job we're subscribing for
        /// </summary>
        /// <param name="job">Job we're subscribing for</param>
        public void SetJob(LiveNodePacket job)
        {
        }

        /// <summary>
        /// Subscribe to the specified configuration
        /// </summary>
        /// <param name="dataConfig">defines the parameters to subscribe to a data feed</param>
        /// <param name="newDataAvailableHandler">handler to be fired on new data available</param>
        /// <returns>The new enumerator for this subscription request</returns>
        public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {
            if (!CanSubscribe(dataConfig.Symbol))
            {
                return null;
            }

            if (dataConfig.SecurityType == SecurityType.Option && dataConfig.Resolution != Resolution.Minute)
            {
                throw new ArgumentException($@"Polygon data queue handler does not support {dataConfig.Resolution
                    } resolution options subscriptions. Only {Resolution.Minute} resolution is supported for options.");
            }

            var enumerator = _dataAggregator.Add(dataConfig, newDataAvailableHandler);
            _subscriptionManagers[dataConfig.SecurityType].Subscribe(dataConfig);

            return enumerator;
        }

        /// <summary>
        /// Removes the specified configuration
        /// </summary>
        /// <param name="dataConfig">Subscription config to be removed</param>
        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            _subscriptionManagers[dataConfig.SecurityType].Unsubscribe(dataConfig);
            _dataAggregator.Remove(dataConfig);
        }

        #endregion

        #region IDisposable implementation

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                foreach (var kvp in _subscriptionManagers)
                {
                    kvp.Value.Dispose();
                }
                _dataAggregator.DisposeSafely();

                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~PolygonDataQueueHandler()
        {
            Dispose(disposing: false);
        }

        #endregion

        private void OnMessage(string message)
        {
            foreach (var parsedMessage in JArray.Parse(message))
            {
                var eventType = parsedMessage["ev"].ToString();

                switch (eventType)
                {
                    case "AM":
                        ProcessOptionAggregate(parsedMessage.ToObject<AggregateMessage>());
                        break;

                    case "status":
                        ProcessStatusMessage(parsedMessage);
                        break;

                    default:
                        break;
                }
            }
        }

        private void ProcessOptionAggregate(AggregateMessage aggregate)
        {
            var symbol = _symbolMapper.GetLeanOptionSymbol(aggregate.Symbol);
            var time = GetTickTime(symbol, aggregate.StartingTickTimestamp);
            var period = TimeSpan.FromMilliseconds(aggregate.EndingTickTimestamp - aggregate.StartingTickTimestamp);
            var bar = new TradeBar(time, symbol, aggregate.Open, aggregate.High, aggregate.Low, aggregate.Close, aggregate.Volume, period);

            _dataAggregator.Update(bar);
        }

        private void ProcessStatusMessage(JToken jStatusMessage)
        {
            var jstatus = jStatusMessage["status"];
            if (jstatus != null && jstatus.Type == JTokenType.String)
            {
                var status = jstatus.ToString();
                if (status.Contains("auth_failed", StringComparison.InvariantCultureIgnoreCase))
                {
                    var errorMessage = string.Empty;
                    var jmessage = jStatusMessage["message"];
                    if (jmessage != null)
                    {
                        errorMessage = jmessage.ToString();
                    }
                    Log.Error($"PolygonDataQueueHandler(): authentication failed: '{errorMessage}'.");
                }
                else if (status.Contains("auth_success", StringComparison.InvariantCultureIgnoreCase))
                {
                    Log.Trace($"PolygonDataQueueHandler(): successful authentication.");
                }
            }
        }

        private DateTime GetTickTime(Symbol symbol, long timestamp)
        {
            var utcTime = Time.UnixMillisecondTimeStampToDateTime(timestamp);

            return GetTickTime(symbol, utcTime);
        }

        private DateTime GetTickTime(Symbol symbol, DateTime utcTime)
        {
            if (!_symbolExchangeTimeZones.TryGetValue(symbol, out var exchangeTimeZone))
            {
                // read the exchange time zone from market-hours-database
                if (_marketHoursDatabase.TryGetEntry(symbol.ID.Market, symbol, symbol.SecurityType, out var entry))
                {
                    exchangeTimeZone = entry.ExchangeHours.TimeZone;
                }
                // If there is no entry for the given Symbol, default to New York
                else
                {
                    exchangeTimeZone = TimeZones.NewYork;
                }

                _symbolExchangeTimeZones.Add(symbol, exchangeTimeZone);
            }

            return utcTime.ConvertFromUtc(exchangeTimeZone);
        }

        /// <summary>
        /// Determines whether or not the specified symbol can be subscribed to
        /// </summary>
        private static bool CanSubscribe(Symbol symbol)
        {
            var securityType = symbol.ID.SecurityType;

            // Only options are supported for now
            return symbol.Value.IndexOfInvariant("universe", true) == -1 && securityType == SecurityType.Option;
        }
    }
}
