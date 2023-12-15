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
using System.Net.NetworkInformation;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using Newtonsoft.Json.Linq;
using NodaTime;
using QuantConnect.Api;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Util;
using RestSharp;

namespace QuantConnect.Polygon
{
    /// <summary>
    /// Polygon.io implementation of <see cref="IDataQueueHandler"/> and <see cref="IHistoryProvider"/>
    /// </summary>
    public partial class PolygonDataQueueHandler : IDataQueueHandler, IDataQueueUniverseProvider
    {
        private static readonly ReadOnlyCollection<SecurityType> _supportedSecurityTypes = Array.AsReadOnly(new[]
        {
            SecurityType.Equity,
            SecurityType.Option,
            SecurityType.IndexOption
        });

        private string _apiKey;
        private PolygonSubscriptionPlan _subscriptionPlan;

        private readonly PolygonAggregationManager _dataAggregator;

        protected readonly PolygonSubscriptionManager _subscriptionManager;

        private readonly List<ExchangeMapping> _exchangeMappings;
        private readonly PolygonSymbolMapper _symbolMapper = new();
        private readonly MarketHoursDatabase _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();
        private readonly Dictionary<Symbol, DateTimeZone> _symbolExchangeTimeZones = new();

        private readonly ManualResetEvent _successfulAuthenticationEvent = new(false);
        private readonly ManualResetEvent _failedAuthenticationEvent = new(false);
        private readonly ManualResetEvent _subscribedEvent = new(false);

        private IOptionChainProvider _optionChainProvider;

        private bool _disposed;

        protected virtual ITimeProvider TimeProvider => RealTimeProvider.Instance;

        /// <summary>
        /// Creates and initializes a new instance of the <see cref="PolygonDataQueueHandler"/> class
        /// </summary>
        public PolygonDataQueueHandler()
            : this(Config.Get("polygon-api-key"),
                Config.GetValue("polygon-subscription-plan", PolygonSubscriptionPlan.Advanced),
                Config.GetInt("polygon-max-subscriptions-per-websocket", -1))
        {
        }

        /// <summary>
        /// Creates and initializes a new instance of the <see cref="PolygonDataQueueHandler"/> class
        /// </summary>
        /// <param name="apiKey">The Polygon API key for authentication</param>
        /// <param name="subscriptionPlan">Polygon subscription plan</param>
        /// <param name="streamingEnabled">
        /// Whether this handle will be used for streaming data.
        /// If false, the handler is supposed to be used as a history provider only.
        /// </param>
        public PolygonDataQueueHandler(string apiKey, PolygonSubscriptionPlan? subscriptionPlan = null, bool streamingEnabled = true)
            : this(apiKey,
                subscriptionPlan ?? Config.GetValue("polygon-subscription-plan", PolygonSubscriptionPlan.Advanced),
                Config.GetInt("polygon-max-subscriptions-per-websocket", -1),
                streamingEnabled)
        {
        }

        /// <summary>
        /// Creates and initializes a new instance of the <see cref="PolygonDataQueueHandler"/> class
        /// </summary>
        /// <param name="apiKey">The Polygon.io API key for authentication</param>
        /// <param name="subscriptionPlan">Polygon subscription plan</param>
        /// <param name="maxSubscriptionsPerWebSocket">The maximum number of subscriptions allowed per websocket</param>
        /// <param name="streamingEnabled">
        /// Whether this handle will be used for streaming data.
        /// If false, the handler is supposed to be used as a history provider only.
        /// </param>
        public PolygonDataQueueHandler(string apiKey, PolygonSubscriptionPlan subscriptionPlan,
            int maxSubscriptionsPerWebSocket, bool streamingEnabled = true)
        {
            _apiKey = apiKey;
            _subscriptionPlan = subscriptionPlan;
            _dataAggregator = new PolygonAggregationManager(subscriptionPlan);
            _optionChainProvider = Composer.Instance.GetPart<IOptionChainProvider>();

            ValidateSubscription();

            // Initialize the exchange mappings
            _exchangeMappings = FetchExchangeMappings();

            // Initialize the subscription manager if this instance is going to be used as a data queue handler
            if (streamingEnabled)
            {
                _subscriptionManager = new PolygonSubscriptionManager(
                    _supportedSecurityTypes,
                    maxSubscriptionsPerWebSocket,
                    (securityType) => new PolygonWebSocketClientWrapper(_apiKey, _subscriptionPlan, _symbolMapper, securityType, OnMessage));
            }
        }

        #region IDataQueueHandler implementation

        /// <summary>
        /// Returns whether the data provider is connected
        /// </summary>
        /// <returns>True if the data provider is connected</returns>
        public bool IsConnected => _subscriptionManager.IsConnected;

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
            if (!CanSubscribe(dataConfig))
            {
                return null;
            }

            var enumerator = _dataAggregator.Add(dataConfig, newDataAvailableHandler);

            // On first subscription per websocket, authentication is performed.
            // Let's make sure authentication is successful:
            _successfulAuthenticationEvent.Reset();
            _failedAuthenticationEvent.Reset();
            _subscribedEvent.Reset();

            // Subscribe
            Log.Debug($"PolygonDataQueueHandler.Subscribe(): Subscribing to {dataConfig.Symbol} | {dataConfig.TickType}");
            _subscriptionManager.Subscribe(dataConfig);

            var events = new WaitHandle[] { _failedAuthenticationEvent, _successfulAuthenticationEvent, _subscribedEvent };
            var triggeredEventIndex = WaitHandle.WaitAny(events, TimeSpan.FromMinutes(1));
            if (triggeredEventIndex == WaitHandle.WaitTimeout)
            {
                throw new TimeoutException("Timeout waiting for websocket to connect.");
            }
            // Authentication failed
            else if (triggeredEventIndex == 0)
            {
                throw new PolygonFailedAuthenticationException("Polygon WebSocket authentication failed");
            }
            // On successful authentication or subscription, we just continue

            return enumerator;
        }

        /// <summary>
        /// Removes the specified configuration
        /// </summary>
        /// <param name="dataConfig">Subscription config to be removed</param>
        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            _subscriptionManager.Unsubscribe(dataConfig);
            _dataAggregator.Remove(dataConfig);
        }

        #endregion

        #region IDataQueueUniverseProvider

        /// <summary>
        /// Method returns a collection of symbols that are available at the broker.
        /// </summary>
        /// <param name="symbol">Symbol to search option chain for</param>
        /// <param name="includeExpired">Include expired contracts</param>
        /// <param name="securityCurrency">Expected security currency(if any)</param>
        /// <returns>Future/Option chain associated with the Symbol provided</returns>
        public IEnumerable<Symbol> LookupSymbols(Symbol symbol, bool includeExpired, string securityCurrency = null)
        {
            if (_optionChainProvider == null)
            {
                return Enumerable.Empty<Symbol>();
            }

            if (!IsSecurityTypeSupported(symbol.SecurityType))
            {
                throw new ArgumentException($"Unsupported security type {symbol.SecurityType}");
            }

            Log.Trace($"PolygonDataQueueHandler.LookupSymbols(): Requesting symbol list for {symbol}");

            var symbols = _optionChainProvider.GetOptionContractList(symbol, TimeProvider.GetUtcNow().Date).ToList();

            // Try to remove options contracts that have expired
            if (!includeExpired)
            {
                var removedSymbols = new List<Symbol>();
                symbols.RemoveAll(x =>
                {
                    var expired = x.ID.Date < GetTickTime(x, TimeProvider.GetUtcNow()).Date;
                    if (expired)
                    {
                        removedSymbols.Add(x);
                    }
                    return expired;
                });

                if (removedSymbols.Count > 0)
                {
                    Log.Trace($@"PolygonDataQueueHandler.LookupSymbols(): Removed contract(s) for having expiry in the past: {
                        string.Join(",", removedSymbols.Select(x => x.Value))}");
                }
            }

            Log.Trace($"PolygonDataQueueHandler.LookupSymbols(): Returning {symbols.Count} contract(s) for {symbol}");

            return symbols;
        }

        /// <summary>
        /// Returns whether selection can take place or not.
        /// </summary>
        /// <returns>True if selection can take place</returns>
        public bool CanPerformSelection()
        {
            return IsConnected;
        }

        #endregion

        #region IDisposable implementation

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                _subscriptionManager?.DisposeSafely();
                _dataAggregator.DisposeSafely();
                RestApiRateLimiter.DisposeSafely();

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

        #region WebSocket

        /// <summary>
        /// Handles Polygon.io websocket messages
        /// </summary>
        private void OnMessage(string message)
        {
            foreach (var parsedMessage in JArray.Parse(message))
            {
                var eventType = parsedMessage["ev"].ToString();

                switch (eventType)
                {
                    case "A":
                        ProcessAggregate(parsedMessage.ToObject<AggregateMessage>());
                        break;

                    case "T":
                        ProcessTrade(parsedMessage.ToObject<TradeMessage>());
                        break;

                    case "Q":
                        ProcessQuote(parsedMessage.ToObject<QuoteMessage>());
                        break;

                    case "status":
                        ProcessStatusMessage(parsedMessage);
                        break;

                    default:
                        break;
                }
            }
        }

        /// <summary>
        /// Processes an aggregate event message handling the incoming bar
        /// </summary>
        private void ProcessAggregate(AggregateMessage aggregate)
        {
            var symbol = _symbolMapper.GetLeanSymbol(aggregate.Symbol);
            var time = GetTickTime(symbol, aggregate.StartingTickTimestamp);
            var period = TimeSpan.FromMilliseconds(aggregate.EndingTickTimestamp - aggregate.StartingTickTimestamp);
            var bar = new TradeBar(time, symbol, aggregate.Open, aggregate.High, aggregate.Low, aggregate.Close, aggregate.Volume, period);

            _dataAggregator.Update(bar);
        }

        /// <summary>
        /// Processes and incoming trade tick
        /// </summary>
        private void ProcessTrade(TradeMessage trade)
        {
            var symbol = _symbolMapper.GetLeanSymbol(trade.Symbol);
            var time = GetTickTime(symbol, trade.Timestamp);
            // TODO: Map trade.Conditions to Lean sale conditions
            var tick = new Tick(time, symbol, string.Empty, GetExchangeCode(trade.ExchangeID), trade.Price, trade.Size);
            _dataAggregator.Update(tick);
        }

        /// <summary>
        /// Processes and incoming quote tick
        /// </summary>
        private void ProcessQuote(QuoteMessage quote)
        {
            var symbol = _symbolMapper.GetLeanSymbol(quote.Symbol);
            var time = GetTickTime(symbol, quote.Timestamp);
            // TODO: Map trade.Conditions to Lean sale conditions
            // Note: Polygon's quotes have bid/ask exchange IDs, but Lean only has one exchange per tick. We'll use the bid exchange.
            var tick = new Tick(time, symbol, string.Empty, GetExchangeCode(quote.BidExchangeID),
                quote.BidSize, quote.BidPrice, quote.AskSize, quote.AskPrice);
            _dataAggregator.Update(tick);
        }

        /// <summary>
        /// Processes status message
        /// </summary>
        private void ProcessStatusMessage(JToken jStatusMessage)
        {
            var jstatus = jStatusMessage["status"];
            if (jstatus != null && jstatus.Type == JTokenType.String)
            {
                var status = jstatus.ToString();
                if (status.Contains("auth_failed", StringComparison.InvariantCultureIgnoreCase))
                {
                    var errorMessage = jStatusMessage["message"]?.ToString() ?? string.Empty;
                    Log.Error($"PolygonDataQueueHandler(): authentication failed: '{errorMessage}'.");
                    _failedAuthenticationEvent.Set();
                }
                else if (status.Contains("auth_success", StringComparison.InvariantCultureIgnoreCase))
                {
                    Log.Trace($"PolygonDataQueueHandler(): successful authentication.");
                    _successfulAuthenticationEvent.Set();
                }
                else if (status.Contains("success", StringComparison.InvariantCultureIgnoreCase))
                {
                    var statusMessage = jStatusMessage["message"]?.ToString() ?? string.Empty;
                    if (statusMessage.Contains("subscribed to", StringComparison.InvariantCultureIgnoreCase))
                    {
                        _subscribedEvent.Set();
                    }
                }
            }
        }

        /// <summary>
        /// Converts the given UTC timestamp into the symbol security exchange time zone
        /// </summary>
        private DateTime GetTickTime(Symbol symbol, long timestamp)
        {
            var utcTime = Time.UnixMillisecondTimeStampToDateTime(timestamp);

            return GetTickTime(symbol, utcTime);
        }

        /// <summary>
        /// Converts the given UTC time into the symbol security exchange time zone
        /// </summary>
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

        #endregion

        #region Exchange mappings

        /// <summary>
        /// Gets the exchange code mappings from Polygon.io to be cached and used when fetching tick data
        /// </summary>
        private List<ExchangeMapping> FetchExchangeMappings()
        {
            var url = $"{RestApiBaseUrl}/v3/reference/exchanges";
            // This url is not paginated, so we expect a single response
            var response = DownloadAndParseData<ExchangesResponse>(url).SingleOrDefault();
            if (response == null)
            {
                throw new Exception($"Failed to download exchange mappings from {url}");
            }

            return response.Results.ToList();
        }

        /// <summary>
        /// Gets the exchange code for the given exchange polygon id.
        /// This code is universal and can be used by Lean to create and <see cref="Exchange"/> instance.
        /// </summary>
        private string GetExchangeCode(int exchangePolygonId)
        {
            var mapping = _exchangeMappings.FirstOrDefault(x => x.ID == exchangePolygonId);
            if (mapping == null)
            {
                // Unknown exchange
                return string.Empty;
            }

            return mapping.Code;
        }

        #endregion

        /// <summary>
        /// Determines whether or not the specified config can be subscribed to
        /// </summary>
        private bool CanSubscribe(SubscriptionDataConfig config)
        {
            // Basic plan does not support streaming data
            if (_subscriptionPlan == PolygonSubscriptionPlan.Basic)
            {
                Log.Trace($"PolygonDataQueueHandler.CanSubscribe(): Basic plan does not support streaming data.");
                return false;
            }

            return
                // Filter out universe symbols
                config.Symbol.Value.IndexOfInvariant("universe", true) == -1 &&
                IsSupported(config.SecurityType, config.TickType, config.Resolution);
        }

        private bool IsSupported(SecurityType securityType, TickType tickType, Resolution resolution)
        {
            // Check supported security types
            if (!IsSecurityTypeSupported(securityType))
            {
                Log.Trace($"PolygonDataQueueHandler.IsSupported(): Unsupported security type: {securityType}");
                return false;
            }

            if (tickType == TickType.OpenInterest)
            {
                Log.Trace($"PolygonDataQueueHandler.IsSupported(): Unsupported tick type: {tickType}");
                return false;
            }

            // Starter plan does not support ticks
            if (_subscriptionPlan < PolygonSubscriptionPlan.Developer && resolution < Resolution.Second)
            {
                Log.Trace($"PolygonDataQueueHandler.IsSupported(): Unsupported resolution: {resolution} " +
                    $"for {_subscriptionPlan} subscription plan");
                return false;
            }

            // Only advanced plan supports quotes
            if (_subscriptionPlan < PolygonSubscriptionPlan.Advanced && tickType != TickType.Trade)
            {
                Log.Trace($"PolygonDataQueueHandler.CanSubscribe(): Unsupported tick type: {tickType} " +
                    $"for {_subscriptionPlan} subscription plan");
                return false;
            }

            return true;
        }

        /// <summary>
        /// Determines whether or not the specified security type is a supported option
        /// </summary>
        private static bool IsSecurityTypeSupported(SecurityType securityType)
        {
            return _supportedSecurityTypes.Contains(securityType);
        }

        private class ModulesReadLicenseRead : Api.RestResponse
        {
            [JsonProperty(PropertyName = "license")]
            public string License;

            [JsonProperty(PropertyName = "organizationId")]
            public string OrganizationId;
        }

        /// <summary>
        /// Validate the user of this project has permission to be using it via our web API.
        /// </summary>
        private static void ValidateSubscription()
        {
            try
            {
                const int productId = 306;
                var userId = Config.GetInt("job-user-id");
                var token = Config.Get("api-access-token");
                var organizationId = Config.Get("job-organization-id", null);
                // Verify we can authenticate with this user and token
                var api = new ApiConnection(userId, token);
                if (!api.Connected)
                {
                    throw new ArgumentException("Invalid api user id or token, cannot authenticate subscription.");
                }
                // Compile the information we want to send when validating
                var information = new Dictionary<string, object>()
                {
                    {"productId", productId},
                    {"machineName", Environment.MachineName},
                    {"userName", Environment.UserName},
                    {"domainName", Environment.UserDomainName},
                    {"os", Environment.OSVersion}
                };
                // IP and Mac Address Information
                try
                {
                    var interfaceDictionary = new List<Dictionary<string, object>>();
                    foreach (var nic in NetworkInterface.GetAllNetworkInterfaces().Where(nic => nic.OperationalStatus == OperationalStatus.Up))
                    {
                        var interfaceInformation = new Dictionary<string, object>();
                        // Get UnicastAddresses
                        var addresses = nic.GetIPProperties().UnicastAddresses
                            .Select(uniAddress => uniAddress.Address)
                            .Where(address => !IPAddress.IsLoopback(address)).Select(x => x.ToString());
                        // If this interface has non-loopback addresses, we will include it
                        if (!addresses.IsNullOrEmpty())
                        {
                            interfaceInformation.Add("unicastAddresses", addresses);
                            // Get MAC address
                            interfaceInformation.Add("MAC", nic.GetPhysicalAddress().ToString());
                            // Add Interface name
                            interfaceInformation.Add("name", nic.Name);
                            // Add these to our dictionary
                            interfaceDictionary.Add(interfaceInformation);
                        }
                    }
                    information.Add("networkInterfaces", interfaceDictionary);
                }
                catch (Exception)
                {
                    // NOP, not necessary to crash if fails to extract and add this information
                }
                // Include our OrganizationId if specified
                if (!string.IsNullOrEmpty(organizationId))
                {
                    information.Add("organizationId", organizationId);
                }
                var request = new RestRequest("modules/license/read", Method.POST) { RequestFormat = DataFormat.Json };
                request.AddParameter("application/json", JsonConvert.SerializeObject(information), ParameterType.RequestBody);
                api.TryRequest(request, out ModulesReadLicenseRead result);
                if (!result.Success)
                {
                    throw new InvalidOperationException($"Request for subscriptions from web failed, Response Errors : {string.Join(',', result.Errors)}");
                }

                var encryptedData = result.License;
                // Decrypt the data we received
                DateTime? expirationDate = null;
                long? stamp = null;
                bool? isValid = null;
                if (encryptedData != null)
                {
                    // Fetch the org id from the response if it was not set, we need it to generate our validation key
                    if (string.IsNullOrEmpty(organizationId))
                    {
                        organizationId = result.OrganizationId;
                    }
                    // Create our combination key
                    var password = $"{token}-{organizationId}";
                    var key = SHA256.HashData(Encoding.UTF8.GetBytes(password));
                    // Split the data
                    var info = encryptedData.Split("::");
                    var buffer = Convert.FromBase64String(info[0]);
                    var iv = Convert.FromBase64String(info[1]);
                    // Decrypt our information
                    using var aes = new AesManaged();
                    var decryptor = aes.CreateDecryptor(key, iv);
                    using var memoryStream = new MemoryStream(buffer);
                    using var cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read);
                    using var streamReader = new StreamReader(cryptoStream);
                    var decryptedData = streamReader.ReadToEnd();
                    if (!decryptedData.IsNullOrEmpty())
                    {
                        var jsonInfo = JsonConvert.DeserializeObject<JObject>(decryptedData);
                        expirationDate = jsonInfo["expiration"]?.Value<DateTime>();
                        isValid = jsonInfo["isValid"]?.Value<bool>();
                        stamp = jsonInfo["stamped"]?.Value<int>();
                    }
                }
                // Validate our conditions
                if (!expirationDate.HasValue || !isValid.HasValue || !stamp.HasValue)
                {
                    throw new InvalidOperationException("Failed to validate subscription.");
                }

                var nowUtc = DateTime.UtcNow;
                var timeSpan = nowUtc - Time.UnixTimeStampToDateTime(stamp.Value);
                if (timeSpan > TimeSpan.FromHours(12))
                {
                    throw new InvalidOperationException("Invalid API response.");
                }
                if (!isValid.Value)
                {
                    throw new ArgumentException($"Your subscription is not valid, please check your product subscriptions on our website.");
                }
                if (expirationDate < nowUtc)
                {
                    throw new ArgumentException($"Your subscription expired {expirationDate}, please renew in order to use this product.");
                }
            }
            catch (Exception e)
            {
                Log.Error($"PolygonDataQueueHandler.ValidateSubscription(): Failed during validation, shutting down. Error : {e.Message}");
                throw;
            }
        }
    }
}
