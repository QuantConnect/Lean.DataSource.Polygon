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
using QuantConnect.Lean.Engine.DataFeeds;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Polygon.io implementation of <see cref="IDataQueueHandler"/> and <see cref="IHistoryProvider"/>
    /// </summary>
    /// <remarks>
    /// Polygon.io equities documentation: https://polygon.io/docs/stocks/getting-started
    /// Polygon.io options documentation: https://polygon.io/docs/options/getting-started
    /// </remarks>
    public partial class PolygonDataProvider : IDataQueueHandler
    {
        private static readonly ReadOnlyCollection<SecurityType> _supportedSecurityTypes = Array.AsReadOnly(new[]
        {
            SecurityType.Equity,
            SecurityType.Option,
            SecurityType.IndexOption,
            SecurityType.Index,
        });

        private string _apiKey;

        private PolygonAggregationManager _dataAggregator;

        protected PolygonSubscriptionManager _subscriptionManager;

        private List<ExchangeMapping> _exchangeMappings;
        private readonly PolygonSymbolMapper _symbolMapper = new();
        private readonly MarketHoursDatabase _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();
        private readonly Dictionary<Symbol, DateTimeZone> _symbolExchangeTimeZones = new();

        private bool _initialized;
        private bool _disposed;

        private volatile bool _unsupportedSecurityTypeMessageLogged;
        private volatile bool _unsupportedTickTypeMessagedLogged;
        private volatile bool _unsupportedDataTypeMessageLogged;
        private volatile bool _potentialUnsupportedResolutionMessageLogged;
        private volatile bool _potentialUnsupportedTickTypeMessageLogged;

        /// <summary>
        /// <inheritdoc cref="IMapFileProvider"/>
        /// </summary>
        private readonly IMapFileProvider _mapFileProvider = Composer.Instance.GetPart<IMapFileProvider>();

        /// <summary>
        /// The time provider instance. Used for improved testability
        /// </summary>
        protected virtual ITimeProvider TimeProvider { get; } = RealTimeProvider.Instance;

        /// <summary>
        /// The rest client instance
        /// </summary>
        protected virtual PolygonRestApiClient RestApiClient { get; set; }

        /// <summary>
        /// Creates and initializes a new instance of the <see cref="PolygonDataProvider"/> class
        /// </summary>
        public PolygonDataProvider()
            : this(Config.Get("polygon-api-key"), Config.GetInt("polygon-max-subscriptions-per-websocket", -1))
        {
        }

        /// <summary>
        /// Creates and initializes a new instance of the <see cref="PolygonDataProvider"/> class
        /// </summary>
        /// <param name="apiKey">The Polygon API key for authentication</param>
        /// <param name="streamingEnabled">
        /// Whether this handle will be used for streaming data.
        /// If false, the handler is supposed to be used as a history provider only.
        /// </param>
        public PolygonDataProvider(string apiKey, bool streamingEnabled = true)
            : this(apiKey, Config.GetInt("polygon-max-subscriptions-per-websocket", -1), streamingEnabled)
        {
        }

        /// <summary>
        /// Creates and initializes a new instance of the <see cref="PolygonDataProvider"/> class
        /// </summary>
        /// <param name="apiKey">The Polygon.io API key for authentication</param>
        /// <param name="maxSubscriptionsPerWebSocket">The maximum number of subscriptions allowed per websocket</param>
        /// <param name="streamingEnabled">
        /// Whether this handle will be used for streaming data.
        /// If false, the handler is supposed to be used as a history provider only.
        /// </param>
        public PolygonDataProvider(string apiKey, int maxSubscriptionsPerWebSocket, bool streamingEnabled = true)
        {
            if (string.IsNullOrWhiteSpace(apiKey))
            {
                // If the API key is not provided, we can't do anything.
                // The handler might going to be initialized using a node packet job.
                return;
            }

            Initialize(apiKey, maxSubscriptionsPerWebSocket, streamingEnabled);
        }

        /// <summary>
        /// Initializes the data queue handler and validates the product subscription
        /// </summary>
        private void Initialize(string apiKey, int maxSubscriptionsPerWebSocket, bool streamingEnabled = true)
        {
            if (string.IsNullOrWhiteSpace(apiKey))
            {
                throw new PolygonAuthenticationException("History calls for Polygon.io require an API key.");
            }
            _apiKey = apiKey;

            _initialized = true;
            _dataAggregator = new PolygonAggregationManager();
            RestApiClient = new PolygonRestApiClient(_apiKey);
            _optionChainProvider = new CachingOptionChainProvider(new PolygonOptionChainProvider(RestApiClient, _symbolMapper));

            ValidateSubscription();

            // Initialize the exchange mappings
            _exchangeMappings = FetchExchangeMappings();

            // Initialize the subscription manager if this instance is going to be used as a data queue handler
            if (streamingEnabled)
            {
                _subscriptionManager = new PolygonSubscriptionManager(
                    _supportedSecurityTypes,
                    maxSubscriptionsPerWebSocket,
                    (securityType) => new PolygonWebSocketClientWrapper(_apiKey, _symbolMapper, securityType, OnMessage));
            }
            var openInterestManager = new PolygonOpenInterestProcessorManager(TimeProvider, RestApiClient, _symbolMapper, _subscriptionManager, _dataAggregator, GetTickTime);
            openInterestManager.ScheduleNextRun();
        }

        #region IDataQueueHandler implementation

        /// <summary>
        /// Returns whether the data provider is connected
        /// </summary>
        /// <returns>True if the data provider is connected</returns>
        public bool IsConnected => _subscriptionManager != null && _subscriptionManager.IsConnected;

        /// <summary>
        /// Sets the job we're subscribing for
        /// </summary>
        /// <param name="job">Job we're subscribing for</param>
        public void SetJob(LiveNodePacket job)
        {
            if (_initialized)
            {
                return;
            }

            if (!job.BrokerageData.TryGetValue("polygon-api-key", out var apiKey))
            {
                throw new ArgumentException("The Polygon.io API key is missing from the brokerage data.");
            }

            if (!job.BrokerageData.TryGetValue("polygon-max-subscriptions-per-websocket", out var maxSubscriptionsPerWebSocketStr) ||
                !int.TryParse(maxSubscriptionsPerWebSocketStr, out var maxSubscriptionsPerWebSocket))
            {
                maxSubscriptionsPerWebSocket = -1;
            }

            Initialize(apiKey, maxSubscriptionsPerWebSocket);
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

            Log.Trace($"PolygonDataProvider.Subscribe(): Subscribing to {dataConfig.Symbol} | {dataConfig.TickType}");

            _subscriptionManager.Subscribe(dataConfig);

            _dataAggregator.SetUsingAggregates(_subscriptionManager.UsingAggregates);
            var enumerator = _dataAggregator.Add(dataConfig, newDataAvailableHandler);

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

        #region IDisposable implementation

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                _subscriptionManager?.DisposeSafely();
                _dataAggregator.DisposeSafely();
                RestApiClient.DisposeSafely();

                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~PolygonDataProvider()
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
            var tick = new Tick(time, symbol, string.Empty, GetExchangeCode(trade.ExchangeID), trade.Size, trade.Price);
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
            DateTimeZone exchangeTimeZone;
            lock (_symbolExchangeTimeZones)
            {
                if (!_symbolExchangeTimeZones.TryGetValue(symbol, out exchangeTimeZone))
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
            }

            return utcTime.ConvertFromUtc(exchangeTimeZone);
        }

        #endregion

        #region Exchange mappings

        /// <summary>
        /// Gets the exchange code mappings from Polygon.io to be cached and used when fetching tick data
        /// </summary>
        protected virtual List<ExchangeMapping> FetchExchangeMappings()
        {
            // This url is not paginated, so we expect a single response
            const string uri = "v3/reference/exchanges";
            var request = new RestRequest(uri, Method.GET);
            var response = RestApiClient.DownloadAndParseData<ExchangesResponse>(request).SingleOrDefault();
            if (response == null)
            {
                throw new PolygonAuthenticationException($"Failed to download exchange mappings from {uri}. Make sure your API key is valid.");
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
            return
                // Filter out universe symbols
                config.Symbol.Value.IndexOfInvariant("universe", true) == -1 &&
                // Filter out canonical options
                !config.Symbol.IsCanonical() &&
                IsSupported(config.SecurityType, config.Type, config.TickType, config.Resolution);
        }

        private bool IsSupported(SecurityType securityType, Type dataType, TickType tickType, Resolution resolution)
        {
            // Check supported security types
            if (!IsSecurityTypeSupported(securityType))
            {
                if (!_unsupportedSecurityTypeMessageLogged)
                {
                    _unsupportedSecurityTypeMessageLogged = true;
                    Log.Trace($"PolygonDataProvider.IsSupported(): Unsupported security type: {securityType}");
                }
                return false;
            }

            if (tickType == TickType.OpenInterest)
            {
                if (!_unsupportedTickTypeMessagedLogged)
                {
                    _unsupportedTickTypeMessagedLogged = true;
                    Log.Trace($"PolygonDataProvider.IsSupported(): Unsupported tick type: {tickType}");
                }
                return false;
            }

            if (!dataType.IsAssignableFrom(typeof(TradeBar)) &&
                !dataType.IsAssignableFrom(typeof(QuoteBar)) &&
                !dataType.IsAssignableFrom(typeof(Tick)))
            {
                if (!_unsupportedDataTypeMessageLogged)
                {
                    _unsupportedDataTypeMessageLogged = true;
                    Log.Trace($"PolygonDataProvider.IsSupported(): Unsupported data type: {dataType}");
                }
                return false;
            }

            if (resolution < Resolution.Second && !_potentialUnsupportedResolutionMessageLogged)
            {
                _potentialUnsupportedResolutionMessageLogged = true;
                Log.Trace("PolygonDataProvider.IsSupported(): " +
                    $"Subscription for {securityType}-{dataType}-{tickType}-{resolution} will be attempted. " +
                    $"An Advanced Polygon.io subscription plan is required to stream tick data.");
            }

            if (tickType == TickType.Quote && !_potentialUnsupportedTickTypeMessageLogged)
            {
                _potentialUnsupportedTickTypeMessageLogged = true;
                Log.Trace("PolygonDataProvider.IsSupported(): " +
                    $"Subscription for {securityType}-{dataType}-{tickType}-{resolution} will be attempted. " +
                    $"An Advanced Polygon.io subscription plan is required to stream quote data.");
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
                var userId = Globals.UserId;
                var token = Globals.UserToken;
                var organizationId = Globals.OrganizationID;
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
                Log.Error($"PolygonDataProvider.ValidateSubscription(): Failed during validation, shutting down. Error : {e.Message}");
                throw;
            }
        }
    }
}
