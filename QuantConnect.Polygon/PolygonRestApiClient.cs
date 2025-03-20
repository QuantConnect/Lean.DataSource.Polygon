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

using RestSharp;
using Newtonsoft.Json;
using QuantConnect.Util;
using Newtonsoft.Json.Linq;
using QuantConnect.Logging;
using QuantConnect.Configuration;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Provides a common API to download data from Polygon.io's REST API
    /// </summary>
    public class PolygonRestApiClient : IDisposable
    {
        private readonly static string RestApiBaseUrl = Config.Get("polygon-api-url", "https://api.polygon.io");

        private readonly RestClient _restClient;

        private readonly CancellationTokenSource _cancellationTokenSource = new();

        /// <summary>
        /// The maximum number of retry attempts for downloading data or executing a request.
        /// </summary>
        private const int MaxRetries = 10;

        private readonly string _apiKey;

        // Made virtual for testing purposes
        protected virtual string ApiResponseLimit { get; } = Config.Get("polygon-aggregate-response-limit", "5000");

        // Made virtual for testing purposes
        protected virtual RateGate RateLimiter { get; } = new(300, TimeSpan.FromSeconds(1));

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonRestApiClient"/> class
        /// </summary>
        /// <param name="apiKey">The Polygon API key</param>
        public PolygonRestApiClient(string apiKey)
        {
            _apiKey = apiKey;
            _restClient = new RestClient(RestApiBaseUrl) { Timeout = 300000 }; // 5 minutes in milliseconds
        }

        /// <summary>
        /// Downloads data and tries to parse the JSON response data into the specified type
        /// </summary>
        public virtual IEnumerable<T> DownloadAndParseData<T>(RestRequest? request)
            where T : BaseResponse
        {
            if (request != null && !request.Parameters.Any(parameter => parameter.Type == ParameterType.QueryString && parameter.Name == "limit"))
            {
                request.AddQueryParameter("limit", ApiResponseLimit);
            }

            while (request != null)
            {
                Log.Debug($"PolygonRestApi.DownloadAndParseData(): Downloading {request.Resource}");

                var responseContent = DownloadWithRetries(request);

                var result = ParseResponse<T>(responseContent);

                if (result == null)
                {
                    yield break;
                }

                yield return result;

                request = result.NextUrl != null ? new RestRequest(result.NextUrl, Method.GET) : null;
            }
        }

        private string DownloadWithRetries(RestRequest request)
        {
            var response = default(IRestResponse);
            for (var attempt = 0; attempt < MaxRetries; attempt++)
            {
                if (RateLimiter != null)
                {
                    if (RateLimiter.IsRateLimited)
                    {
                        Log.Trace("PolygonRestApi.DownloadAndParseData(): Rest API calls are limited; waiting to proceed.");
                    }
                    RateLimiter.WaitToProceed();
                }

                request.AddOrUpdateHeader("Authorization", $"Bearer {_apiKey}");

                response = _restClient.Execute(request);

                var baseResponse = JsonConvert.DeserializeObject<BaseResponse>(response.Content);

                if (response.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                {
                    var waitTime = TimeSpan.FromSeconds(10 * attempt);

                    Log.Trace($"PolygonRestApi.DownloadAndParseData(): Attempt {attempt + 1} was throttled due to too many requests. Waiting {waitTime.TotalSeconds} seconds before retrying... (Last error: {baseResponse?.Error ?? "Unknown error"})");

                    _cancellationTokenSource.Token.WaitHandle.WaitOne(waitTime);
                    continue;
                }

                if (response != null && response.Content.Length > 0 && response.StatusCode == System.Net.HttpStatusCode.OK)
                {
                    return response.Content;
                }
            }

            throw new Exception($"Failed after {MaxRetries} attempts for {request.Resource}. Content: {response?.Content}");
        }

        private T? ParseResponse<T>(string responseContent) where T : BaseResponse
        {
            var result = JObject.Parse(responseContent).ToObject<T>();

            if (result == null)
            {
                throw new ArgumentException($"{nameof(PolygonRestApiClient)}.{nameof(ParseResponse)}: Unable to parse response. Response: {responseContent}");
            }

            return result;
        }

        public void Dispose()
        {
            RateLimiter?.DisposeSafely();
            _cancellationTokenSource?.Dispose();
        }
    }
}
