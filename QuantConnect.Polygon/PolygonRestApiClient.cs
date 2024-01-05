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
using RestSharp;
using QuantConnect.Logging;
using QuantConnect.Configuration;
using QuantConnect.Util;

namespace QuantConnect.Polygon
{
    /// <summary>
    /// Provides a common API to download data from Polygon.io's REST API
    /// </summary>
    public class PolygonRestApiClient : IDisposable
    {
        private readonly static string RestApiBaseUrl = Config.Get("polygon-api-url", "https://api.polygon.io");

        private readonly RestClient _restClient;

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
            _restClient = new RestClient(RestApiBaseUrl);
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

                if (RateLimiter != null)
                {
                    if (RateLimiter.IsRateLimited)
                    {
                        Log.Trace("PolygonRestApi.DownloadAndParseData(): Rest API calls are limited; waiting to proceed.");
                    }
                    RateLimiter.WaitToProceed();
                }

                request.AddHeader("Authorization", $"Bearer {_apiKey}");

                var response = _restClient.Execute(request);
                if (response == null)
                {
                    Log.Debug($"PolygonRestApi.DownloadAndParseData(): No response for {request.Resource}");
                    yield break;
                }

                // If the data download was not successful, log the reason
                var resultJson = JObject.Parse(response.Content);
                if (resultJson["status"]?.ToString().ToUpperInvariant() != "OK")
                {
                    Log.Debug($"PolygonRestApi.DownloadAndParseData(): No data for {request.Resource}. Reason: {response.Content}");
                    yield break;
                }

                var result = resultJson.ToObject<T>();
                if (result == null)
                {
                    Log.Debug($"PolygonRestApi.DownloadAndParseData(): Unable to parse response for {request.Resource}. " +
                        $"Response: {response.Content}");
                    yield break;
                }

                yield return result;

                request = result.NextUrl != null ? new RestRequest(result.NextUrl, Method.GET) : null;
            }
        }

        public void Dispose()
        {
            RateLimiter?.DisposeSafely();
        }
    }
}
