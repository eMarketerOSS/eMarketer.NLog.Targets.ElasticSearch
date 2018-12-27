using System;
using System.IO;
using System.Net;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog.Common;
using NLog.Config;
using NLog.Layouts;
using NLog.Targets;

namespace eMarketer.NLog.Targets.ElasticSearch_New
{
    [Target("ElasticSearch")]
    public sealed class ElasticSearchTarget : Target
    {
        [RequiredParameter]
        public Layout Layout { get; set; }

        private readonly Layout _indexTemplate = Layout.FromString("logs-${shortdate}/all");

        static string _url = "http://localhost:9200/";
        static string _authorizationHeader;

        public static string WireToEsServer(string uri)
        {
            try
            {
                var builder = new UriBuilder(uri);

                if (string.IsNullOrEmpty(builder.UserName) == false || string.IsNullOrEmpty(builder.Password) == false)
                {
                    _authorizationHeader = String.Format("Basic {0}", Convert.ToBase64String(Encoding.ASCII.GetBytes(String.Format("{0}:{1}", builder.UserName, builder.Password))));
                    builder.UserName = builder.Password = "";
                }

                _url = builder.ToString();
            }
            catch (Exception)
            {
                //Silence, please
            }

            return _url;
        }

        protected override void Write(AsyncLogEventInfo info)
        {
            try
            {
                var client = new WebClient();
                client.Headers.Add(HttpRequestHeader.ContentType, "application/json");

                if (_authorizationHeader != null)
                {
                    client.Headers[HttpRequestHeader.Authorization] = _authorizationHeader;
                }

                var url = new Uri(_url + _indexTemplate.Render(info.LogEvent));
                var layout = this.Layout.Render(info.LogEvent);
                var json = JObject.Parse(layout); // make sure the json is valid

                if (info.LogEvent.Exception != null)
                {
                    var nfo = JObject.FromObject(info.LogEvent.Exception);
                    json.Add("exception", nfo);
                }

                var message = json["@message"].ToString();

                if (IsStringAValidJson(message))
                    json["@message"] = JObject.Parse(message);
                else
                    json["@message"] = JObject.Parse(string.Format(@"{{ ""Content"":""{0}""}}", message));


                UploadStringCompletedEventHandler cb = null;
                cb = (s, e) =>
                {
                    if (cb != null)
                        client.UploadStringCompleted -= cb;

                    if (e.Error != null)
                    {
                        if (e.Error is WebException)
                        {
                            var we = e.Error as WebException;
                            try
                            {
                                var result = JObject.Load(new JsonTextReader(new StreamReader(we.Response.GetResponseStream())));
                                var error = result.GetValue("error");
                                if (error != null)
                                {
                                    info.Continuation(new Exception(result.ToString(), e.Error));
                                    return;
                                }
                            }
                            catch (Exception) { info.Continuation(new Exception("Failed to send log event to ElasticSearch", e.Error)); }
                        }

                        info.Continuation(e.Error);

                        return;
                    }

                    info.Continuation(null);
                };

                client.UploadStringCompleted += cb;
                client.UploadStringAsync(url, "POST", json.ToString());
            }
            catch (Exception ex)
            {
                info.Continuation(ex);
            }
        }

        private static bool IsStringAValidJson(string message)
        {
            try
            {
                JObject.Parse(message);
            }
            catch (JsonReaderException)
            {
                return false;
            }

            return true;
        }
    }
}
